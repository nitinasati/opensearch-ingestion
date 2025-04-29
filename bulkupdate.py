"""
OpenSearch Bulk Ingestion Manager

This module provides functionality to ingest data from S3 CSV files into OpenSearch.
It handles the process of reading CSV files, transforming data, and performing
bulk ingestion operations with validation and error handling.

Key features:
- S3 CSV file processing using pandas
- Batch processing for efficient ingestion
- Document count validation
- Error handling and recovery
- Comprehensive logging
- Parallel processing with configurable threads
- Support for AWS IAM authentication
"""

import boto3
import pandas as pd
import json
import requests
import logging
from typing import List, Dict, Any, Optional, Tuple
from io import StringIO
import time
import os
from dotenv import load_dotenv
import argparse
from index_cleanup import OpenSearchIndexManager
import urllib3
from opensearch_base_manager import OpenSearchBaseManager, OpenSearchException
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
import threading
from file_processor import FileProcessor
from datetime import datetime

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

# Disable SSL verification warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Constants
NO_FILES_MESSAGE = "No files to process"
TRACKING_FILE = "processed_files.json"

class OpenSearchBulkIngestion(OpenSearchBaseManager):
    """
    Manages bulk ingestion of data from S3 CSV files into OpenSearch.
    
    This class handles all operations related to reading CSV files from S3,
    transforming the data, and performing bulk ingestion operations.
    
    Attributes:
        opensearch_endpoint (str): The OpenSearch cluster endpoint URL
        batch_size (int): Number of documents to process in each batch
        s3_client (boto3.client): AWS S3 client
        index_manager (OpenSearchIndexManager): Manager for index operations
        max_workers (int): Maximum number of parallel threads for processing
    """
    
    def __init__(self, batch_size: int = 10000, opensearch_endpoint: Optional[str] = None, 
                 max_workers: int = 4):
        """
        Initialize the bulk ingestion manager.
        
        Args:
            batch_size (int): Number of documents to process in each batch
            opensearch_endpoint (str, optional): The OpenSearch cluster endpoint URL
            max_workers (int): Maximum number of parallel threads for processing
        """
        # Initialize parent class
        super().__init__(opensearch_endpoint=opensearch_endpoint)
        
        # Initialize instance attributes
        self.batch_size = batch_size
        self.max_workers = max_workers
        self._batch_queue = Queue()
        self._processed_count = 0
        self._lock = threading.Lock()
        
        # Initialize clients and processors
        self.s3_client = boto3.client('s3')
        self.index_manager = OpenSearchIndexManager(opensearch_endpoint=opensearch_endpoint)
        self.file_processor = FileProcessor(batch_size=batch_size, max_workers=max_workers)
        
        logger.info(f"Initialized OpenSearchBulkIngestion with batch_size: {batch_size}, max_workers: {max_workers}")

    def _verify_document_count(self, total_rows_from_file: int, total_processed_count_from_bulk: int, resume: bool = False) -> Dict[str, Any]:
        """
        Verify that the document count matches the expected count.
        
        Args:
            index_name (str): Name of the index
            expected_count (int): Expected number of documents
            resume (bool): Whether we're in resume mode
            initial_count (int): Initial document count (used in resume mode)
            
        Returns:
            dict: Verification result containing status and details
        """
        try:
            # Use the processed count from the FileProcessor
            logger.info(f"Document count verification - Expected new documents: {total_rows_from_file}, Actual new documents: {total_processed_count_from_bulk}")
            
            if total_rows_from_file == total_processed_count_from_bulk:
                logger.info(f"Document count verification successful: {total_rows_from_file} new documents match expected count")
                return {
                    "status": "success",
                    "message": f"Document count verification successful: {total_rows_from_file} new documents match expected count",
                    "expected_count": total_rows_from_file,
                    "actual_count": total_processed_count_from_bulk,
                    "documents_indexed": total_processed_count_from_bulk
                }
            else:
                logger.warning(f"Document count mismatch: Expected {total_rows_from_file} new documents, got {total_processed_count_from_bulk}")
                return {
                    "status": "error",
                    "message": f"Document count mismatch: Expected {total_rows_from_file} new documents, got {total_processed_count_from_bulk}",
                    "expected_count": total_rows_from_file,
                    "actual_count": total_processed_count_from_bulk,
                    "documents_indexed": total_processed_count_from_bulk
                }
        except Exception as e:
            logger.error(f"Error verifying document count: {str(e)}")
            return {
                "status": "error",
                "message": f"Error verifying document count: {str(e)}",
                "expected_count": total_rows_from_file,
                "actual_count": 0,
                "documents_indexed": 0
            }

    def _get_processed_files(self, index_name: str) -> List[str]:
        """
        Get a list of files that have already been processed for a given index.
        
        Args:
            index_name (str): Name of the index
            
        Returns:
            List[str]: List of file identifiers that have been processed
        """
        try:
            if not os.path.exists(TRACKING_FILE):
                return []
                
            with open(TRACKING_FILE, 'r') as f:
                tracking_data = json.load(f)
                
            # Return files for the specific index or empty list if index not found
            return tracking_data.get(index_name, [])
            
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Error reading tracking file: {str(e)}")
            return []
            
    def _update_processed_files(self, index_name: str, file_identifier: str) -> None:
        """
        Update the list of processed files for a given index.
        
        Args:
            index_name (str): Name of the index
            file_identifier (str): Unique identifier for the processed file
        """
        try:
            # Load existing tracking data
            tracking_data = {}
            if os.path.exists(TRACKING_FILE):
                with open(TRACKING_FILE, 'r') as f:
                    tracking_data = json.load(f)
            
            # Initialize index entry if it doesn't exist
            if index_name not in tracking_data:
                tracking_data[index_name] = []
                
            # Add file if not already in the list
            if file_identifier not in tracking_data[index_name]:
                tracking_data[index_name].append(file_identifier)
                
            # Write updated tracking data
            with open(TRACKING_FILE, 'w') as f:
                json.dump(tracking_data, f, indent=2)
                
        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Error updating tracking file: {str(e)}")
            
    def _clear_processed_files(self, index_name: Optional[str] = None) -> None:
        """
        Clear the tracking file for a specific index or all indices.
        
        Args:
            index_name (str, optional): Name of the index to clear. If None, clears all indices.
        """
        try:
            if not os.path.exists(TRACKING_FILE):
                return
                
            if index_name is None:
                # Clear entire tracking file
                with open(TRACKING_FILE, 'w') as f:
                    json.dump({}, f, indent=2)
                logger.info("Cleared all processed files tracking data")
            else:
                # Clear only the specified index
                with open(TRACKING_FILE, 'r') as f:
                    tracking_data = json.load(f)
                    
                if index_name in tracking_data:
                    tracking_data[index_name] = []
                    with open(TRACKING_FILE, 'w') as f:
                        json.dump(tracking_data, f, indent=2)
                    logger.info(f"Cleared processed files tracking data for index: {index_name}")
                    
        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Error clearing tracking file: {str(e)}")
            
    def _get_file_identifier(self, file_info: Dict[str, Any]) -> str:
        """
        Generate a unique identifier for a file based on its information.
        
        Args:
            file_info (Dict[str, Any]): File information
            
        Returns:
            str: Unique identifier for the file
        """
        if "bucket" in file_info and "key" in file_info:
            # S3 file
            return f"{file_info['bucket']}/{file_info['key']}"
        else:
            # Local file
            return file_info.get("file_path", "")

    def _determine_file_type(self, file_path):
        """Determine file type based on extension."""
        if file_path.lower().endswith('.json'):
            return "json"
        elif file_path.lower().endswith('.csv'):
            return "csv"
        return "unknown"
        
    def _process_file_info(self, file_info: Dict[str, Any], index_name: str, processed_files: List[str], resume: bool) -> Tuple[int, int, Dict[str, Any]]:
        """Process a single file and return its results."""
        if isinstance(file_info, str):
            file_identifier = file_info
            file_info_dict = {
                "file_path": file_info,
                "type": self._determine_file_type(file_info)
            }
        else:
            file_identifier = self._get_file_identifier(file_info)
            file_info_dict = file_info
            
        if resume and file_identifier in processed_files:
            logger.info(f"Skipping already processed file: {file_identifier}")
            return 0, 0, None
            
        try:
            rows_processed_from_file, processed_count_from_bulk = self.file_processor.process_file(file_info_dict, index_name, self._make_request)
            
            # Determine status based on processing results
            if rows_processed_from_file == processed_count_from_bulk and rows_processed_from_file > 0:
                status = "success"
            elif processed_count_from_bulk == 0 or rows_processed_from_file == 0:
                status = "failed"
            else:
                status = "partial"
                
            file_result = {
                "file_name": file_identifier,
                "total_rows": rows_processed_from_file,
                "processed_rows": processed_count_from_bulk,
                "status": status
            }
            return rows_processed_from_file, processed_count_from_bulk, file_result
        except Exception as e:
            logger.error(f"Error processing file {file_identifier}: {str(e)}")
            return 0, 0, {
                "file_name": file_identifier,
                "total_rows": 0,
                "processed_rows": 0,
                "status": "error",
                "error": str(e)
            }

    def _process_files(self, all_files: List[Dict[str, Any]], index_name: str, resume: bool = False) -> Tuple[int, int, int]:
        """Process a list of files and return total rows and files processed."""
        if not all_files:
            logger.warning(NO_FILES_MESSAGE)
            return 0, 0, 0
            
        logger.info(f"Processing {len(all_files)} files in total")
        processed_files = self._get_processed_files(index_name) if resume else []
        
        total_rows_from_file = 0
        total_processed_count_from_bulk = 0
        total_files = 0
        file_processing_results = []
        
        for file_info in all_files:
            rows_processed, processed_count, file_result = self._process_file_info(file_info, index_name, processed_files, resume)
            if file_result:
                total_files += 1
                total_rows_from_file += rows_processed
                total_processed_count_from_bulk += processed_count
                file_processing_results.append(file_result)
                
                if rows_processed == processed_count and rows_processed > 0:
                    logger.info(f"{file_result['file_name']} processed successfully with {rows_processed} rows")
                    self._update_processed_files(index_name, file_result['file_name'])
                elif processed_count == 0 or rows_processed == 0:
                    logger.error(f"{file_result['file_name']} failed to process any rows")
                else:
                    logger.warning(f"{file_result['file_name']} processed with error {processed_count} out of {rows_processed} rows processed successfully")
                    
        self._generate_summary_report(file_processing_results)
        return total_rows_from_file, total_files, total_processed_count_from_bulk

    def _generate_summary_report(self, file_results: List[Dict[str, Any]]) -> None:
        """
        Generate a summary report of file processing results.
        
        Args:
            file_results (List[Dict[str, Any]]): List of file processing results
        """
        logger.info("== Processing Summary Report ===")
        logger.info(f"{'File Name':<30} {'Total Rows':<12} {'Processed Rows':<15} {'Status':<10}")
        logger.info("-" * 70)
        
        for result in file_results:
            # Extract just the filename from the path
            file_name = os.path.basename(result['file_name'])
            logger.info(f"{file_name:<30} {result['total_rows']:<12} {result['processed_rows']:<15} {result['status']:<10}")
            
        # Calculate totals
        total_files = len(file_results)
        total_rows = sum(r['total_rows'] for r in file_results)
        total_processed = sum(r['processed_rows'] for r in file_results)
        success_files = sum(1 for r in file_results if r['status'] == 'success')
        partial_files = sum(1 for r in file_results if r['status'] == 'partial')
        error_files = sum(1 for r in file_results if r['status'] == 'failed')
        
        logger.info("-" * 70)
        logger.info(f"Total Files: {total_files}")
        logger.info(f"Successfully Processed Files: {success_files}")
        logger.info(f"Partially Processed Files: {partial_files}")
        logger.info(f"Failed Files: {error_files}")
        logger.info(f"Total Rows: {total_rows}")
        logger.info(f"Successfully Processed Rows: {total_processed}")
        logger.info("=== End of Summary Report ===\n")

    def _process_local_sources(self, local_folder, local_files):
        """Process local folder and files, return list of files to process."""
        all_files = []
        
        if local_folder:
            folder_result = self.file_processor.process_local_folder(local_folder)
            if folder_result["status"] == "error":
                return folder_result
            if "files" in folder_result:
                all_files.extend(folder_result["files"])
        
        if local_files:
            for file_path in local_files:
                file_type = self._determine_file_type(file_path)
                if file_type:
                    all_files.append({"file_path": file_path, "type": file_type})
                else:
                    logger.warning(f"Ignoring file {file_path} due to unsupported file type. Only .csv and .json files are supported.")
        
        return all_files
    
    def _filter_s3_files(self, s3_files):
        """Filter S3 files to only include CSV and JSON files."""
        filtered_files = []
        for file_info in s3_files:
            file_path = file_info.get("file_path", f"{file_info.get('bucket', '')}/{file_info.get('key', '')}")
            if file_info.get("type") in ["csv", "json"]:
                filtered_files.append(file_info)
            else:
                logger.warning(f"Ignoring file {file_path} due to unsupported file type. Only .csv and .json files are supported.")
        return filtered_files
    
    def _process_s3_source(self, bucket, prefix):
        """Process S3 bucket and prefix, return list of files to process."""
        if not bucket or not prefix:
            return []
            
        s3_result = self.file_processor.process_s3_files(bucket, prefix)
        if s3_result["status"] == "error":
            return s3_result
            
        if "files" not in s3_result:
            return []
            
        return self._filter_s3_files(s3_result["files"])
    
    def _format_verification_result(self, verification_result, total_rows, total_files, start_time):
        """Format verification result into a response dictionary."""
        # Calculate total time based on start_time type
        if isinstance(start_time, datetime):
            total_time = time.time() - start_time.timestamp()
        else:
            total_time = time.time() - start_time
            
        if verification_result["status"] == "error":
            return {
                "status": "error",
                "message": verification_result["message"],
                "total_rows_processed": total_rows,
                "total_files_processed": total_files,
                "expected_documents": verification_result["expected_count"],
                "actual_documents": verification_result["actual_count"],
                "total_time_seconds": round(total_time, 2)
            }
            
        return {
            "status": "success",
            "message": verification_result["message"],
            "total_rows_processed": total_rows,
            "total_files_processed": total_files,
            "expected_documents": verification_result["expected_count"],
            "actual_documents": verification_result["actual_count"],
            "total_time_seconds": round(total_time, 2)
        }
    
    def _handle_verification_error(self, e, total_rows, total_files, start_time):
        """Handle verification error and return error response."""
        logger.error(f"Error verifying document count: {str(e)}")
        
        # Calculate total time based on start_time type
        if isinstance(start_time, datetime):
            total_time = time.time() - start_time.timestamp()
        else:
            total_time = time.time() - start_time
            
        return {
            "status": "error",
            "message": f"Error verifying document count: {str(e)}",
            "total_rows_processed": total_rows,
            "total_files_processed": total_files,
            "expected_documents": 0,
            "actual_documents": 0,
            "total_time_seconds": round(total_time, 2)
        }
    
    def _verify_results(self,total_rows_from_file, total_files, total_processed_count_from_bulk, start_time, resume):
        """Verify document count and return results."""
        try:
            verification_result = self._verify_document_count(total_rows_from_file, total_processed_count_from_bulk, resume)
            return self._format_verification_result(verification_result, total_rows_from_file, total_files, start_time)
            
        except Exception as e:
            return self._handle_verification_error(e, total_rows_from_file, total_files, start_time)

    def ingest_data(self, bucket: Optional[str] = None, prefix: Optional[str] = None, 
                    local_files: Optional[List[str]] = None, local_folder: Optional[str] = None,
                    index_name: str = None, resume: bool = False, fresh_load: bool = True) -> Dict[str, Any]:
        """
        Ingest data from CSV or JSON files into OpenSearch.
        
        This method can process files from S3, local files, or both.
        
        Args:
            bucket (str, optional): S3 bucket name
            prefix (str, optional): S3 prefix to filter files
            local_files (List[str], optional): List of local CSV or JSON file paths
            local_folder (str, optional): Path to a folder containing CSV or JSON files
            index_name (str): Name of the target index
            resume (bool): Whether to resume processing from previously processed files
            fresh_load (bool): Whether to perform a fresh load, clearing the tracking file (default: True)
            
        Returns:
            Dict[str, Any]: Result containing status and details
        """
        start_time = time.time()
        
        try:
            logger.info(f"Starting data ingestion to index: {index_name}")
            self._processed_count = 0
            
            # checking if index exists
            logger.info(f"Checking if index {index_name} exists")
            index_exists = self.index_manager._verify_index_exists(index_name)
            if not index_exists:
                logger.error(f"Index {index_name} does not exist")
                return {
                    "status": "error",
                    "message": f"Index {index_name} does not exist"
                }
            # Clear tracking file if fresh load is requested
            if fresh_load:
                logger.info("Performing fresh load - clearing processed files tracking")
                self._clear_processed_files(index_name)
            
            # Process local sources
            local_result = self._process_local_sources(local_folder, local_files)
            if isinstance(local_result, dict) and local_result.get("status") == "error":
                return local_result
            all_files = local_result
            
            # Process S3 source
            s3_files = self._process_s3_source(bucket, prefix)
            if isinstance(s3_files, dict) and s3_files.get("status") == "error":
                return s3_files
            all_files.extend(s3_files)
            
            # Check if there are any files to process
            if not all_files:
                logger.warning(NO_FILES_MESSAGE)
                return {
                    "status": "warning",
                    "message": NO_FILES_MESSAGE,
                    "total_rows_processed": 0,
                    "total_files_processed": 0,
                    "total_time_seconds": round(time.time() - start_time, 2)
                }
            
            # Validate and cleanup target index only if not in resume mode
            if not resume:
                logger.info(f"Validating and cleaning up index: {index_name}")
                cleanup_result = self.index_manager.validate_and_cleanup_index(index_name)
                if cleanup_result["status"] == "error":
                    logger.error(f"Index cleanup failed: {cleanup_result['message']}")
                    return cleanup_result
            else:
                logger.info("Resume mode enabled - skipping index cleanup to preserve existing data")
                
          
            # Process all files
            total_rows_from_file, total_files, total_processed_count_from_bulk = self._process_files(all_files, index_name, resume)
            
            # Verify results
            return self._verify_results(total_rows_from_file, total_files, total_processed_count_from_bulk, start_time, resume)
            
        except Exception as e:
            error_msg = f"Error during data ingestion: {str(e)}"
            logger.error(error_msg)
            return {
                "status": "error",
                "message": error_msg
            }

def _arguments_printing(args):
    """Print arguments and their values."""
    logger.info(f"Index name: {args.index}")
    logger.info(f"Total rows: {args.batch_size}")
    logger.info(f"Total files: {args.max_workers}")
    logger.info(f"Start time: {args.resume}")
    logger.info(f"Resume: {args.fresh_load}")

def main():
    """
    Main entry point for the bulk ingestion script.
    
    Handles command line arguments and orchestrates the data ingestion process.
    """
    # Set up argument parser
    parser = argparse.ArgumentParser(description='OpenSearch Bulk Ingestion from S3 or Local Files')
    parser.add_argument('--bucket', help='S3 bucket name')
    parser.add_argument('--prefix', help='S3 prefix')
    parser.add_argument('--local-files', nargs='+', help='List of local CSV or JSON files to process')
    parser.add_argument('--local-folder', help='Path to a folder containing CSV or JSON files to process')
    parser.add_argument('--index', required=True, help='OpenSearch index name')
    parser.add_argument('--batch-size', type=int, default=10000, help='Number of documents to process in each batch (default: 10000)')
    parser.add_argument('--max-workers', type=int, default=4, help='Maximum number of parallel threads (default: 4)')
    parser.add_argument('--resume', action='store_true', help='Resume processing from previously processed files')
    parser.add_argument('--fresh-load', action='store_true', help='Perform a fresh load, clearing the tracking file (default behavior)')
    args = parser.parse_args()
    
    # Validate that at least one data source is provided
    if not args.bucket and not args.local_files and not args.local_folder:
        parser.error("At least one of --bucket, --local-files, or --local-folder must be provided")
    
    # Validate that resume and fresh-load are not both specified
    if args.resume and args.fresh_load:
        parser.error("Cannot specify both --resume and --fresh-load options")
    
    logger.info(f"Starting bulk ingestion script with index: {args.index}")
    _arguments_printing(args)
    
    try:
        # Initialize ingestion service with batch size and max workers
        ingestion_service = OpenSearchBulkIngestion(
            batch_size=args.batch_size,
            max_workers=args.max_workers
        )
        
        # Start ingestion with command line arguments
        result = ingestion_service.ingest_data(
            bucket=args.bucket,
            prefix=args.prefix,
            local_files=args.local_files,
            local_folder=args.local_folder,
            index_name=args.index,
            resume=args.resume,
            fresh_load=not args.resume  # Default to fresh_load unless resume is specified
        )
        
        # Print results
        if result["status"] == "success":
            logger.info(f"Successfully processed {result['total_rows_processed']} rows from {result['total_files_processed']} files")
            logger.info(f"Total documents indexed: {result['actual_documents']}")
            logger.info(f"Total time taken: {result['total_time_seconds']} seconds")
        else:
            logger.error(f"Failed to ingest data: {result['message']}")
            if 'expected_documents' in result and 'actual_documents' in result:
                logger.error(f"Expected documents: {result['expected_documents']}, Actual documents: {result['actual_documents']}")
            elif 'actual_documents' in result:
                logger.error(f"Documents indexed: {result['actual_documents']}")
                
    except ValueError as e:
        logger.error(f"Configuration error: {str(e)}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    main()

# Example usage:
# python bulkupdate.py --bucket openlpocbucket --prefix opensearch/ --index member_index_primary --batch-size 1000 --max-workers 2
# python bulkupdate.py --local-files member_data.csv member_data.json --index member_index_primary --batch-size 1000 --max-workers 2
# python bulkupdate.py --local-files data1.json data2.json --index my_index_primary --batch-size 1000 --max-workers 2
# python bulkupdate.py --bucket openlpocbucket --prefix opensearch/ --local-files data1.csv data2.json --index my_index_primary --batch-size 1000 --max-workers 2
# python bulkupdate.py --local-folder ./testdata --index member_index_primary --batch-size 1000 --max-workers 2

