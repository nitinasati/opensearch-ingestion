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
                 verify_ssl: bool = False, max_workers: int = 4):
        """
        Initialize the bulk ingestion manager.
        
        Args:
            batch_size (int): Number of documents to process in each batch
            opensearch_endpoint (str, optional): The OpenSearch cluster endpoint URL
            verify_ssl (bool): Whether to verify SSL certificates
            max_workers (int): Maximum number of parallel threads for processing
        """
        # Initialize parent class
        super().__init__(opensearch_endpoint=opensearch_endpoint, verify_ssl=verify_ssl)
        
        # Initialize instance attributes
        self.batch_size = batch_size
        self.max_workers = max_workers
        self._batch_queue = Queue()
        self._processed_count = 0
        self._lock = threading.Lock()
        
        # Initialize clients and processors
        self.s3_client = boto3.client('s3')
        self.index_manager = OpenSearchIndexManager(opensearch_endpoint=opensearch_endpoint, verify_ssl=verify_ssl)
        self.file_processor = FileProcessor(batch_size=batch_size, max_workers=max_workers)
        
        logger.info(f"Initialized OpenSearchBulkIngestion with batch_size: {batch_size}, max_workers: {max_workers}")

    def _verify_document_count(self, index_name: str, expected_count: int, resume: bool = False, initial_count: int = 0) -> Dict[str, Any]:
        """
        Verify the document count in the target index.
        
        Args:
            index_name (str): Name of the target index
            expected_count (int): Expected number of documents
            resume (bool): Whether we're in resume mode
            
        Returns:
            dict: Verification result containing status and details
        """
        max_retries = 3
        retry_delay = 5
        
        # In resume mode, we need to track the initial count and verify the difference
      
        
        for attempt in range(max_retries):
            try:
                actual_count = self._get_index_count(index_name)
                
                if resume:
                    # In resume mode, we verify the difference between final and initial count
                    new_documents = actual_count - initial_count
                    logger.info(f"Document count verification (resume mode) - Expected new documents: {expected_count}, Actual new documents: {new_documents}")
                    
                    if new_documents == expected_count:
                        logger.info(f"Document count verification successful: {new_documents} new documents match expected count")
                        return {
                            "status": "success",
                            "documents_indexed": new_documents,
                            "expected_count": expected_count,
                            "actual_count": new_documents,
                            "total_count": actual_count,
                            "initial_count": initial_count
                        }
                    
                    logger.warning(f"Document count mismatch on attempt {attempt + 1}/{max_retries}")
                    logger.warning(f"Expected new documents: {expected_count}, Actual new documents: {new_documents}, Difference: {new_documents - expected_count}")
                else:
                    # In fresh load mode, we verify the total count
                    logger.info(f"Document count verification - Expected: {expected_count}, Actual: {actual_count}")
                    
                    if actual_count == expected_count:
                        logger.info(f"Document count verification successful: {actual_count} documents match expected count")
                        return {
                            "status": "success",
                            "documents_indexed": actual_count,
                            "expected_count": expected_count,
                            "actual_count": actual_count
                        }
                    
                    logger.warning(f"Document count mismatch on attempt {attempt + 1}/{max_retries}")
                    logger.warning(f"Expected: {expected_count}, Actual: {actual_count}, Difference: {actual_count - expected_count}")
                
                if attempt < max_retries - 1:
                    logger.info(f"Retrying verification in {retry_delay} seconds...")
                    time.sleep(retry_delay)
            except (requests.exceptions.RequestException, ValueError, KeyError) as e:
                logger.error(f"Error getting document count on attempt {attempt + 1}: {str(e)}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying verification in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    continue
                return {
                    "status": "error",
                    "message": f"Failed to get document count after {max_retries} attempts: {str(e)}",
                    "expected_count": expected_count,
                    "actual_count": None
                }
        
        if resume:
            new_documents = actual_count - initial_count
            return {
                "status": "error",
                "message": f"Document count mismatch after {max_retries} attempts",
                "expected_count": expected_count,
                "actual_count": new_documents,
                "total_count": actual_count,
                "initial_count": initial_count,
                "difference": new_documents - expected_count
            }
        else:
            return {
                "status": "error",
                "message": f"Document count mismatch after {max_retries} attempts",
                "expected_count": expected_count,
                "actual_count": actual_count,
                "difference": actual_count - expected_count
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

    def _process_files(self, all_files: List[Dict[str, Any]], index_name: str, resume: bool = False) -> Tuple[int, int]:
        """
        Process a list of files and return total rows and files processed.
        
        Args:
            all_files (List[Dict[str, Any]]): List of files to process
            index_name (str): Target index name
            resume (bool): Whether to resume processing from previously processed files
            
        Returns:
            Tuple[int, int]: (total_rows, total_files)
            
        Raises:
            OpenSearchException: If there's an error processing any file
        """
        total_rows = 0
        total_files = 0
        
        if not all_files:
            logger.warning(NO_FILES_MESSAGE)
            return total_rows, total_files
            
        logger.info(f"Processing {len(all_files)} files in total")
        
        # Get list of already processed files if resuming
        processed_files = []
        if resume:
            processed_files = self._get_processed_files(index_name)
            logger.info(f"Found {len(processed_files)} previously processed files")
            
        for file_info in all_files:
            # Handle both string and dictionary file_info formats
            if isinstance(file_info, str):
                file_identifier = file_info
                file_info_dict = {
                    "file_path": file_info,
                    "type": "json" if file_info.lower().endswith('.json') else "csv"
                }
            else:
                file_identifier = self._get_file_identifier(file_info)
                file_info_dict = file_info
                
            # Skip if file was already processed and we're in resume mode
            if resume and file_identifier in processed_files:
                logger.info(f"Skipping already processed file: {file_identifier}")
                continue
                
            try:
                total_files += 1
                rows_processed = self.file_processor.process_file(file_info_dict, index_name, self._make_request)
                total_rows += rows_processed
                with self._lock:
                    self._processed_count += rows_processed
                logger.info(f"Processed {rows_processed} rows from {file_identifier}")
                
                # Update tracking file with successfully processed file
                self._update_processed_files(index_name, file_identifier)
                    
            except Exception as e:
                logger.error(f"Error processing file {file_identifier}: {str(e)}")
                raise OpenSearchException(f"Error processing file {file_identifier}: {str(e)}")
                
        return total_rows, total_files

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
        all_files = []
        
        try:
            logger.info(f"Starting data ingestion to index: {index_name}")
            self._processed_count = 0
            
            # Clear tracking file if fresh load is requested (default behavior)
            if fresh_load:
                logger.info("Performing fresh load - clearing processed files tracking")
                self._clear_processed_files(index_name)
            
            # Process local folder if provided
            if local_folder:
                folder_result = self.file_processor.process_local_folder(local_folder)
                if folder_result["status"] == "error":
                    return folder_result
                if "files" in folder_result:
                    all_files.extend(folder_result["files"])
            
            # Process local files if provided
            if local_files:
                for file_path in local_files:
                    # Determine file type
                    file_type = None
                    if file_path.lower().endswith('.json'):
                        file_type = "json"
                    elif file_path.lower().endswith('.csv'):
                        file_type = "csv"
                        
                    if file_type:
                        all_files.append({"file_path": file_path, "type": file_type})
                    else:
                        logger.warning(f"Ignoring file {file_path} due to unsupported file type. Only .csv and .json files are supported.")
            
            # Process S3 files if bucket and prefix are provided
            if bucket and prefix:
                s3_result = self.file_processor.process_s3_files(bucket, prefix)
                if s3_result["status"] == "error":
                    return s3_result
                if "files" in s3_result:
                    # Filter out unsupported file types
                    filtered_files = []
                    for file_info in s3_result["files"]:
                        file_path = file_info.get("file_path", f"{file_info.get('bucket', '')}/{file_info.get('key', '')}")
                        if file_info.get("type") in ["csv", "json"]:
                            filtered_files.append(file_info)
                        else:
                            logger.warning(f"Ignoring file {file_path} due to unsupported file type. Only .csv and .json files are supported.")
                    all_files.extend(filtered_files)
            
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
            initial_count = 0
            if resume:
                try:
                    initial_count = self._get_index_count(index_name)
                    logger.info(f"Resume mode - Initial document count: {initial_count}")
                except Exception as e:
                    logger.error(f"Error getting initial document count: {str(e)}")
                    # Continue with verification, but we won't be able to verify the difference
            # Process all files
            total_rows, total_files = self._process_files(all_files, index_name, resume)
            
            # Verify document count
            try:
                verification_result = self._verify_document_count(index_name, self._processed_count, resume,initial_count)
                
                if verification_result["status"] == "error":
                    return {
                        "status": "error",
                        "message": verification_result["message"],
                        "total_rows_processed": total_rows,
                        "total_files_processed": total_files,
                        "expected_documents": verification_result["expected_count"],
                        "actual_documents": verification_result["actual_count"],
                        "total_time_seconds": round(time.time() - start_time, 2)
                    }
                    
                total_time = time.time() - start_time
                return {
                    "status": "success",
                    "total_rows_processed": total_rows,
                    "total_files_processed": total_files,
                    "documents_indexed": verification_result["documents_indexed"],
                    "total_time_seconds": round(total_time, 2),
                    "average_time_per_file": round(total_time / total_files, 2) if total_files > 0 else 0,
                    "files_processed": total_rows > 0
                }
                
            except Exception as e:
                logger.error(f"Error verifying document count: {str(e)}")
                return {
                    "status": "error",
                    "message": f"Error verifying document count: {str(e)}",
                    "total_rows_processed": total_rows,
                    "total_files_processed": total_files,
                    "total_time_seconds": round(time.time() - start_time, 2)
                }
            
        except Exception as e:
            error_msg = f"Error during data ingestion: {str(e)}"
            logger.error(error_msg)
            return {
                "status": "error",
                "message": error_msg
            }

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
    if args.bucket:
        logger.info(f"S3 source: bucket={args.bucket}, prefix={args.prefix}")
    if args.local_files:
        logger.info(f"Local files: {', '.join(args.local_files)}")
    if args.local_folder:
        logger.info(f"Local folder: {args.local_folder}")
    if args.resume:
        logger.info("Resume mode enabled - will skip previously processed files")
    else:
        logger.info("Fresh load mode (default) - will process all files")
    
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
            logger.info(f"Total documents indexed: {result['documents_indexed']}")
            logger.info(f"Total time taken: {result['total_time_seconds']} seconds")
            logger.info(f"Average time per file: {result['average_time_per_file']} seconds")
        else:
            logger.error(f"Failed to ingest data: {result['message']}")
            if 'expected_documents' in result and 'actual_documents' in result:
                logger.error(f"Expected documents: {result['expected_documents']}, Actual documents: {result['actual_documents']}")
            elif 'documents_indexed' in result:
                logger.error(f"Documents indexed: {result['documents_indexed']}")
                
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

