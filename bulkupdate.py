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

    def _verify_document_count(self, index_name: str, expected_count: int) -> dict:
        """
        Verify the document count in the target index.
        
        Args:
            index_name (str): Name of the target index
            expected_count (int): Expected number of documents
            
        Returns:
            dict: Verification result containing status and details
        """
        max_retries = 3
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                actual_count = self._get_index_count(index_name)
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
        
        return {
            "status": "error",
            "message": f"Document count mismatch after {max_retries} attempts",
            "expected_count": expected_count,
            "actual_count": actual_count,
            "difference": actual_count - expected_count
        }

    def _process_files(self, all_files: List[Dict[str, Any]], index_name: str) -> Tuple[int, int]:
        """
        Process a list of files and return total rows and files processed.
        
        Args:
            all_files (List[Dict[str, Any]]): List of files to process
            index_name (str): Target index name
            
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
        for file_info in all_files:
            # Handle both string and dictionary file_info formats
            if isinstance(file_info, str):
                file_identifier = file_info
                file_info_dict = {
                    "file_path": file_info,
                    "type": "json" if file_info.lower().endswith('.json') else "csv"
                }
            else:
                file_identifier = file_info.get("file_path", f"{file_info.get('bucket', '')}/{file_info.get('key', '')}")
                file_info_dict = file_info
                
            try:
                total_files += 1
                rows_processed = self.file_processor.process_file(file_info_dict, index_name, self._make_request)
                total_rows += rows_processed
                with self._lock:
                    self._processed_count += rows_processed
                logger.info(f"Processed {rows_processed} rows from {file_identifier}")
            except Exception as e:
                logger.error(f"Error processing file {file_identifier}: {str(e)}")
                raise OpenSearchException(f"Error processing file {file_identifier}: {str(e)}")
                
        return total_rows, total_files

    def ingest_data(self, bucket: Optional[str] = None, prefix: Optional[str] = None, 
                    local_files: Optional[List[str]] = None, local_folder: Optional[str] = None,
                    index_name: str = None) -> Dict[str, Any]:
        """
        Ingest data from CSV or JSON files into OpenSearch.
        
        This method can process files from S3, local files, or both.
        
        Args:
            bucket (str, optional): S3 bucket name
            prefix (str, optional): S3 prefix to filter files
            local_files (List[str], optional): List of local CSV or JSON file paths
            local_folder (str, optional): Path to a folder containing CSV or JSON files
            index_name (str): Name of the target index
            
        Returns:
            Dict[str, Any]: Result containing status and details
        """
        start_time = time.time()
        all_files = []
        
        try:
            logger.info(f"Starting data ingestion to index: {index_name}")
            self._processed_count = 0
            
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
                    file_type = "csv" if file_path.lower().endswith('.csv') else "json"
                    all_files.append({"file_path": file_path, "type": file_type})
            
            # Process S3 files if bucket and prefix are provided
            if bucket and prefix:
                s3_result = self.file_processor.process_s3_files(bucket, prefix)
                if s3_result["status"] == "error":
                    return s3_result
                if "files" in s3_result:
                    all_files.extend(s3_result["files"])
            
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
            
            # Validate and cleanup target index
            logger.info(f"Validating and cleaning up index: {index_name}")
            cleanup_result = self.index_manager.validate_and_cleanup_index(index_name)
            if cleanup_result["status"] == "error":
                logger.error(f"Index cleanup failed: {cleanup_result['message']}")
                return cleanup_result
            
            # Process all files
            total_rows, total_files = self._process_files(all_files, index_name)
            
            # Verify document count
            try:
                verification_result = self._verify_document_count(index_name, self._processed_count)
                
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
    args = parser.parse_args()
    
    # Validate that at least one data source is provided
    if not args.bucket and not args.local_files and not args.local_folder:
        parser.error("At least one of --bucket, --local-files, or --local-folder must be provided")
    
    logger.info(f"Starting bulk ingestion script with index: {args.index}")
    if args.bucket:
        logger.info(f"S3 source: bucket={args.bucket}, prefix={args.prefix}")
    if args.local_files:
        logger.info(f"Local files: {', '.join(args.local_files)}")
    if args.local_folder:
        logger.info(f"Local folder: {args.local_folder}")
    
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
            index_name=args.index
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
# python bulkupdate.py --local-folder ./testdata/member_data --index my_index_primary --batch-size 1000 --max-workers 2

