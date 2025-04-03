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
from typing import List, Dict, Any, Optional
from io import StringIO
import time
import os
from dotenv import load_dotenv
import argparse
from index_cleanup import OpenSearchIndexManager
import urllib3
from opensearch_base_manager import OpenSearchBaseManager
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
import threading

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

# Disable SSL verification warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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
        super().__init__(opensearch_endpoint, verify_ssl)
        self.batch_size = batch_size
        self.s3_client = boto3.client('s3')
        self.index_manager = OpenSearchIndexManager()
        self.max_workers = max_workers
        self._batch_queue = Queue()
        self._processed_count = 0
        self._lock = threading.Lock()
        logger.info(f"Initialized OpenSearchBulkIngestion with batch_size: {batch_size}, max_workers: {max_workers}")

    def _create_bulk_request(self, documents: List[Dict[str, Any]], index_name: str) -> str:
        """
        Create bulk request body in NDJSON format.
        
        Args:
            documents (List[Dict[str, Any]]): List of documents to index
            index_name (str): Name of the target index
            
        Returns:
            str: NDJSON formatted bulk request body
        """
        bulk_request = []
        for doc in documents:
            bulk_request.append(json.dumps({
                "index": {
                    "_index": index_name
                }
            }))
            bulk_request.append(json.dumps(doc))
        return '\n'.join(bulk_request) + '\n'

    def _process_batch(self, batch: List[Dict[str, Any]], index_name: str, file_key: str) -> bool:
        """
        Process a batch of documents and send to OpenSearch.
        
        Args:
            batch (List[Dict[str, Any]]): List of documents to process
            index_name (str): Name of the target index
            file_key (str): S3 file key being processed
            
        Returns:
            bool: True if batch was processed successfully, False otherwise
        """
        max_retries = 3
        base_delay = 1  # Base delay in seconds
        max_delay = 30  # Maximum delay in seconds
        
        for attempt in range(max_retries):
            try:
                bulk_request = self._create_bulk_request(batch, index_name)
                response = self._make_request(
                    'POST',
                    '/_bulk',
                    data=bulk_request,
                    headers={'Content-Type': 'application/x-ndjson'}
                )
                
                if response.status_code == 429:  # Rate limit hit
                    delay = min(base_delay * (2 ** attempt), max_delay)  # Exponential backoff
                    logger.warning(f"Rate limit hit, retrying in {delay} seconds (attempt {attempt + 1}/{max_retries})")
                    time.sleep(delay)
                    continue
                
                result = response.json()
                if result.get('errors', False):
                    logger.error(f"Errors in batch from {file_key}: {json.dumps(result, indent=2)}")
                    return False
                
                with self._lock:
                    self._processed_count += len(batch)
                
                return True
                
            except Exception as e:
                logger.error(f"Error processing batch from {file_key}: {str(e)}")
                if attempt < max_retries - 1:
                    delay = min(base_delay * (2 ** attempt), max_delay)
                    time.sleep(delay)
                    continue
                return False
        
        return False

    def _process_batch_worker(self, index_name: str, file_key: str) -> None:
        """
        Worker function to process batches from the queue.
        
        Args:
            index_name (str): Name of the target index
            file_key (str): S3 file key being processed
        """
        while True:
            try:
                batch = self._batch_queue.get_nowait()
                if batch is None:  # Poison pill to stop the worker
                    break
                
                self._process_batch(batch, index_name, file_key)
                self._batch_queue.task_done()
                
            except Queue.Empty:
                break

    def _create_document(self, row: pd.Series) -> Dict[str, Any]:
        """
        Create a document from CSV row.
        
        Args:
            row (pd.Series): CSV row to convert to a document
            
        Returns:
            Dict[str, Any]: Document ready for indexing
        """
        document = {}
        for column in row.index:
            value = row[column]
            
            # Handle empty values
            if pd.isna(value):
                value = None
            # Handle numeric values
            elif pd.api.types.is_numeric_dtype(type(value)):
                value = float(value)
            # Handle boolean values
            elif isinstance(value, bool):
                value = value
            # Handle string values
            else:
                value = str(value).strip()
            
            document[column] = value
        
        return document

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
                    return {
                        "status": "success",
                        "documents_indexed": actual_count
                    }
                
                logger.warning(f"Document count mismatch on attempt {attempt + 1}/{max_retries}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
            except Exception as e:
                logger.error(f"Error getting document count on attempt {attempt + 1}: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                return {
                    "status": "error",
                    "message": f"Failed to get document count after {max_retries} attempts: {str(e)}"
                }
        
        return {
            "status": "error",
            "message": f"Document count mismatch after {max_retries} attempts",
            "expected_count": expected_count,
            "actual_count": actual_count
        }

    def process_s3_file(self, bucket: str, key: str, index_name: str) -> int:
        """
        Process a single S3 file and return number of processed rows.
        
        Args:
            bucket (str): S3 bucket name
            key (str): S3 object key
            index_name (str): Name of the target index
            
        Returns:
            int: Number of rows processed
        """
        file_start_time = time.time()
        try:
            logger.info(f"Processing file: {key}")
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            content = response['Body'].read().decode('utf-8')
            
            # Read CSV using pandas
            df = pd.read_csv(StringIO(content))
            logger.info(f"Found {len(df.columns)} columns in file: {key}")
            
            batch = []
            row_count = 0
            
            # Reset processed count
            self._processed_count = 0
            batch_count = 0
            # Create thread pool for batch processing
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = []
                
                for _, row in df.iterrows():
                    row_count += 1
                    try:
                        document = self._create_document(row)
                        batch.append(document)
                        
                        if len(batch) >= self.batch_size:
                            batch_count += 1
                            logger.info(f"Batch {batch_count} created with {len(batch)} documents")
                            self._batch_queue.put(batch.copy())
                            batch = []
                            # Add a small delay between batches to prevent overwhelming the cluster
                            time.sleep(0.1)
                            
                    except Exception as e:
                        logger.error(f"Error processing row {row_count} in file {key}: {str(e)}")
                
                # Process remaining documents
                if batch:
                    self._batch_queue.put(batch)
                
                # Add poison pills to stop workers
                for _ in range(self.max_workers):
                    logger.info(f"Adding poison pill {_} to queue")
                    self._batch_queue.put(None)
                
                # Start worker threads
                for _ in range(self.max_workers):
                    logger.info(f"Adding worker thread {_} to executor")
                    futures.append(
                        executor.submit(self._process_batch_worker, index_name, key)
                    )
                
                # Wait for all workers to complete
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"Worker thread error: {str(e)}")
            
            file_time = time.time() - file_start_time
            logger.info(f"Completed processing file {key} in {file_time:.2f} seconds")
            return self._processed_count
            
        except Exception as e:
            logger.error(f"Error processing file {key}: {str(e)}")
            return 0

    def process_local_file(self, file_path: str, index_name: str) -> int:
        """
        Process a local CSV file and return number of processed rows.
        
        Args:
            file_path (str): Path to the local CSV file
            index_name (str): Name of the target index
            
        Returns:
            int: Number of rows processed
        """
        file_start_time = time.time()
        try:
            logger.info(f"Processing local file: {file_path}")
            
            # Read CSV using pandas
            df = pd.read_csv(file_path)
            logger.info(f"Found {len(df.columns)} columns in file: {file_path}")
            
            batch = []
            row_count = 0
            
            # Reset processed count
            self._processed_count = 0
            batch_count = 0
            # Create thread pool for batch processing
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = []
                
                for _, row in df.iterrows():
                    row_count += 1
                    try:
                        document = self._create_document(row)
                        batch.append(document)
                        
                        if len(batch) >= self.batch_size:
                            batch_count += 1
                            logger.info(f"Batch {batch_count} created with {len(batch)} documents")
                            self._batch_queue.put(batch.copy())
                            batch = []
                            # Add a small delay between batches to prevent overwhelming the cluster
                            time.sleep(0.1)
                            
                    except Exception as e:
                        logger.error(f"Error processing row {row_count} in file {file_path}: {str(e)}")
                
                # Process remaining documents
                if batch:
                    self._batch_queue.put(batch)
                
                # Add poison pills to stop workers
                for _ in range(self.max_workers):
                    logger.info(f"Adding poison pill {_} to queue")
                    self._batch_queue.put(None)
                
                # Start worker threads
                for _ in range(self.max_workers):
                    logger.info(f"Adding worker thread {_} to executor")
                    futures.append(
                        executor.submit(self._process_batch_worker, index_name, file_path)
                    )
                
                # Wait for all workers to complete
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"Worker thread error: {str(e)}")
            
            file_time = time.time() - file_start_time
            logger.info(f"Completed processing file {file_path} in {file_time:.2f} seconds")
            return self._processed_count
            
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {str(e)}")
            return 0

    def process_local_json_file(self, file_path: str, index_name: str) -> int:
        """
        Process a local JSON file and return number of processed rows.
        
        Args:
            file_path (str): Path to the local JSON file
            index_name (str): Name of the target index
            
        Returns:
            int: Number of rows processed
        """
        file_start_time = time.time()
        try:
            logger.info(f"Processing local JSON file: {file_path}")
            
            # Read JSON file
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            # Handle both single object and array of objects
            if isinstance(data, dict):
                data = [data]
            
            logger.info(f"Found {len(data)} records in JSON file: {file_path}")
            
            batch = []
            row_count = 0
            
            # Reset processed count
            self._processed_count = 0
            batch_count = 0
            # Create thread pool for batch processing
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = []
                
                for item in data:
                    row_count += 1
                    try:
                        # For JSON, we can use the item directly as a document
                        document = item
                        batch.append(document)
                        
                        if len(batch) >= self.batch_size:
                            batch_count += 1
                            logger.info(f"Batch {batch_count} created with {len(batch)} documents")
                            self._batch_queue.put(batch.copy())
                            batch = []
                            # Add a small delay between batches to prevent overwhelming the cluster
                            time.sleep(0.1)
                            
                    except Exception as e:
                        logger.error(f"Error processing record {row_count} in file {file_path}: {str(e)}")
                
                # Process remaining documents
                if batch:
                    self._batch_queue.put(batch)
                
                # Add poison pills to stop workers
                for _ in range(self.max_workers):
                    logger.info(f"Adding poison pill {_} to queue")
                    self._batch_queue.put(None)
                
                # Start worker threads
                for _ in range(self.max_workers):
                    logger.info(f"Adding worker thread {_} to executor")
                    futures.append(
                        executor.submit(self._process_batch_worker, index_name, file_path)
                    )
                
                # Wait for all workers to complete
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"Worker thread error: {str(e)}")
            
            file_time = time.time() - file_start_time
            logger.info(f"Completed processing file {file_path} in {file_time:.2f} seconds")
            return self._processed_count
            
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {str(e)}")
            return 0

    def ingest_data(self, bucket: Optional[str] = None, prefix: Optional[str] = None, 
                    local_files: Optional[List[str]] = None, index_name: str = None) -> Dict[str, Any]:
        """
        Ingest data from CSV or JSON files into OpenSearch.
        
        This method can process files from S3, local files, or both.
        
        Args:
            bucket (str, optional): S3 bucket name
            prefix (str, optional): S3 prefix to filter files
            local_files (List[str], optional): List of local CSV or JSON file paths
            index_name (str): Name of the target index
            
        Returns:
            Dict[str, Any]: Result containing status and details
        """
        start_time = time.time()
        total_rows = 0
        total_files = 0
        
        try:
            logger.info(f"Starting data ingestion to index: {index_name}")
            
            # Validate and cleanup target index
            logger.info(f"Validating and cleaning up index: {index_name}")
            cleanup_result = self.index_manager.validate_and_cleanup_index(index_name)
            if cleanup_result["status"] == "error":
                logger.error(f"Index cleanup failed: {cleanup_result['message']}")
                return cleanup_result
            
            # Process local files if provided
            if local_files:
                logger.info(f"Processing {len(local_files)} local files")
                for file_path in local_files:
                    if file_path.endswith('.csv'):
                        total_files += 1
                        logger.info(f"Processing local CSV file {total_files}: {file_path}")
                        rows_processed = self.process_local_file(file_path, index_name)
                        total_rows += rows_processed
                        logger.info(f"Processed {rows_processed} rows from {file_path}")
                    elif file_path.endswith('.json'):
                        total_files += 1
                        logger.info(f"Processing local JSON file {total_files}: {file_path}")
                        rows_processed = self.process_local_json_file(file_path, index_name)
                        total_rows += rows_processed
                        logger.info(f"Processed {rows_processed} records from {file_path}")
                    else:
                        logger.warning(f"Skipping unsupported file format: {file_path}")
            
            # Process S3 files if bucket and prefix are provided
            if bucket and prefix:
                logger.info(f"Processing files from S3 bucket: {bucket}, prefix: {prefix}")
                # List objects in S3 bucket with prefix
                paginator = self.s3_client.get_paginator('list_objects_v2')
                for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                    if 'Contents' not in page:
                        logger.warning(f"No objects found in bucket {bucket} with prefix {prefix}")
                        continue
                    
                    for obj in page['Contents']:
                        key = obj['Key']
                        if not key.endswith('.csv'):
                            logger.debug(f"Skipping non-CSV file: {key}")
                            continue
                        
                        total_files += 1
                        logger.info(f"Processing S3 file {total_files}: {key}")
                        rows_processed = self.process_s3_file(bucket, key, index_name)
                        total_rows += rows_processed
                        logger.info(f"Processed {rows_processed} rows from {key}")
            
            # Verify document count with retries
            verification_result = self._verify_document_count(index_name, total_rows)
            
            if verification_result["status"] == "error":
                logger.error(f"Record count verification failed: {verification_result['message']}")
                return {
                    "status": "error",
                    "message": verification_result["message"],
                    "total_rows_processed": total_rows,
                    "total_files_processed": total_files,
                    "expected_documents": verification_result["expected_count"],
                    "actual_documents": verification_result["actual_count"],
                    "total_time_seconds": round(time.time() - start_time, 2)
                }
            
            end_time = time.time()
            total_time = end_time - start_time
            
            success_msg = f"Successfully processed {total_rows} rows from {total_files} files"
            logger.info(success_msg)
            logger.info(f"Total time taken: {round(total_time, 2)} seconds")
            
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
    parser.add_argument('--index', required=True, help='OpenSearch index name')
    parser.add_argument('--batch-size', type=int, default=10000, help='Number of documents to process in each batch (default: 10000)')
    parser.add_argument('--max-workers', type=int, default=4, help='Maximum number of parallel threads (default: 4)')
    args = parser.parse_args()
    
    # Validate that at least one data source is provided
    if not args.bucket and not args.local_files:
        parser.error("At least one of --bucket or --local-files must be provided")
    
    logger.info(f"Starting bulk ingestion script with index: {args.index}")
    if args.bucket:
        logger.info(f"S3 source: bucket={args.bucket}, prefix={args.prefix}")
    if args.local_files:
        logger.info(f"Local files: {', '.join(args.local_files)}")
    
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
# python bulkupdate.py --bucket openlpocbucket --prefix opensearch/ --index my_index_primary --batch-size 1000 --max-workers 8
# python bulkupdate.py --local-files data1.csv data2.csv --index my_index_primary --batch-size 1000 --max-workers 8
# python bulkupdate.py --local-files data1.json data2.json --index my_index_primary --batch-size 1000 --max-workers 8
# python bulkupdate.py --bucket openlpocbucket --prefix opensearch/ --local-files data1.csv data2.json --index my_index_primary --batch-size 1000 --max-workers 8

