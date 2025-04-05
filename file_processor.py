"""
File Processor Module

This module provides functionality to process files from different sources (local and S3)
and prepare them for ingestion into OpenSearch.
"""

import os
import logging
import boto3
from typing import Dict, Any, List, Tuple, Optional
import pandas as pd
from io import StringIO
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
import threading
import time

logger = logging.getLogger(__name__)

class FileProcessor:
    """
    Handles file processing from different sources (local and S3).
    """
    
    def __init__(self, batch_size: int = 10000, max_workers: int = 4):
        """
        Initialize the file processor.
        
        Args:
            batch_size (int): Number of documents to process in each batch
            max_workers (int): Maximum number of parallel threads for processing
        """
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.s3_client = boto3.client('s3')
        self._batch_queue = Queue()
        self._processed_count = 0
        self._lock = threading.Lock()
        logger.info(f"Initialized FileProcessor with batch_size: {batch_size}, max_workers: {max_workers}")

    def _get_files_by_type(self, folder_path: str) -> Tuple[List[str], List[str]]:
        """
        Get CSV and JSON files from a folder.
        
        Args:
            folder_path (str): Path to the folder
            
        Returns:
            tuple: (csv_files, json_files)
        """
        # Get all files in the folder
        all_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) 
                    if os.path.isfile(os.path.join(folder_path, f))]
        
        # Filter for CSV and JSON files
        csv_files = [f for f in all_files if f.lower().endswith('.csv')]
        json_files = [f for f in all_files if f.lower().endswith('.json')]
        
        logger.info(f"Found {len(csv_files)} CSV files and {len(json_files)} JSON files in folder {folder_path}")
        return csv_files, json_files

    def process_local_folder(self, folder_path: str) -> Dict[str, Any]:
        """
        Get all CSV and JSON files in a local folder.
        
        Args:
            folder_path (str): Path to the local folder containing CSV or JSON files
            
        Returns:
            Dict[str, Any]: Result containing status and list of files
        """
        try:
            # Normalize the folder path to handle both relative and absolute paths
            folder_path = os.path.abspath(folder_path)
            logger.info(f"Scanning for files in folder: {folder_path}")
            
            # Check if folder exists
            if not os.path.isdir(folder_path):
                error_msg = f"Folder does not exist: {folder_path}"
                logger.error(error_msg)
                return {
                    "status": "error",
                    "message": error_msg
                }
            
            # Get files by type
            csv_files, json_files = self._get_files_by_type(folder_path)
            
            # Combine all files
            all_files = csv_files + json_files
            
            if not all_files:
                logger.warning(f"No CSV or JSON files found in folder: {folder_path}")
                return {
                    "status": "warning",
                    "message": f"No CSV or JSON files found in folder: {folder_path}",
                    "files": []
                }
            
            logger.info(f"Found {len(csv_files)} CSV files and {len(json_files)} JSON files in folder {folder_path}")
            return {
                "status": "success",
                "files": all_files,
                "csv_files": csv_files,
                "json_files": json_files
            }
            
        except Exception as e:
            error_msg = f"Error scanning folder {folder_path}: {str(e)}"
            logger.error(error_msg)
            return {
                "status": "error",
                "message": error_msg,
                "files": []
            }

    def process_s3_files(self, bucket: str, prefix: str) -> Dict[str, Any]:
        """
        Get a list of CSV and JSON files from an S3 bucket with a given prefix.
        
        Args:
            bucket (str): S3 bucket name
            prefix (str): S3 prefix to filter files
            
        Returns:
            Dict[str, Any]: Result containing status and list of files
        """
        try:
            logger.info(f"Scanning for files in S3 bucket: {bucket}, prefix: {prefix}")
            
            # List objects in S3 bucket with prefix
            paginator = self.s3_client.get_paginator('list_objects_v2')
            csv_files = []
            json_files = []
            found_any_contents = False
            
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                if 'Contents' not in page:
                    continue
                
                found_any_contents = True
                for obj in page['Contents']:
                    key = obj['Key']
                    if key.lower().endswith('.csv'):
                        csv_files.append({"bucket": bucket, "key": key, "type": "csv", "size": obj.get('Size', 0)})
                    elif key.lower().endswith('.json'):
                        json_files.append({"bucket": bucket, "key": key, "type": "json", "size": obj.get('Size', 0)})
                    else:
                        logger.debug(f"Skipping non-CSV/JSON file: {key}")
            
            # Combine all files
            all_files = csv_files + json_files
            
            if not found_any_contents:
                logger.warning(f"No objects found in bucket {bucket} with prefix {prefix}")
                return {
                    "status": "warning",
                    "message": f"No objects found in bucket {bucket} with prefix {prefix}",
                    "files": []
                }
            
            if not all_files:
                logger.warning(f"No CSV or JSON files found in S3 bucket {bucket} with prefix {prefix}")
                return {
                    "status": "warning",
                    "message": f"No CSV or JSON files found in S3 bucket {bucket} with prefix {prefix}",
                    "files": []
                }
            
            logger.info(f"Found {len(csv_files)} CSV files and {len(json_files)} JSON files in S3 bucket {bucket} with prefix {prefix}")
            return {
                "status": "success",
                "files": all_files,
                "csv_files": csv_files,
                "json_files": json_files
            }
            
        except Exception as e:
            error_msg = f"Error scanning S3 bucket {bucket} with prefix {prefix}: {str(e)}"
            logger.error(error_msg)
            return {
                "status": "error",
                "message": error_msg,
                "files": []
            }

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
            # Use the 'id' field from the document if it exists
            index_action = {
                "index": {
                    "_index": index_name
                }
            }
            
            # Add the document ID if it exists in the document
            if "id" in doc:
                index_action["index"]["_id"] = doc["id"]
                
            bulk_request.append(json.dumps(index_action))
            bulk_request.append(json.dumps(doc))
        return '\n'.join(bulk_request) + '\n'

    def _process_batch(self, batch: List[Dict[str, Any]], index_name: str, file_key: str) -> bool:
        """
        Process a batch of documents.
        
        Args:
            batch (List[Dict[str, Any]]): List of documents to process
            index_name (str): Name of the target index
            file_key (str): File identifier for logging
            
        Returns:
            bool: True if batch was processed successfully, False otherwise
        """
        try:
            # Create bulk request
            bulk_request = self._create_bulk_request(batch, index_name)
            
            # Send request
            result = self._make_request('POST', '/_bulk', data=bulk_request, headers={'Content-Type': 'application/x-ndjson'})
            
            # Check for errors
            if result['status'] != 'success':
                logger.error(f"Bulk request failed for file {file_key}: {result['message']}")
                return False
                
            response = result['response'].json()
            if response.get('errors', False):
                logger.error(f"Bulk request had errors for file {file_key}")
                return False
            
            # Update processed count
            with self._lock:
                self._processed_count += len(batch)
                
            logger.debug(f"Successfully processed batch of {len(batch)} documents from {file_key}")
            return True
            
        except Exception as e:
            logger.error(f"Error processing batch from {file_key}: {str(e)}")
            return False

    def _process_batch_worker(self, index_name: str, file_key: str) -> None:
        """
        Worker function to process batches from the queue.
        
        Args:
            index_name (str): Name of the target index
            file_key (str): File key being processed
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
            except Exception as e:
                logger.error(f"Error in batch worker for {file_key}: {str(e)}")
                # Mark the task as done even if there was an error
                try:
                    self._batch_queue.task_done()
                except:
                    pass
                break

    def _process_csv_file(self, content: str, file_path: str) -> int:
        """Process CSV file content and return number of rows processed."""
        df = pd.read_csv(StringIO(content))
        logger.info(f"Found {len(df.columns)} columns in file: {file_path}")
        
        batch = []
        row_count = 0
        
        for _, row in df.iterrows():
            row_count += 1
            try:
                document = self._create_document(row)
                batch.append(document)
                
                if len(batch) >= self.batch_size:
                    self._batch_queue.put(batch.copy())
                    batch = []
                    time.sleep(0.1)
                    
            except (ValueError, KeyError) as e:
                logger.error(f"Error processing row {row_count} in file {file_path}: {str(e)}")
        
        if batch:
            self._batch_queue.put(batch)
            
        return row_count

    def _process_json_file(self, content: str, file_path: str) -> int:
        """Process JSON file content and return number of rows processed."""
        data = json.loads(content)
        if isinstance(data, dict):
            data = [data]
            
        batch = []
        row_count = 0
        
        for item in data:
            row_count += 1
            try:
                batch.append(item)
                
                if len(batch) >= self.batch_size:
                    self._batch_queue.put(batch.copy())
                    batch = []
                    time.sleep(0.1)
                    
            except (ValueError, TypeError) as e:
                logger.error(f"Error processing item {row_count} in file {file_path}: {str(e)}")
        
        if batch:
            self._batch_queue.put(batch)
            
        return row_count

    def _get_file_content(self, file_info: Dict[str, Any]) -> Tuple[str, str, str]:
        """Get file content and return (content, file_path, file_type)."""
        file_type = file_info.get("type", "")
        
        if "bucket" in file_info and "key" in file_info:
            bucket = file_info["bucket"]
            key = file_info["key"]
            file_path = f"{bucket}/{key}"
            logger.info(f"Processing S3 file: {file_path}")
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            content = response['Body'].read().decode('utf-8')
        else:
            file_path = file_info.get("file_path", "")
            if not file_path:
                raise ValueError(f"Invalid file information: {file_info}")
            logger.info(f"Processing local file: {file_path}")
            with open(file_path, 'r') as f:
                content = f.read()
                
        if not file_type:
            raise ValueError(f"Invalid file information: {file_info}")
            
        return content, file_path, file_type

    def process_file(self, file_info: Dict[str, Any], index_name: str, make_request_func: callable) -> int:
        """
        Process a single file (local or S3) and return number of processed rows.
        
        Args:
            file_info (Dict[str, Any]): File information
            index_name (str): Name of the target index
            make_request_func (callable): Function to make requests to OpenSearch
            
        Returns:
            int: Number of rows processed
        """
        self._make_request = make_request_func
        file_start_time = time.time()
        file_path = file_info.get("file_path", "")
        
        try:
            content, file_path, file_type = self._get_file_content(file_info)
            logger.info(f"Processing {file_type.upper()} file: {file_path}")
            
            row_count = 0
            if file_type.lower() == 'csv':
                row_count = self._process_csv_file(content, file_path)
            elif file_type.lower() == 'json':
                row_count = self._process_json_file(content, file_path)
            else:
                logger.error(f"Unsupported file type: {file_type}")
                return 0
            
            # Process batches
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = []
                for _ in range(self.max_workers):
                    self._batch_queue.put(None)
                    futures.append(executor.submit(self._process_batch_worker, index_name, file_path))
                
                for future in as_completed(futures):
                    try:
                        future.result()
                    except (ValueError, RuntimeError) as e:
                        logger.error(f"Worker thread error: {str(e)}")
            
            file_time = time.time() - file_start_time
            logger.info(f"Completed processing file {file_path} in {file_time:.2f} seconds")
            return row_count
                
        except (ValueError, IOError, json.JSONDecodeError) as e:
            logger.error(f"Error processing file {file_path}: {str(e)}")
            return 0 