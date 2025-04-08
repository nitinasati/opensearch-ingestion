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
from datetime import datetime
from dotenv import load_dotenv
from opensearch_base_manager import OpenSearchBaseManager

# Load environment variables
load_dotenv()

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
        self._processed_count_from_bulk = 0
        self._lock = threading.Lock()
        self.opensearch_manager = OpenSearchBaseManager()
        self.sqs_client = boto3.client('sqs')
        
        # Get SQS queue ARN from environment variable
        self.sqs_dlq_arn = os.getenv('SQS-DLQ-ARN')
        if not self.sqs_dlq_arn:
            logger.warning("SQS-DLQ-ARN environment variable not set. Error reporting to SQS will be disabled.")
        else:
            logger.info(f"Using SQS DLQ ARN: {self.sqs_dlq_arn}")
            
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

    def _send_error_to_sqs(self, error_payload: Dict[str, Any]) -> bool:
        """
        Send error information to SQS queue.
        
        Args:
            error_payload (Dict[str, Any]): Error information to send
            
        Returns:
            bool: True if message was sent successfully, False otherwise
        """
        # Check if SQS ARN is configured
        if not self.sqs_dlq_arn:
            logger.warning("SQS-DLQ-ARN not configured. Skipping error reporting to SQS.")
            return False
            
        try:
            # Extract queue URL from ARN
            # Format: arn:aws:sqs:region:account-id:queue-name
            arn_parts = self.sqs_dlq_arn.split(':')
            if len(arn_parts) < 6:
                logger.error(f"Invalid SQS ARN format: {self.sqs_dlq_arn}")
                return False
                
            region = arn_parts[3]
            account_id = arn_parts[4]
            queue_name = arn_parts[5]
            
            # Get queue URL
            queue_url = f"https://sqs.{region}.amazonaws.com/{account_id}/{queue_name}"
            
            # Add timestamp to the payload
            error_payload['timestamp'] = datetime.now().isoformat()
            
            # Check if the message size exceeds 230 KB (235,520 bytes)
            message_json = json.dumps(error_payload)
            message_size = len(message_json.encode('utf-8'))
            
            if message_size > 235520:  # 230 KB in bytes
                logger.info(f"Error message size ({message_size} bytes) exceeds 230 KB limit. Splitting into multiple messages.")
                
                # Extract the failed records
                failed_records = error_payload.get('failed_records', [])
                error_message = error_payload.get('error_message', '')
                file_key = error_payload.get('file_key', '')
                source = error_payload.get('source', '')
                timestamp = error_payload.get('timestamp', '')
                
                # Calculate how many records we can fit in each message
                # Start with a base payload without records
                base_payload = {
                    'error_message': error_message,
                    'file_key': file_key,
                    'source': source,
                    'timestamp': timestamp,
                    'total_records': len(failed_records),
                    'message_part': 0,
                    'total_parts': 0  # Will be calculated
                }
                
                # Calculate the size of the base payload
                base_size = len(json.dumps(base_payload).encode('utf-8'))
                
                # Calculate how many records we can fit in each message
                # Leave some buffer for the record structure and overhead
                available_size = 235520 - base_size - 1000  # 1 KB buffer
                
                # Estimate size per record (average)
                if failed_records:
                    sample_record = json.dumps(failed_records[0]).encode('utf-8')
                    avg_record_size = len(sample_record)
                    records_per_message = max(1, available_size // avg_record_size)
                else:
                    records_per_message = 1
                
                # Calculate total number of parts
                total_parts = (len(failed_records) + records_per_message - 1) // records_per_message
                
                # Send each part
                for i in range(total_parts):
                    start_idx = i * records_per_message
                    end_idx = min((i + 1) * records_per_message, len(failed_records))
                    
                    part_payload = base_payload.copy()
                    part_payload['failed_records'] = failed_records[start_idx:end_idx]
                    part_payload['message_part'] = i + 1
                    part_payload['total_parts'] = total_parts
                    
                    # Send the part
                    response = self.sqs_client.send_message(
                        QueueUrl=queue_url,
                        MessageBody=json.dumps(part_payload)
                    )
                    
                    logger.info(f"Sent part {i+1}/{total_parts} of error message to SQS: {response.get('MessageId')}")
                
                return True
            else:
                # Send the entire message as is
                response = self.sqs_client.send_message(
                    QueueUrl=queue_url,
                    MessageBody=message_json
                )
                
                logger.info(f"Error message sent to SQS queue: {response.get('MessageId')}")
                return True
                
        except Exception as e:
            logger.error(f"Failed to send error message to SQS: {str(e)}")
            return False

    def _print_error_records(self, failed_records: List[Dict[str, Any]], file_key: str) -> None:
        """
        Print detailed information about failed records.
        
        Args:
            failed_records (List[Dict[str, Any]]): List of failed records with error details
            file_key (str): File identifier for logging
        """
        if not failed_records:
            return
            
        error_message = f"Bulk request had {len(failed_records)} failed records for file {file_key}"
         
        # Create a complete error payload with all failed records
        error_payload = {
            'error_message': error_message,
            'failed_records': failed_records,
            'file_key': file_key,
            'source': 'opensearch_ingestion'
        }
        
        # Send error to SQS queue
        self._send_error_to_sqs(error_payload)
        
     
        # Also log individual records for better readability
        for record in failed_records:
            logger.error(f"Failed document ID: {record['document_id']}")
            logger.error(f"Error type: {record['error_type']}")
            logger.error(f"Error reason: {record['error_reason']}")
            logger.error("---")

    def _process_batch(self, batch: List[Dict[str, Any]], index_name: str, file_key: str) -> bool:
        """
        Process a batch of documents.
        
        Args:
            batch (List[Dict[str, Any]]): List of documents to process
            index_name (str): Name of the target index
            file_key (str): File identifier for logging
            
        Returns:
            bool: True if batch was processed successfully
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
                
            # Only try to access response.json() if status is success
            response = result['response'].json()
            if response.get('errors', False):
                # Extract failed records
                failed_records = []
                for i, item in enumerate(response.get('items', [])):
                    index_result = item.get('index', {})
                    if index_result.get('status', 200) >= 400:  # Error status codes are 400 and above
                        error_info = {
                            'document_id': index_result.get('_id', 'unknown'),
                            'error_type': index_result.get('error', {}).get('type', 'unknown'),
                            'error_reason': index_result.get('error', {}).get('reason', 'unknown'),
                            'document': batch[i] if i < len(batch) else 'unknown'
                        }
                        failed_records.append(error_info)
                
                # Print error records using the dedicated function
                if failed_records:
                    self._print_error_records(failed_records, file_key)
                return False

            if 'items' in response:
                processed_count = len(response['items'])
                logger.info(f"Processed {processed_count} records from bulk request for file {file_key}")
            
            # Update processed count with the actual count from the response
            with self._lock:
                self._processed_count_from_bulk += processed_count
                
            return True
            
        except Exception as e:
            logger.error(f"Error processing batch for file {file_key}: {str(e)}")
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
                
                success = self._process_batch(batch, index_name, file_key)
                logger.info(f"Batch of length {len(batch)} processed for file {file_key}")
                if not success:
                    logger.warning(f"Failed to process batch for file {file_key}")
                self._batch_queue.task_done()
                
            except Queue.Empty:
                break
            except Exception as e:
                logger.error(f"Error in batch worker for {file_key}: {str(e)}")
                # Mark the task as done even if there was an error
                try:
                    self._batch_queue.task_done()
                except Exception:
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
                    logger.info(f"Putting batch of {len(batch)} documents into queue")
                    self._batch_queue.put(batch.copy())
                    batch = []
                    time.sleep(0.1)
                    
            except (ValueError, KeyError) as e:
                logger.error(f"Error processing row {row_count} in file {file_path}: {str(e)}")
        
        if batch:
            logger.info(f"Putting the lasts batch of {len(batch)} documents into queue")
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
                    except (RuntimeError) as e:
                        logger.error(f"Worker thread error: {str(e)}")
            
            file_time = time.time() - file_start_time
            logger.info(f"Completed processing file {file_path} in {file_time:.2f} seconds")
            return row_count
                
        except (IOError, json.JSONDecodeError) as e:
            logger.error(f"Error processing file {file_path}: {str(e)}")
            return 0 