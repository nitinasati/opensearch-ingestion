"""
Unit tests for the FileProcessor class.

This module contains tests for the file processing functionality, including
local and S3 file handling, batch processing, and document creation.
"""

import unittest
from unittest.mock import patch, MagicMock, call
import json
import os
import pandas as pd
from io import StringIO
from file_processor import FileProcessor

class TestFileProcessor(unittest.TestCase):
    """Test cases for the FileProcessor class."""
    
    def setUp(self):
        """Set up test environment."""
        # Create a mock for boto3.client
        self.s3_client_mock = MagicMock()
        
        # Apply patches
        self.boto3_patcher = patch('boto3.client', return_value=self.s3_client_mock)
        self.boto3_patcher.start()
        
        # Initialize the processor
        self.processor = FileProcessor(batch_size=5, max_workers=2)
        
        # Create mock make_request function
        self.mock_make_request = MagicMock(return_value={
            'status': 'success',
            'response': MagicMock(json=lambda: {'errors': False, 'items': []})
        })
    
    def tearDown(self):
        """Clean up after tests."""
        self.boto3_patcher.stop()
    
    def test_init(self):
        """Test initialization of the FileProcessor class."""
        self.assertEqual(self.processor.batch_size, 5)
        self.assertEqual(self.processor.max_workers, 2)
    
    def test_get_files_by_type(self):
        """Test getting files by type."""
        # Mock os.listdir and os.path.isfile
        with patch('os.listdir', return_value=['file1.csv', 'file2.json', 'file3.txt']):
            with patch('os.path.isfile', return_value=True):
                with patch('os.path.join', side_effect=lambda *args: '/'.join(args)):
                    csv_files, json_files = self.processor._get_files_by_type('test-folder')
                    
                    self.assertEqual(len(csv_files), 1)
                    self.assertEqual(len(json_files), 1)
                    self.assertEqual(csv_files[0], 'test-folder/file1.csv')
                    self.assertEqual(json_files[0], 'test-folder/file2.json')
    
    def test_process_local_folder_success(self):
        """Test processing local folder successfully."""
        # Mock the _get_files_by_type method
        with patch.object(self.processor, '_get_files_by_type', return_value=(
            ['test-folder/file1.csv'], ['test-folder/file2.json']
        )):
            result = self.processor.process_local_folder('test-folder')
            
            self.assertEqual(result['status'], 'success')
            self.assertEqual(len(result['files']), 2)
            self.assertEqual(result['csv_files'], ['test-folder/file1.csv'])
            self.assertEqual(result['json_files'], ['test-folder/file2.json'])
    
    def test_process_local_folder_no_files(self):
        """Test processing local folder with no files."""
        # Mock the _get_files_by_type method
        with patch.object(self.processor, '_get_files_by_type', return_value=([], [])):
            result = self.processor.process_local_folder('test-folder')
            
            self.assertEqual(result['status'], 'warning')
            # The actual message contains the full path, so we'll just check that it contains the folder name
            self.assertIn('test-folder', result['message'])
            self.assertEqual(result['files'], [])
    
    def test_process_local_folder_error(self):
        """Test processing local folder with an error."""
        # Mock the _get_files_by_type method to raise an exception
        with patch.object(self.processor, '_get_files_by_type', side_effect=Exception('Test error')):
            result = self.processor.process_local_folder('test-folder')
            
            self.assertEqual(result['status'], 'error')
            # The actual message contains the full path, so we'll just check that it contains the folder name
            self.assertIn('test-folder', result['message'])
            self.assertEqual(result['files'], [])
    
    def test_process_s3_files_success(self):
        """Test processing S3 files successfully."""
        # Mock the S3 client
        self.s3_client_mock.get_paginator.return_value.paginate.return_value = [
            {'Contents': [
                {'Key': 'file1.csv', 'Size': 100},
                {'Key': 'file2.json', 'Size': 200},
                {'Key': 'file3.txt', 'Size': 300}
            ]}
        ]
        
        result = self.processor.process_s3_files('test-bucket', 'test-prefix')
        
        self.assertEqual(result['status'], 'success')
        self.assertEqual(len(result['files']), 2)
        self.assertEqual(result['csv_files'][0]['key'], 'file1.csv')
        self.assertEqual(result['json_files'][0]['key'], 'file2.json')
    
    def test_process_s3_files_no_files(self):
        """Test processing S3 files with no files."""
        # Mock the S3 client
        self.s3_client_mock.get_paginator.return_value.paginate.return_value = [
            {'Contents': [
                {'Key': 'file3.txt', 'Size': 300}
            ]}
        ]
        
        result = self.processor.process_s3_files('test-bucket', 'test-prefix')
        
        self.assertEqual(result['status'], 'warning')
        self.assertEqual(result['message'], 'No CSV or JSON files found in S3 bucket test-bucket with prefix test-prefix')
        self.assertEqual(result['files'], [])
    
    def test_process_s3_files_error(self):
        """Test processing S3 files with an error."""
        # Mock the S3 client to raise an exception
        self.s3_client_mock.get_paginator.side_effect = Exception('Test error')
        
        result = self.processor.process_s3_files('test-bucket', 'test-prefix')
        
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Error scanning S3 bucket test-bucket with prefix test-prefix: Test error')
        self.assertEqual(result['files'], [])
    
    def test_create_document(self):
        """Test creating a document from a CSV row."""
        # Create a test DataFrame row
        df = pd.DataFrame({
            'id': [1],
            'name': ['test'],
            'value': [42.5],
            'empty': [None]
        })
        row = df.iloc[0]
        
        document = self.processor._create_document(row)
        
        self.assertEqual(document['id'], 1)
        self.assertEqual(document['name'], 'test')
        self.assertEqual(document['value'], 42.5)
        self.assertIsNone(document['empty'])
    
    def test_create_bulk_request(self):
        """Test creating a bulk request."""
        # Create test documents
        documents = [
            {'id': 1, 'name': 'test1'},
            {'id': 2, 'name': 'test2'}
        ]
        
        bulk_request = self.processor._create_bulk_request(documents, 'test-index')
        
        # Check that the bulk request is correctly formatted
        lines = bulk_request.strip().split('\n')
        self.assertEqual(len(lines), 4)  # 2 index actions + 2 documents
        
        # Check the index actions
        index_action1 = json.loads(lines[0])
        self.assertEqual(index_action1['index']['_index'], 'test-index')
        self.assertEqual(index_action1['index']['_id'], 1)
        
        index_action2 = json.loads(lines[2])
        self.assertEqual(index_action2['index']['_index'], 'test-index')
        self.assertEqual(index_action2['index']['_id'], 2)
        
        # Check the documents
        document1 = json.loads(lines[1])
        self.assertEqual(document1['id'], 1)
        self.assertEqual(document1['name'], 'test1')
        
        document2 = json.loads(lines[3])
        self.assertEqual(document2['id'], 2)
        self.assertEqual(document2['name'], 'test2')
    
    def test_process_file_csv(self):
        """Test processing a CSV file."""
        # Mock the necessary functions
        with patch.object(self.processor, '_get_file_content', return_value=('test content', 'test-file.csv', 'csv')):
            with patch.object(self.processor, '_process_csv_file', return_value=10):
                with patch('concurrent.futures.ThreadPoolExecutor') as executor_mock:
                    executor_mock.return_value.__enter__.return_value.submit.return_value.result.return_value = None
                    
                    # Process the file
                    rows_processed, processed_count = self.processor.process_file({
                        'file_path': 'test-file.csv',
                        'type': 'csv'
                    }, 'test-index', lambda *args, **kwargs: {'status': 'success'})
                    
                    # Check that the correct number of rows were processed
                    self.assertEqual(rows_processed, 10)
                    self.assertEqual(processed_count, 0)  # Since we mocked the worker threads
    
    def test_process_file_json(self):
        """Test processing a JSON file."""
        # Mock the necessary functions
        with patch.object(self.processor, '_get_file_content', return_value=('test content', 'test-file.json', 'json')):
            with patch.object(self.processor, '_process_json_file', return_value=10):
                with patch('concurrent.futures.ThreadPoolExecutor') as executor_mock:
                    executor_mock.return_value.__enter__.return_value.submit.return_value.result.return_value = None
                    
                    # Process the file
                    rows_processed, processed_count = self.processor.process_file({
                        'file_path': 'test-file.json',
                        'type': 'json'
                    }, 'test-index', lambda *args, **kwargs: {'status': 'success'})
                    
                    # Check that the correct number of rows were processed
                    self.assertEqual(rows_processed, 10)
                    self.assertEqual(processed_count, 0)  # Since we mocked the worker threads
    
    def test_process_file_error(self):
        """Test error handling in process_file."""
        file_info = {"file_path": "test.csv", "type": "invalid"}
        result = self.processor.process_file(file_info, "test-index", self.mock_make_request)
        self.assertEqual(result, (0, 0))
    
    def test_process_batch_success(self):
        """Test processing a batch successfully."""
        # Set the _make_request attribute on the processor
        self.processor._make_request = MagicMock()
        
        # Create a mock response with proper json method
        mock_response = MagicMock()
        mock_response.json.return_value = {'errors': False, 'items': [{'index': {'status': 200}}, {'index': {'status': 200}}]}
        
        # Mock the _make_request function
        with patch.object(self.processor, '_make_request', return_value={
            'status': 'success',
            'response': mock_response
        }):
            # Create test batch
            batch = [
                {'id': 1, 'name': 'test1'},
                {'id': 2, 'name': 'test2'}
            ]
            
            result = self.processor._process_batch(batch, 'test-index', 'test-file')
            
            self.assertTrue(result)
            self.assertEqual(self.processor._processed_count_from_bulk, 2)
    
    def test_process_batch_error(self):
        """Test processing a batch with an error."""
        # Set the _make_request attribute on the processor
        self.processor._make_request = MagicMock()
        
        # Mock the _make_request function
        with patch.object(self.processor, '_make_request', return_value={
            'status': 'error',
            'message': 'Test error'
        }):
            # Create test batch
            batch = [
                {'id': 1, 'name': 'test1'},
                {'id': 2, 'name': 'test2'}
            ]
            
            result = self.processor._process_batch(batch, 'test-index', 'test-file')
            
            self.assertFalse(result)
            self.assertEqual(self.processor._processed_count_from_bulk, 0)
    
    def test_process_batch_worker(self):
        """Test the batch worker function."""
        # Initialize the processed count
        self.processor._processed_count_from_bulk = 0
        
        # Add a batch to the queue
        self.processor._batch_queue.put([
            {'id': 1, 'name': 'test1'},
            {'id': 2, 'name': 'test2'}
        ])
        
        # Add a None to signal the worker to stop
        self.processor._batch_queue.put(None)
        
        # Mock the _make_request function
        self.processor._make_request = MagicMock(return_value={
            'status': 'success',
            'response': MagicMock(json=lambda: {'errors': False, 'items': [{'index': {'status': 200}}, {'index': {'status': 200}}]})
        })
        
        # Mock the _process_batch function to update the counter
        def mock_process_batch(batch, index_name, file_name):
            self.processor._processed_count_from_bulk += len(batch)
            return True
            
        with patch.object(self.processor, '_process_batch', side_effect=mock_process_batch):
            # Run the worker
            self.processor._process_batch_worker('test-index', 'test-file')
            
            # Check that the batch was processed
            self.processor._process_batch.assert_called_once()
            self.assertEqual(self.processor._processed_count_from_bulk, 2)
    
    def test_process_batch_with_failed_records(self):
        """Test processing a batch with failed records."""
        # Set the _make_request attribute on the processor
        self.processor._make_request = MagicMock()
        
        # Create a mock response with failed records
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'errors': True,
            'items': [
                {'index': {'status': 200, '_id': '1'}},
                {'index': {
                    'status': 400,
                    '_id': '2',
                    'error': {
                        'type': 'mapper_parsing_exception',
                        'reason': 'failed to parse field'
                    }
                }},
                {'index': {
                    'status': 500,
                    '_id': '3',
                    'error': {
                        'type': 'internal_error',
                        'reason': 'server error'
                    }
                }}
            ]
        }
        
        # Mock the _make_request function
        with patch.object(self.processor, '_make_request', return_value={
            'status': 'success',
            'response': mock_response
        }):
            # Create test batch
            batch = [
                {'id': '1', 'name': 'test1'},
                {'id': '2', 'name': 'test2'},
                {'id': '3', 'name': 'test3'}
            ]
            
            # Mock the _print_error_records method to verify it's called
            with patch.object(self.processor, '_print_error_records') as mock_print_errors:
                result = self.processor._process_batch(batch, 'test-index', 'test-file')
                
                # Verify the result
                self.assertTrue(result)
                self.assertEqual(self.processor._processed_count_from_bulk, 1)  # Only one successful record
                
                # Verify error records were collected and printed
                mock_print_errors.assert_called_once()
                call_args = mock_print_errors.call_args[0]
                self.assertEqual(len(call_args[0]), 2)  # Two failed records
                self.assertEqual(call_args[1], 'test-file')  # File name
                
                # Verify error record details
                error_records = call_args[0]
                self.assertEqual(error_records[0]['document_id'], '2')
                self.assertEqual(error_records[0]['error_type'], 'mapper_parsing_exception')
                self.assertEqual(error_records[1]['document_id'], '3')
                self.assertEqual(error_records[1]['error_type'], 'internal_error')
    
    def test_process_csv_file_success(self):
        """Test successful CSV file processing."""
        # Create test CSV content
        csv_content = "id,name,value\n1,test1,100\n2,test2,200\n3,test3,300"
        
        # Mock the batch queue to verify documents are added
        with patch.object(self.processor, '_batch_queue') as mock_queue:
            # Process the CSV content
            row_count = self.processor._process_csv_file(csv_content, 'test.csv')
            
            # Verify the row count
            self.assertEqual(row_count, 3)
            
            # Verify that batches were added to the queue
            # Since batch_size is 5 and we have 3 rows, only one batch should be added
            mock_queue.put.assert_called_once()
            batch = mock_queue.put.call_args[0][0]
            self.assertEqual(len(batch), 3)
            
            # Verify document content
            self.assertEqual(batch[0]['id'], 1)
            self.assertEqual(batch[0]['name'], 'test1')
            self.assertEqual(batch[0]['value'], 100)
            
    def test_process_csv_file_with_batching(self):
        """Test CSV file processing with multiple batches."""
        # Create test CSV content with more rows than batch size
        csv_content = "id,name,value\n" + "\n".join([f"{i},test{i},{i*100}" for i in range(1, 8)])
        
        # Mock the batch queue to verify documents are added
        with patch.object(self.processor, '_batch_queue') as mock_queue:
            # Process the CSV content
            row_count = self.processor._process_csv_file(csv_content, 'test.csv')
            
            # Verify the row count
            self.assertEqual(row_count, 7)
            
            # Verify that two batches were added to the queue
            # First batch should have 5 rows (batch_size)
            # Second batch should have 2 rows
            self.assertEqual(mock_queue.put.call_count, 2)
            
            # Verify first batch
            first_batch = mock_queue.put.call_args_list[0][0][0]
            self.assertEqual(len(first_batch), 5)
            
            # Verify second batch
            second_batch = mock_queue.put.call_args_list[1][0][0]
            self.assertEqual(len(second_batch), 2)
            
    def test_process_csv_file_error_handling(self):
        """Test CSV file processing with error handling."""
        # Create test CSV content with invalid data
        csv_content = "id,name,value\n1,test1,100\n2,test2,invalid\n3,test3,300"
        
        # Mock the batch queue to verify documents are added
        with patch.object(self.processor, '_batch_queue') as mock_queue:
            # Process the CSV content
            row_count = self.processor._process_csv_file(csv_content, 'test.csv')
            
            # Verify the row count (should count all rows)
            self.assertEqual(row_count, 3)
            
            # Verify that all documents were added to the queue
            mock_queue.put.assert_called_once()
            batch = mock_queue.put.call_args[0][0]
            self.assertEqual(len(batch), 3)  # All rows are processed
            
            # Verify document content
            self.assertEqual(batch[0]['id'], 1)
            self.assertEqual(batch[0]['name'], 'test1')
            self.assertEqual(batch[0]['value'], '100')  # CSV values are read as strings
            
            # The invalid value should be converted to a string
            self.assertEqual(batch[1]['id'], 2)
            self.assertEqual(batch[1]['name'], 'test2')
            self.assertEqual(batch[1]['value'], 'invalid')
            
            self.assertEqual(batch[2]['id'], 3)
            self.assertEqual(batch[2]['name'], 'test3')
            self.assertEqual(batch[2]['value'], '300')  # CSV values are read as strings

if __name__ == '__main__':
    unittest.main() 