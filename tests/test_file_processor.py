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
    
    def test_process_batch_success(self):
        """Test processing a batch successfully."""
        # Set the _make_request attribute on the processor
        self.processor._make_request = MagicMock()
        
        # Mock the _make_request function
        with patch.object(self.processor, '_make_request', return_value={
            'status': 'success',
            'response': MagicMock(
                json=lambda: {'errors': False}
            )
        }):
            # Create test batch
            batch = [
                {'id': 1, 'name': 'test1'},
                {'id': 2, 'name': 'test2'}
            ]
            
            result = self.processor._process_batch(batch, 'test-index', 'test-file')
            
            self.assertTrue(result)
    
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
    
    def test_process_batch_worker(self):
        """Test the batch worker function."""
        # Mock the _process_batch function
        with patch.object(self.processor, '_process_batch', return_value=True):
            # Add a batch to the queue
            self.processor._batch_queue.put([
                {'id': 1, 'name': 'test1'},
                {'id': 2, 'name': 'test2'}
            ])
            
            # Add a None to signal the worker to stop
            self.processor._batch_queue.put(None)
            
            # Run the worker
            self.processor._process_batch_worker('test-index', 'test-file')
            
            # Check that the batch was processed
            self.processor._process_batch.assert_called_once()
    
    def test_process_csv_file(self):
        """Test processing a CSV file."""
        # Mock the _batch_queue.put method
        with patch.object(self.processor._batch_queue, 'put'):
            # Create a test CSV content
            csv_content = 'id,name\n1,test1\n2,test2\n3,test3\n4,test4\n5,test5\n6,test6'
            
            # Process the CSV file
            row_count = self.processor._process_csv_file(csv_content, 'test-file.csv')
            
            # Check that the correct number of rows were processed
            self.assertEqual(row_count, 6)
            
            # Check that the batches were added to the queue
            self.assertEqual(self.processor._batch_queue.put.call_count, 2)  # 5 rows in first batch, 1 in second
    
    def test_process_json_file(self):
        """Test processing a JSON file."""
        # Mock the _batch_queue.put method
        with patch.object(self.processor._batch_queue, 'put'):
            # Create a test JSON content
            json_content = json.dumps([
                {'id': 1, 'name': 'test1'},
                {'id': 2, 'name': 'test2'},
                {'id': 3, 'name': 'test3'},
                {'id': 4, 'name': 'test4'},
                {'id': 5, 'name': 'test5'},
                {'id': 6, 'name': 'test6'}
            ])
            
            # Process the JSON file
            row_count = self.processor._process_json_file(json_content, 'test-file.json')
            
            # Check that the correct number of rows were processed
            self.assertEqual(row_count, 6)
            
            # Check that the batches were added to the queue
            self.assertEqual(self.processor._batch_queue.put.call_count, 2)  # 5 rows in first batch, 1 in second
    
    def test_get_file_content_local(self):
        """Test getting content from a local file."""
        # Mock the open function
        with patch('builtins.open', new_callable=MagicMock) as mock_open:
            mock_open.return_value.__enter__.return_value.read.return_value = 'test content'
            
            content, file_path, file_type = self.processor._get_file_content({
                'file_path': 'test-file.csv',
                'type': 'csv'
            })
            
            self.assertEqual(content, 'test content')
            self.assertEqual(file_path, 'test-file.csv')
            self.assertEqual(file_type, 'csv')
    
    def test_get_file_content_s3(self):
        """Test getting content from an S3 file."""
        # Mock the S3 client
        self.s3_client_mock.get_object.return_value = {
            'Body': MagicMock(read=lambda: b'test content')
        }
        
        content, file_path, file_type = self.processor._get_file_content({
            'bucket': 'test-bucket',
            'key': 'test-file.csv',
            'type': 'csv'
        })
        
        self.assertEqual(content, 'test content')
        self.assertEqual(file_path, 'test-bucket/test-file.csv')
        self.assertEqual(file_type, 'csv')
    
    def test_process_file_csv(self):
        """Test processing a CSV file."""
        # Mock the necessary functions
        with patch.object(self.processor, '_get_file_content', return_value=('test content', 'test-file.csv', 'csv')):
            with patch.object(self.processor, '_process_csv_file', return_value=10):
                with patch('concurrent.futures.ThreadPoolExecutor') as executor_mock:
                    executor_mock.return_value.__enter__.return_value.submit.return_value.result.return_value = None
                    
                    # Process the file
                    row_count = self.processor.process_file({
                        'file_path': 'test-file.csv',
                        'type': 'csv'
                    }, 'test-index', lambda *args, **kwargs: {'status': 'success'})
                    
                    # Check that the correct number of rows were processed
                    self.assertEqual(row_count, 10)
    
    def test_process_file_json(self):
        """Test processing a JSON file."""
        # Mock the necessary functions
        with patch.object(self.processor, '_get_file_content', return_value=('test content', 'test-file.json', 'json')):
            with patch.object(self.processor, '_process_json_file', return_value=10):
                with patch('concurrent.futures.ThreadPoolExecutor') as executor_mock:
                    executor_mock.return_value.__enter__.return_value.submit.return_value.result.return_value = None
                    
                    # Process the file
                    row_count = self.processor.process_file({
                        'file_path': 'test-file.json',
                        'type': 'json'
                    }, 'test-index', lambda *args, **kwargs: {'status': 'success'})
                    
                    # Check that the correct number of rows were processed
                    self.assertEqual(row_count, 10)
    
    def test_process_file_error(self):
        """Test processing a file with an error."""
        # Mock the _get_file_content function to raise an IOError
        with patch.object(self.processor, '_get_file_content', side_effect=IOError('Test error')):
            # Process the file
            row_count = self.processor.process_file({
                'file_path': 'test-file.csv',
                'type': 'csv'
            }, 'test-index', lambda *args, **kwargs: {'status': 'success'})
            
            # Check that the error was handled
            self.assertEqual(row_count, 0)

if __name__ == '__main__':
    unittest.main() 