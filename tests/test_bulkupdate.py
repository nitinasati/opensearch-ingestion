"""
Unit tests for the OpenSearchBulkIngestion class and main function.

This module contains tests for bulk ingestion operations, including
the main function that handles command line arguments and orchestrates
the data ingestion process.
"""

import unittest
from unittest.mock import patch, MagicMock, call
import json
import os
import pandas as pd
from io import StringIO
from datetime import datetime
from bulkupdate import OpenSearchBulkIngestion, NO_FILES_MESSAGE, TRACKING_FILE, main

class TestOpenSearchBulkIngestion(unittest.TestCase):
    """Test cases for the OpenSearchBulkIngestion class."""
    
    def setUp(self):
        """Set up test environment."""
        # Mock environment variables
        self.env_patcher = patch.dict('os.environ', {
            'OPENSEARCH_ENDPOINT': 'test-endpoint',
            'AWS_REGION': 'us-east-1',
            'VERIFY_SSL': 'false'
        })
        self.env_patcher.start()
        
        # Create a mock for boto3.Session
        self.session_mock = MagicMock()
        self.credentials_mock = MagicMock()
        self.credentials_mock.access_key = 'test-access-key'
        self.credentials_mock.secret_key = 'test-secret-key'
        self.credentials_mock.token = 'test-token'
        self.session_mock.get_credentials.return_value = self.credentials_mock
        
        # Create a mock for AWS4Auth
        self.auth_mock = MagicMock()
        
        # Create a mock for requests
        self.requests_mock = MagicMock()
        self.response_mock = MagicMock()
        self.response_mock.status_code = 200
        self.response_mock.json.return_value = {'acknowledged': True}
        self.requests_mock.get.return_value = self.response_mock
        self.requests_mock.post.return_value = self.response_mock
        self.requests_mock.put.return_value = self.response_mock
        self.requests_mock.delete.return_value = self.response_mock
        
        # Apply patches
        self.boto3_patcher = patch('boto3.Session', return_value=self.session_mock)
        self.aws4auth_patcher = patch('requests_aws4auth.AWS4Auth', return_value=self.auth_mock)
        self.requests_patcher = patch('requests.get', self.requests_mock.get)
        self.requests_post_patcher = patch('requests.post', self.requests_mock.post)
        self.requests_put_patcher = patch('requests.put', self.requests_mock.put)
        self.requests_delete_patcher = patch('requests.delete', self.requests_mock.delete)
        
        self.boto3_patcher.start()
        self.aws4auth_patcher.start()
        self.requests_patcher.start()
        self.requests_post_patcher.start()
        self.requests_put_patcher.start()
        self.requests_delete_patcher.start()
        
        # Mock the OpenSearchIndexManager
        self.index_manager_mock = MagicMock()
        self.index_manager_mock.validate_and_cleanup_index.return_value = {
            'status': 'success',
            'message': 'Index validated and cleaned up successfully'
        }
        
        # Mock the FileProcessor
        self.file_processor_mock = MagicMock()
        self.file_processor_mock.process_file.return_value = 10
        
        # Apply additional patches
        self.index_manager_patcher = patch('bulkupdate.OpenSearchIndexManager', return_value=self.index_manager_mock)
        self.file_processor_patcher = patch('bulkupdate.FileProcessor', return_value=self.file_processor_mock)
        
        self.index_manager_patcher.start()
        self.file_processor_patcher.start()
        
        # Initialize the manager
        self.manager = OpenSearchBulkIngestion(batch_size=1000, max_workers=2)
    
    def tearDown(self):
        """Clean up after tests."""
        self.env_patcher.stop()
        self.boto3_patcher.stop()
        self.aws4auth_patcher.stop()
        self.requests_patcher.stop()
        self.requests_post_patcher.stop()
        self.requests_put_patcher.stop()
        self.requests_delete_patcher.stop()
        self.index_manager_patcher.stop()
        self.file_processor_patcher.stop()
    
    def test_init(self):
        """Test initialization of the OpenSearchBulkIngestion class."""
        self.assertIsNotNone(self.manager)
        self.assertEqual(self.manager.batch_size, 1000)
        self.assertEqual(self.manager.max_workers, 2)
    
    def test_verify_document_count_success(self):
        """Test successful document count verification."""
        # Mock a successful count response
        count_response = MagicMock()
        count_response.status_code = 200
        count_response.json.return_value = {'count': 42}
        self.requests_mock.post.return_value = count_response
        
        # Mock the _processed_count_from_bulk attribute
        self.file_processor_mock._processed_count_from_bulk = 42
        
        result = self.manager._verify_document_count('test-index', 42)
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['message'], 'Document count verification successful: 42 documents match expected count')
        self.assertEqual(result['actual_count'], 42)
        self.assertEqual(result['expected_count'], 42)
    
    def test_verify_document_count_mismatch(self):
        """Test document count verification with a mismatch."""
        # Mock a successful count response with a different count
        count_response = MagicMock()
        count_response.status_code = 200
        count_response.json.return_value = {'count': 40}
        self.requests_mock.post.return_value = count_response
        
        # Mock the _processed_count_from_bulk attribute
        self.file_processor_mock._processed_count_from_bulk = 40
        
        result = self.manager._verify_document_count('test-index', 42)
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Document count mismatch: Expected 42, got 40')
        self.assertEqual(result['actual_count'], 40)
        self.assertEqual(result['expected_count'], 42)
    
    def test_get_processed_files(self):
        """Test getting processed files."""
        # Mock file reading
        with patch('builtins.open', new_callable=MagicMock) as mock_open:
            mock_open.return_value.__enter__.return_value.read.return_value = json.dumps({
                'test-index': ['file1.csv', 'file2.csv']
            })
            
            files = self.manager._get_processed_files('test-index')
            self.assertEqual(files, ['file1.csv', 'file2.csv'])
    
    def test_update_processed_files(self):
        """Test updating processed files."""
        # Mock file reading and writing
        with patch('builtins.open', new_callable=MagicMock) as mock_open:
            # Setup the mock to return a file-like object
            mock_file = MagicMock()
            mock_open.return_value.__enter__.return_value = mock_file
            
            # Setup the read method to return the initial data
            mock_file.read.return_value = json.dumps({
                'test-index': ['file1.csv']
            })
            
            # Call the method
            self.manager._update_processed_files('test-index', 'file2.csv')
            
            # Verify file operations
            mock_open.assert_any_call(TRACKING_FILE, 'r')
            mock_open.assert_any_call(TRACKING_FILE, 'w')
            
            # Get all write calls and combine them
            written_data = ''.join(call.args[0] for call in mock_file.write.call_args_list)
            
            # Parse and verify the written data
            data = json.loads(written_data)
            self.assertEqual(data['test-index'], ['file1.csv', 'file2.csv'])
    
    def test_clear_processed_files(self):
        """Test clearing processed files."""
        # Create test data
        test_data = {
            'test-index': ['file1.csv', 'file2.csv'],
            'other-index': ['file3.csv']
        }
        expected_data = {
            'test-index': [],
            'other-index': ['file3.csv']
        }
        
        # Mock file operations
        mock_open = MagicMock()
        mock_file = MagicMock()
        mock_open.return_value.__enter__.return_value = mock_file
        mock_file.read.return_value = json.dumps(test_data)
        
        with patch('builtins.open', mock_open):
            # Call the method
            self.manager._clear_processed_files('test-index')
            
            # Verify file operations
            mock_open.assert_any_call(TRACKING_FILE, 'r')
            mock_open.assert_any_call(TRACKING_FILE, 'w')
            
            # Verify that write was called with the correct data
            # Combine all write calls into a single string
            written_data = ''.join(call.args[0] for call in mock_file.write.call_args_list)
            
            # Parse and verify the written data
            written_json = json.loads(written_data)
            self.assertEqual(written_json, expected_data)
    
    def test_get_file_identifier(self):
        """Test getting file identifier."""
        # Test with S3 file
        s3_file = {
            'bucket': 'test-bucket',
            'key': 'test/path/file.csv'
        }
        identifier = self.manager._get_file_identifier(s3_file)
        self.assertEqual(identifier, 'test-bucket/test/path/file.csv')
        
        # Test with local file
        local_file = {
            'file_path': '/path/to/file.csv'
        }
        identifier = self.manager._get_file_identifier(local_file)
        self.assertEqual(identifier, '/path/to/file.csv')
    
    def test_determine_file_type(self):
        """Test determining file type."""
        # Test CSV file
        file_type = self.manager._determine_file_type('test.csv')
        self.assertEqual(file_type, 'csv')
        
        # Test JSON file
        file_type = self.manager._determine_file_type('test.json')
        self.assertEqual(file_type, 'json')
        
        # Test unknown file type
        file_type = self.manager._determine_file_type('test.txt')
        self.assertEqual(file_type, 'unknown')
    
    def test_process_files(self):
        """Test processing files."""
        # Mock the file processor
        self.file_processor_mock.process_file.return_value = 10
        
        # Create test files
        all_files = [
            {'file_path': 'file1.csv', 'type': 'csv'},
            {'file_path': 'file2.json', 'type': 'json'}
        ]
        
        # Mock the processed files
        with patch.object(self.manager, '_get_processed_files', return_value=[]):
            total_rows, total_files = self.manager._process_files(all_files, 'test-index')
            
            self.assertEqual(total_rows, 20)  # 10 rows per file
            self.assertEqual(total_files, 2)
            
            # Check that the file processor was called for each file
            self.assertEqual(self.file_processor_mock.process_file.call_count, 2)
    
    def test_process_local_sources(self):
        """Test processing local sources."""
        # Mock the file processor
        self.file_processor_mock.process_local_folder.return_value = {
            'status': 'success',
            'files': [
                {'file_path': 'file1.csv', 'type': 'csv'},
                {'file_path': 'file2.json', 'type': 'json'}
            ]
        }
        
        # Test with local folder
        result = self.manager._process_local_sources('test-folder', None)
        self.assertEqual(len(result), 2)
        
        # Test with local files
        result = self.manager._process_local_sources(None, ['file1.csv', 'file2.json'])
        self.assertEqual(len(result), 2)
    
    def test_filter_s3_files(self):
        """Test filtering S3 files."""
        # Create test files
        s3_files = [
            {'bucket': 'test-bucket', 'key': 'file1.csv', 'type': 'csv'},
            {'bucket': 'test-bucket', 'key': 'file2.json', 'type': 'json'},
            {'bucket': 'test-bucket', 'key': 'file3.txt', 'type': 'unknown'}
        ]
        
        # Filter files
        filtered_files = self.manager._filter_s3_files(s3_files)
        
        # Check that only CSV and JSON files are included
        self.assertEqual(len(filtered_files), 2)
        self.assertEqual(filtered_files[0]['type'], 'csv')
        self.assertEqual(filtered_files[1]['type'], 'json')
    
    def test_process_s3_source(self):
        """Test processing S3 source."""
        # Mock the file processor
        self.file_processor_mock.process_s3_files.return_value = {
            'status': 'success',
            'files': [
                {'bucket': 'test-bucket', 'key': 'file1.csv', 'type': 'csv'},
                {'bucket': 'test-bucket', 'key': 'file2.json', 'type': 'json'}
            ]
        }
        
        # Process S3 source
        result = self.manager._process_s3_source('test-bucket', 'test-prefix')
        
        # Check that the file processor was called with the correct arguments
        self.file_processor_mock.process_s3_files.assert_called_with('test-bucket', 'test-prefix')
        
        # Check that the result contains the files
        self.assertEqual(len(result), 2)
    
    def test_format_verification_result(self):
        """Test formatting verification result."""
        # Create test data
        verification_result = {
            'status': 'success',
            'message': 'Document count verification successful',
            'actual_count': 42,
            'expected_count': 42
        }
        total_rows = 42
        total_files = 2
        start_time = datetime.now()
        
        # Format result
        result = self.manager._format_verification_result(verification_result, total_rows, total_files, start_time)
        
        # Check that the result contains the expected fields
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['total_rows_processed'], 42)
        self.assertEqual(result['total_files_processed'], 2)
        self.assertEqual(result['expected_documents'], 42)
        self.assertEqual(result['actual_documents'], 42)
        self.assertIn('total_time_seconds', result)
    
    def test_handle_verification_error(self):
        """Test handling verification error."""
        # Create test data
        error = Exception('Test error')
        total_rows = 42
        total_files = 2
        start_time = datetime.now()
        
        # Handle error
        result = self.manager._handle_verification_error(error, total_rows, total_files, start_time)
        
        # Check that the result contains the expected fields
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Error verifying document count: Test error')
        self.assertEqual(result['total_rows_processed'], total_rows)
        self.assertEqual(result['total_files_processed'], total_files)
        self.assertEqual(result['expected_documents'], 0)
        self.assertEqual(result['actual_documents'], 0)
        self.assertIn('total_time_seconds', result)
    
    def test_verify_results_success(self):
        """Test verifying results successfully."""
        # Mock the verification function
        with patch.object(self.manager, '_verify_document_count', return_value={
            'status': 'success',
            'message': 'Document count verification successful',
            'actual_count': 42,
            'expected_count': 42
        }):
            # Create test data
            total_rows = 42
            total_files = 2
            start_time = datetime.now()
            
            # Verify results
            result = self.manager._verify_results('test-index', total_rows, total_files, start_time, False, 0)
            
            # Check that the result contains the expected fields
            self.assertEqual(result['status'], 'success')
            self.assertEqual(result['actual_documents'], 42)
            self.assertEqual(result['total_files_processed'], 2)
            self.assertIn('total_time_seconds', result)
    
    def test_verify_results_error(self):
        """Test verifying results with an error."""
        # Mock the verification function to raise an exception
        with patch.object(self.manager, '_verify_document_count', side_effect=Exception('Test error')):
            # Create test data
            total_rows = 42
            total_files = 2
            start_time = datetime.now()
            
            # Verify results
            result = self.manager._verify_results('test-index', total_rows, total_files, start_time, False, 0)
            
            # Check that the result contains the expected fields
            self.assertEqual(result['status'], 'error')
            self.assertEqual(result['message'], 'Error verifying document count: Test error')
            self.assertEqual(result['total_rows_processed'], total_rows)
            self.assertEqual(result['total_files_processed'], total_files)
            self.assertEqual(result['expected_documents'], 0)
            self.assertEqual(result['actual_documents'], 0)
            self.assertIn('total_time_seconds', result)
    
    def test_ingest_data_no_files(self):
        """Test ingestion when no files are found."""
        # Mock the necessary methods
        self.manager._process_s3_source = MagicMock(return_value=[])
        self.manager._process_local_sources = MagicMock(return_value=[])
        self.manager._process_files = MagicMock()  # Ensure this is a MagicMock object
        
        # Call ingest_data method
        result = self.manager.ingest_data(
            bucket='empty-bucket',
            prefix='empty-prefix/',
            index_name='test-index'
        )
        
        # Verify the result
        self.assertEqual(result['status'], 'warning')
        self.assertEqual(result['message'], 'No files to process')
        self.assertEqual(result['total_rows_processed'], 0)
        self.assertEqual(result['total_files_processed'], 0)
        
        # Verify method calls
        self.manager.index_manager.validate_and_cleanup_index.assert_not_called()
        self.manager._process_files.assert_not_called()
    
    def test_ingest_data_success(self):
        """Test ingesting data successfully."""
        # Mock the necessary functions
        with patch.object(self.manager, '_process_local_sources', return_value=[
            {'file_path': 'file1.csv', 'type': 'csv'},
            {'file_path': 'file2.json', 'type': 'json'}
        ]):
            with patch.object(self.manager, '_process_files', return_value=(42, 2)):
                with patch.object(self.manager, '_verify_results', return_value={
                    'status': 'success',
                    'message': 'Ingestion completed successfully',
                    'total_documents': 42,
                    'total_files': 2,
                    'processing_time': 1.0
                }):
                    # Ingest data
                    result = self.manager.ingest_data(local_folder='test-folder', index_name='test-index')
                    
                    # Check that the result contains the expected fields
                    self.assertEqual(result['status'], 'success')
                    self.assertEqual(result['message'], 'Ingestion completed successfully')
                    self.assertEqual(result['total_documents'], 42)
                    self.assertEqual(result['total_files'], 2)
                    self.assertIn('processing_time', result)
    
    def test_ingest_data_s3_success(self):
        """Test successful ingestion from S3."""
        # Mock the necessary methods
        self.manager._process_s3_source = MagicMock(return_value=[
            {"bucket": "test-bucket", "key": "test-file.csv", "type": "csv"}
        ])
        self.manager._process_files = MagicMock(return_value=(200, 2))
        self.manager._verify_results = MagicMock(return_value={
            'status': 'success',
            'message': 'Successfully ingested data',
            'total_rows_processed': 200,
            'total_files_processed': 2,
            'actual_documents': 200,
            'expected_documents': 200,
            'total_time_seconds': 1.5
        })
        
        # Call ingest_data method
        result = self.manager.ingest_data(
            bucket='test-bucket',
            prefix='test-prefix/',
            index_name='test-index'
        )
        
        # Verify the result
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['total_rows_processed'], 200)
        self.assertEqual(result['total_files_processed'], 2)
        
        # Verify method calls
        self.manager.index_manager.validate_and_cleanup_index.assert_called_once_with('test-index')
        self.manager._process_files.assert_called_once()
        self.manager._verify_results.assert_called_once()
    
    def test_ingest_data_local_files_success(self):
        """Test successful ingestion from local files."""
        # Mock the necessary methods
        self.manager._process_files = MagicMock(return_value=(150, 2))
        self.manager._verify_results = MagicMock(return_value={
            'status': 'success',
            'message': 'Successfully ingested data',
            'total_rows_processed': 150,
            'total_files_processed': 2,
            'actual_documents': 150,
            'expected_documents': 150,
            'total_time_seconds': 1.2
        })
        
        # Call ingest_data method
        result = self.manager.ingest_data(
            local_files=['file1.csv', 'file2.json'],
            index_name='test-index'
        )
        
        # Verify the result
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['total_rows_processed'], 150)
        self.assertEqual(result['total_files_processed'], 2)
        
        # Verify method calls
        self.manager.index_manager.validate_and_cleanup_index.assert_called_once_with('test-index')
        self.manager._process_files.assert_called_once()
        self.manager._verify_results.assert_called_once()
    
    def test_ingest_data_cleanup_error(self):
        """Test ingestion when index cleanup fails."""
        # Mock the necessary methods
        self.manager._process_s3_source = MagicMock(return_value=[
            {"bucket": "test-bucket", "key": "test-file.csv", "type": "csv"}
        ])
        self.manager.index_manager.validate_and_cleanup_index = MagicMock(return_value={
            'status': 'error',
            'message': 'Failed to clean up index'
        })
        self.manager._process_files = MagicMock()  # Ensure this is a MagicMock object
        
        # Call ingest_data method
        result = self.manager.ingest_data(
            bucket='test-bucket',
            prefix='test-prefix/',
            index_name='test-index'
        )
        
        # Verify the result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Failed to clean up index')
        
        # Verify method calls
        self.manager.index_manager.validate_and_cleanup_index.assert_called_once_with('test-index')
        self.manager._process_files.assert_not_called()
    
    def test_ingest_data_verification_error(self):
        """Test ingestion when verification fails."""
        # Mock the necessary methods
        self.manager._process_s3_source = MagicMock(return_value=[
            {"bucket": "test-bucket", "key": "test-file.csv", "type": "csv"}
        ])
        self.manager._process_files = MagicMock(return_value=(200, 2))
        self.manager._verify_results = MagicMock(return_value={
            'status': 'error',
            'message': 'Document count verification failed',
            'expected_documents': 200,
            'actual_documents': 150
        })
        
        # Call ingest_data method
        result = self.manager.ingest_data(
            bucket='test-bucket',
            prefix='test-prefix/',
            index_name='test-index'
        )
        
        # Verify the result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Document count verification failed')
        self.assertEqual(result['expected_documents'], 200)
        self.assertEqual(result['actual_documents'], 150)
        
        # Verify method calls
        self.manager.index_manager.validate_and_cleanup_index.assert_called_once_with('test-index')
        self.manager._process_files.assert_called_once()
        self.manager._verify_results.assert_called_once()

class TestBulkUpdateMain(unittest.TestCase):
    """Test cases for the main function in bulkupdate.py."""
    
    @patch('argparse.ArgumentParser.parse_args')
    @patch('bulkupdate.OpenSearchBulkIngestion')
    def test_main_s3_success(self, mock_ingestion_class, mock_parse_args):
        """Test the main function with successful S3 ingestion."""
        # Set up mock arguments
        mock_args = MagicMock()
        mock_args.bucket = 'test-bucket'
        mock_args.prefix = 'test-prefix/'
        mock_args.local_files = None
        mock_args.local_folder = None
        mock_args.index = 'test-index'
        mock_args.batch_size = 1000
        mock_args.max_workers = 2
        mock_args.resume = False
        mock_args.fresh_load = True
        mock_parse_args.return_value = mock_args
        
        # Set up mock ingestion service
        mock_ingestion_service = MagicMock()
        mock_ingestion_class.return_value = mock_ingestion_service
        
        # Set up mock result
        mock_result = {
            'status': 'success',
            'message': 'Successfully ingested data',
            'total_rows_processed': 200,
            'total_files_processed': 2,
            'actual_documents': 200,
            'expected_documents': 200,
            'total_time_seconds': 1.5
        }
        mock_ingestion_service.ingest_data.return_value = mock_result
        
        # Call main function
        result = main()
        
        # Verify result
        self.assertEqual(result, 0)
        
        # Verify ingestion service was initialized
        mock_ingestion_class.assert_called_once_with(
            batch_size=1000,
            max_workers=2
        )
        
        # Verify ingest_data was called with correct arguments
        mock_ingestion_service.ingest_data.assert_called_once_with(
            bucket='test-bucket',
            prefix='test-prefix/',
            local_files=None,
            local_folder=None,
            index_name='test-index',
            resume=False,
            fresh_load=True
        )
    
    @patch('argparse.ArgumentParser.parse_args')
    @patch('bulkupdate.OpenSearchBulkIngestion')
    def test_main_local_files_success(self, mock_ingestion_class, mock_parse_args):
        """Test the main function with successful local files ingestion."""
        # Set up mock arguments
        mock_args = MagicMock()
        mock_args.bucket = None
        mock_args.prefix = None
        mock_args.local_files = ['file1.csv', 'file2.json']
        mock_args.local_folder = None
        mock_args.index = 'test-index'
        mock_args.batch_size = 1000
        mock_args.max_workers = 2
        mock_args.resume = False
        mock_args.fresh_load = True
        mock_parse_args.return_value = mock_args
        
        # Set up mock ingestion service
        mock_ingestion_service = MagicMock()
        mock_ingestion_class.return_value = mock_ingestion_service
        
        # Set up mock result
        mock_result = {
            'status': 'success',
            'message': 'Successfully ingested data',
            'total_rows_processed': 150,
            'total_files_processed': 2,
            'actual_documents': 150,
            'expected_documents': 150,
            'total_time_seconds': 1.2
        }
        mock_ingestion_service.ingest_data.return_value = mock_result
        
        # Call main function
        result = main()
        
        # Verify result
        self.assertEqual(result, 0)
        
        # Verify ingestion service was initialized
        mock_ingestion_class.assert_called_once_with(
            batch_size=1000,
            max_workers=2
        )
        
        # Verify ingest_data was called with correct arguments
        mock_ingestion_service.ingest_data.assert_called_once_with(
            bucket=None,
            prefix=None,
            local_files=['file1.csv', 'file2.json'],
            local_folder=None,
            index_name='test-index',
            resume=False,
            fresh_load=True
        )
    
    @patch('argparse.ArgumentParser.parse_args')
    @patch('bulkupdate.OpenSearchBulkIngestion')
    def test_main_error(self, mock_ingestion_class, mock_parse_args):
        """Test the main function with error in ingestion."""
        # Set up mock arguments
        mock_args = MagicMock()
        mock_args.bucket = 'test-bucket'
        mock_args.prefix = 'test-prefix/'
        mock_args.local_files = None
        mock_args.local_folder = None
        mock_args.index = 'test-index'
        mock_args.batch_size = 1000
        mock_args.max_workers = 2
        mock_args.resume = False
        mock_args.fresh_load = True
        mock_parse_args.return_value = mock_args
        
        # Set up mock ingestion service
        mock_ingestion_service = MagicMock()
        mock_ingestion_class.return_value = mock_ingestion_service
        
        # Set up mock result with error
        mock_result = {
            'status': 'error',
            'message': 'Failed to ingest data',
            'expected_documents': 200,
            'actual_documents': 150
        }
        mock_ingestion_service.ingest_data.return_value = mock_result
        
        # Call main function
        result = main()
        
        # Verify result
        self.assertEqual(result, 0)  # Main returns 0 even for error status
        
        # Verify ingestion service was initialized
        mock_ingestion_class.assert_called_once_with(
            batch_size=1000,
            max_workers=2
        )
        
        # Verify ingest_data was called with correct arguments
        mock_ingestion_service.ingest_data.assert_called_once_with(
            bucket='test-bucket',
            prefix='test-prefix/',
            local_files=None,
            local_folder=None,
            index_name='test-index',
            resume=False,
            fresh_load=True
        )
    
    @patch('argparse.ArgumentParser.parse_args')
    def test_main_exception(self, mock_parse_args):
        """Test the main function with exception."""
        # Set up mock arguments
        mock_args = MagicMock()
        mock_args.bucket = 'test-bucket'
        mock_args.prefix = 'test-prefix/'
        mock_args.local_files = None
        mock_args.local_folder = None
        mock_args.index = 'test-index'
        mock_args.batch_size = 1000
        mock_args.max_workers = 2
        mock_args.resume = False
        mock_args.fresh_load = True
        mock_parse_args.return_value = mock_args
        
        # Set up mock to raise exception
        with patch('bulkupdate.OpenSearchBulkIngestion', side_effect=ValueError("Configuration error")):
            # Call main function
            result = main()
            
            # Verify result
            self.assertEqual(result, 1)  # Main returns 1 for exceptions
    
    @patch('argparse.ArgumentParser.parse_args')
    def test_main_no_data_source(self, mock_parse_args):
        """Test the main function with no data source provided."""
        # Set up mock arguments with no data source
        mock_args = MagicMock()
        mock_args.bucket = None
        mock_args.prefix = None
        mock_args.local_files = None
        mock_args.local_folder = None
        mock_args.index = 'test-index'
        mock_args.batch_size = 1000
        mock_args.max_workers = 2
        mock_args.resume = False
        mock_args.fresh_load = True
        mock_parse_args.return_value = mock_args
        
        # Mock the parser.error method to avoid actual exit
        with patch('argparse.ArgumentParser.error', side_effect=SystemExit(2)):
            # Call main function and expect SystemExit
            with self.assertRaises(SystemExit) as cm:
                main()
            
            # Verify exit code
            self.assertEqual(cm.exception.code, 2)
    
    @patch('argparse.ArgumentParser.parse_args')
    def test_main_resume_and_fresh_load(self, mock_parse_args):
        """Test the main function with both resume and fresh-load options."""
        # Set up mock arguments with both resume and fresh-load
        mock_args = MagicMock()
        mock_args.bucket = 'test-bucket'
        mock_args.prefix = 'test-prefix/'
        mock_args.local_files = None
        mock_args.local_folder = None
        mock_args.index = 'test-index'
        mock_args.batch_size = 1000
        mock_args.max_workers = 2
        mock_args.resume = True
        mock_args.fresh_load = True
        mock_parse_args.return_value = mock_args
        
        # Mock the parser.error method to avoid actual exit
        with patch('argparse.ArgumentParser.error', side_effect=SystemExit(2)):
            # Call main function and expect SystemExit
            with self.assertRaises(SystemExit) as cm:
                main()
            
            # Verify exit code
            self.assertEqual(cm.exception.code, 2)

if __name__ == '__main__':
    unittest.main() 