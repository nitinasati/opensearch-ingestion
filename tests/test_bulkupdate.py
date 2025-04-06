"""
Unit tests for the OpenSearchBulkIngestion class.

This module contains tests for the bulk ingestion functionality, including
file processing, document verification, and ingestion operations.
"""

import unittest
from unittest.mock import patch, MagicMock, call
import json
import os
import pandas as pd
from io import StringIO
from datetime import datetime
from bulkupdate import OpenSearchBulkIngestion, NO_FILES_MESSAGE, TRACKING_FILE

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
        self.manager = OpenSearchBulkIngestion()
    
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
        self.assertEqual(self.manager.batch_size, 10000)
        self.assertEqual(self.manager.max_workers, 4)
    
    def test_verify_document_count_success(self):
        """Test successful document count verification."""
        # Mock a successful count response
        count_response = MagicMock()
        count_response.status_code = 200
        count_response.json.return_value = {'count': 42}
        self.requests_mock.post.return_value = count_response
        
        result = self.manager._verify_document_count('test-index', 42)
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['message'], 'Document count verification successful')
        self.assertEqual(result['actual_count'], 42)
        self.assertEqual(result['expected_count'], 42)
    
    def test_verify_document_count_mismatch(self):
        """Test document count verification with a mismatch."""
        # Mock a successful count response with a different count
        count_response = MagicMock()
        count_response.status_code = 200
        count_response.json.return_value = {'count': 40}
        self.requests_mock.post.return_value = count_response
        
        result = self.manager._verify_document_count('test-index', 42)
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Document count mismatch')
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
            mock_open.return_value.__enter__.return_value.read.return_value = json.dumps({
                'test-index': ['file1.csv']
            })
            
            self.manager._update_processed_files('test-index', 'file2.csv')
            
            # Check that the file was written with the updated list
            mock_open.assert_called_with(TRACKING_FILE, 'w')
            written_data = mock_open.return_value.__enter__.return_value.write.call_args[0][0]
            data = json.loads(written_data)
            self.assertEqual(data['test-index'], ['file1.csv', 'file2.csv'])
    
    def test_clear_processed_files(self):
        """Test clearing processed files."""
        # Mock file reading and writing
        with patch('builtins.open', new_callable=MagicMock) as mock_open:
            mock_open.return_value.__enter__.return_value.read.return_value = json.dumps({
                'test-index': ['file1.csv', 'file2.csv'],
                'other-index': ['file3.csv']
            })
            
            self.manager._clear_processed_files('test-index')
            
            # Check that the file was written with the updated data
            mock_open.assert_called_with(TRACKING_FILE, 'w')
            written_data = mock_open.return_value.__enter__.return_value.write.call_args[0][0]
            data = json.loads(written_data)
            self.assertEqual(data['test-index'], [])
            self.assertEqual(data['other-index'], ['file3.csv'])
    
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
        self.assertEqual(result['total_documents'], 42)
        self.assertEqual(result['total_files'], 2)
        self.assertIn('processing_time', result)
    
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
        self.assertEqual(result['message'], 'Test error')
        self.assertEqual(result['total_documents'], 42)
        self.assertEqual(result['total_files'], 2)
        self.assertIn('processing_time', result)
    
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
            self.assertEqual(result['total_documents'], 42)
            self.assertEqual(result['total_files'], 2)
            self.assertIn('processing_time', result)
    
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
            self.assertEqual(result['message'], 'Test error')
            self.assertEqual(result['total_documents'], 42)
            self.assertEqual(result['total_files'], 2)
            self.assertIn('processing_time', result)
    
    def test_ingest_data_no_files(self):
        """Test ingesting data with no files."""
        # Test with no files
        result = self.manager.ingest_data(index_name='test-index')
        
        # Check that the result contains the expected fields
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], NO_FILES_MESSAGE)
    
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
    
    def test_arguments_printing(self):
        """Test printing arguments."""
        # Create test arguments
        args = MagicMock()
        args.bucket = 'test-bucket'
        args.prefix = 'test-prefix'
        args.local_files = ['file1.csv', 'file2.json']
        args.local_folder = 'test-folder'
        args.index = 'test-index'
        args.resume = True
        args.fresh_load = False
        
        # Import the function
        from bulkupdate import _arguments_printing
        
        # Print arguments
        _arguments_printing(args)
        
        # No assertions needed, just checking that it doesn't raise an exception

if __name__ == '__main__':
    unittest.main() 