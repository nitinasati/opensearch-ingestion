"""
Unit tests for the OpenSearchBulkIngestion class and main function.

This module contains tests for bulk ingestion operations, including
the main function that handles command line arguments and orchestrates
the data ingestion process.
"""

import unittest
from unittest.mock import patch, MagicMock, call, PropertyMock
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
        # Create mock for OpenSearch connection
        self.opensearch_mock = MagicMock()
        self.opensearch_mock.info.return_value = {'version': {'number': '7.10.2'}}
        self.opensearch_mock.indices.exists.return_value = True
        self.opensearch_mock.indices.get.return_value = {'test-index': {'mappings': {}}}
        self.opensearch_mock.indices.stats.return_value = {'indices': {'test-index': {'total': {'docs': {'count': 0}}}}}
        self.opensearch_mock.indices.delete.return_value = {'acknowledged': True}
        self.opensearch_mock.indices.create.return_value = {'acknowledged': True}
        self.opensearch_mock.indices.put_mapping.return_value = {'acknowledged': True}
        self.opensearch_mock.indices.put_settings.return_value = {'acknowledged': True}
        self.opensearch_mock.bulk.return_value = {'items': [{'index': {'status': 201}}]}
        
        # Create mock for requests
        self.requests_mock = MagicMock()
        self.requests_mock.get.return_value = MagicMock(
            status_code=200,
            json=lambda: {'version': {'number': '7.10.2'}}
        )
        self.requests_mock.get.return_value.raise_for_status = MagicMock()
        
        # Create mock for OpenSearchBaseManager
        self.manager_mock = MagicMock()
        self.manager_mock.opensearch = self.opensearch_mock
        self.manager_mock.opensearch_endpoint = 'https://dummy-opensearch-endpoint'
        
        # Create mock for FileProcessor
        self.file_processor_mock = MagicMock()
        self.file_processor_mock.process_local_folder.return_value = {
            "status": "success",
            "files": [{"file_path": "test.csv", "type": "csv"}]
        }
        self.file_processor_mock.process_s3_files.return_value = {
            "status": "success",
            "files": [{"bucket": "test-bucket", "key": "test.csv", "type": "csv"}]
        }
        self.file_processor_mock.process_file.return_value = (100, 100)
        
        # Apply patches
        self.opensearch_patcher = patch('opensearchpy.OpenSearch', return_value=self.opensearch_mock)
        self.requests_patcher = patch('requests.get', return_value=self.requests_mock.get.return_value)
        self.manager_patcher = patch('opensearch_base_manager.OpenSearchBaseManager', return_value=self.manager_mock)
        self.file_processor_patcher = patch('file_processor.FileProcessor', return_value=self.file_processor_mock)
        
        self.opensearch_patcher.start()
        self.requests_patcher.start()
        self.manager_patcher.start()
        self.file_processor_patcher.start()
        
        # Initialize the bulk ingestion manager
        self.ingestion_manager = OpenSearchBulkIngestion(batch_size=1000, max_workers=2)
        self.ingestion_manager.opensearch_manager = self.manager_mock
        self.ingestion_manager.file_processor = self.file_processor_mock
        self.ingestion_manager.index_manager = MagicMock()
        self.ingestion_manager.index_manager.validate_and_cleanup_index.return_value = {'status': 'success'}
        self.ingestion_manager.index_manager._verify_index_exists.return_value = True
    
    def tearDown(self):
        """Clean up after tests."""
        self.opensearch_patcher.stop()
        self.requests_patcher.stop()
        self.manager_patcher.stop()
        self.file_processor_patcher.stop()
    
    def test_init(self):
        """Test initialization of the OpenSearchBulkIngestion class."""
        self.assertIsNotNone(self.ingestion_manager)
        self.assertEqual(self.ingestion_manager.batch_size, 1000)
        self.assertEqual(self.ingestion_manager.max_workers, 2)
    
    def test_verify_document_count_success(self):
        """Test successful document count verification."""
        # Mock the _processed_count_from_bulk attribute
        self.ingestion_manager.file_processor._processed_count_from_bulk = 42
        
        result = self.ingestion_manager._verify_document_count(42, 42, False)
        
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['message'], 'Document count verification successful: 42 new documents match expected count')
        self.assertEqual(result['actual_count'], 42)
        self.assertEqual(result['expected_count'], 42)
    
    def test_verify_document_count_mismatch(self):
        """Test document count verification with mismatch."""
        # Mock the processed count from bulk
        with patch.object(self.ingestion_manager.file_processor, '_processed_count_from_bulk', 40):
            result = self.ingestion_manager._verify_document_count(42, 40, False)
            
            self.assertEqual(result["status"], "error")
            self.assertEqual(result["message"], "Document count mismatch: Expected 42 new documents, got 40")
            self.assertEqual(result["expected_count"], 42)
            self.assertEqual(result["actual_count"], 40)
            self.assertEqual(result["documents_indexed"], 40)
    
    def test_verify_document_count_resume_success(self):
        """Test successful document count verification in resume mode."""
        # Set the expected new documents count on the mock file_processor
        self.ingestion_manager.file_processor._processed_count_from_bulk = 50
        
        result = self.ingestion_manager._verify_document_count(50, 50, True)
        
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['message'], 'Document count verification successful: 50 new documents match expected count')
        self.assertEqual(result['expected_count'], 50)
        self.assertEqual(result['actual_count'], 50)
        self.assertEqual(result['documents_indexed'], 50)

    def test_verify_document_count_resume_mismatch(self):
        """Test document count verification in resume mode with mismatch."""
        # Mock the processed count from bulk
        with patch.object(self.ingestion_manager.file_processor, '_processed_count_from_bulk', 45):
            result = self.ingestion_manager._verify_document_count(50, 45, True)
            
            self.assertEqual(result["status"], "error")
            self.assertEqual(result["message"], "Document count mismatch: Expected 50 new documents, got 45")
            self.assertEqual(result["expected_count"], 50)
            self.assertEqual(result["actual_count"], 45)
            self.assertEqual(result["documents_indexed"], 45)

    def test_get_processed_files(self):
        """Test getting processed files."""
        # Mock file reading
        with patch('builtins.open', new_callable=MagicMock) as mock_open:
            mock_open.return_value.__enter__.return_value.read.return_value = json.dumps({
                'test-index': ['file1.csv', 'file2.csv']
            })
            
            files = self.ingestion_manager._get_processed_files('test-index')
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
            self.ingestion_manager._update_processed_files('test-index', 'file2.csv')
            
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
            self.ingestion_manager._clear_processed_files('test-index')
            
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
        identifier = self.ingestion_manager._get_file_identifier(s3_file)
        self.assertEqual(identifier, 'test-bucket/test/path/file.csv')
        
        # Test with local file
        local_file = {
            'file_path': '/path/to/file.csv'
        }
        identifier = self.ingestion_manager._get_file_identifier(local_file)
        self.assertEqual(identifier, '/path/to/file.csv')
    
    def test_determine_file_type(self):
        """Test determining file type."""
        # Test CSV file
        file_type = self.ingestion_manager._determine_file_type('test.csv')
        self.assertEqual(file_type, 'csv')
        
        # Test JSON file
        file_type = self.ingestion_manager._determine_file_type('test.json')
        self.assertEqual(file_type, 'json')
        
        # Test unknown file type
        file_type = self.ingestion_manager._determine_file_type('test.txt')
        self.assertEqual(file_type, 'unknown')
    
    def test_process_files(self):
        """Test processing multiple files."""
        # Mock file processor
        with patch.object(self.ingestion_manager.file_processor, 'process_file', return_value=(10, 10)):
            # Mock index manager
            with patch.object(self.ingestion_manager.index_manager, '_verify_index_exists', return_value=True):
                with patch.object(self.ingestion_manager.index_manager, 'validate_and_cleanup_index', return_value={"status": "success"}):
                    # Test with multiple files
                    files = [
                        {"file_path": "test1.csv", "type": "csv"},
                        {"file_path": "test2.json", "type": "json"}
                    ]
                    
                    total_rows, total_files, total_processed = self.ingestion_manager._process_files(files, "test-index")
                    
                    self.assertEqual(total_rows, 20)
                    self.assertEqual(total_files, 2)
                    self.assertEqual(total_processed, 20)
    
    def test_process_local_sources(self):
        """Test processing local sources."""
        # Mock the file processor
        self.ingestion_manager.file_processor.process_local_folder.return_value = {
            'status': 'success',
            'files': [
                {'file_path': 'file1.csv', 'type': 'csv'},
                {'file_path': 'file2.json', 'type': 'json'}
            ]
        }
        
        # Test with local folder
        result = self.ingestion_manager._process_local_sources('test-folder', None)
        self.assertEqual(len(result), 2)
        
        # Test with local files
        result = self.ingestion_manager._process_local_sources(None, ['file1.csv', 'file2.json'])
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
        filtered_files = self.ingestion_manager._filter_s3_files(s3_files)
        
        # Check that only CSV and JSON files are included
        self.assertEqual(len(filtered_files), 2)
        self.assertEqual(filtered_files[0]['type'], 'csv')
        self.assertEqual(filtered_files[1]['type'], 'json')
    
    def test_process_s3_source(self):
        """Test processing S3 source."""
        # Mock the file processor
        self.ingestion_manager.file_processor.process_s3_files.return_value = {
            'status': 'success',
            'files': [
                {'bucket': 'test-bucket', 'key': 'file1.csv', 'type': 'csv'},
                {'bucket': 'test-bucket', 'key': 'file2.json', 'type': 'json'}
            ]
        }
        
        # Process S3 source
        result = self.ingestion_manager._process_s3_source('test-bucket', 'test-prefix')
        
        # Check that the file processor was called with the correct arguments
        self.ingestion_manager.file_processor.process_s3_files.assert_called_with('test-bucket', 'test-prefix')
        
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
        result = self.ingestion_manager._format_verification_result(verification_result, total_rows, total_files, start_time)
        
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
        result = self.ingestion_manager._handle_verification_error(error, total_rows, total_files, start_time)
        
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
        with patch.object(self.ingestion_manager, '_verify_document_count', return_value={
            'status': 'success',
            'message': 'Document count verification successful',
            'actual_count': 42,
            'expected_count': 42
        }):
            # Create test data
            total_rows = 42
            total_files = 2
            total_processed_count_from_bulk = 42
            start_time = datetime.now()

            # Verify results
            result = self.ingestion_manager._verify_results(total_rows, total_files, total_processed_count_from_bulk, start_time, False)
            
            # Check the result
            self.assertEqual(result['status'], 'success')
            self.assertEqual(result['total_rows_processed'], total_rows)
            self.assertEqual(result['total_files_processed'], total_files)
            self.assertEqual(result['expected_documents'], 42)
            self.assertEqual(result['actual_documents'], 42)
    
    def test_verify_results_error(self):
        """Test verifying results with an error."""
        # Mock the verification function to raise an exception
        with patch.object(self.ingestion_manager, '_verify_document_count', side_effect=Exception('Test error')):
            # Create test data
            total_rows = 42
            total_files = 2
            total_processed_count_from_bulk = 42
            start_time = datetime.now()
            
            # Verify results
            result = self.ingestion_manager._verify_results(total_rows, total_files, total_processed_count_from_bulk, start_time, False)
            
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
        self.ingestion_manager._process_s3_source = MagicMock(return_value=[])
        self.ingestion_manager._process_local_sources = MagicMock(return_value=[])
        self.ingestion_manager._process_files = MagicMock()
        
        # Call ingest_data method
        result = self.ingestion_manager.ingest_data(
            bucket='empty-bucket',
            prefix='empty-prefix/',
            index_name='test-index'
        )
        
        # Verify the result
        self.assertEqual(result['status'], 'warning')
        self.assertEqual(result['message'], NO_FILES_MESSAGE)
        self.assertEqual(result['total_rows_processed'], 0)
        self.assertEqual(result['total_files_processed'], 0)
        
        # Verify method calls
        self.ingestion_manager.index_manager.validate_and_cleanup_index.assert_not_called()
        self.ingestion_manager._process_files.assert_not_called()
    
    def test_ingest_data_success(self):
        """Test ingesting data successfully."""
        # Mock the necessary functions
        with patch.object(self.ingestion_manager, '_process_local_sources', return_value=[
            {'file_path': 'file1.csv', 'type': 'csv'},
            {'file_path': 'file2.json', 'type': 'json'}
        ]):
            with patch.object(self.ingestion_manager, '_process_files', return_value=(42, 2, 42)):
                with patch.object(self.ingestion_manager, '_verify_results', return_value={
                    'status': 'success',
                    'message': 'Successfully ingested data',
                    'total_rows_processed': 42,
                    'total_files_processed': 2,
                    'actual_documents': 42,
                    'expected_documents': 42,
                    'total_time_seconds': 1.0
                }):
                    # Ingest data
                    result = self.ingestion_manager.ingest_data(local_folder='test-folder', index_name='test-index')
                    
                    # Check that the result contains the expected fields
                    self.assertEqual(result['status'], 'success')
                    self.assertEqual(result['message'], 'Successfully ingested data')
                    self.assertEqual(result['total_rows_processed'], 42)
                    self.assertEqual(result['total_files_processed'], 2)
                    self.assertIn('total_time_seconds', result)
    
    def test_ingest_data_s3_success(self):
        """Test successful ingestion from S3."""
        # Mock the necessary methods
        self.ingestion_manager._process_s3_source = MagicMock(return_value=[
            {"bucket": "test-bucket", "key": "test-file.csv", "type": "csv"}
        ])
        self.ingestion_manager._process_files = MagicMock(return_value=(200, 2, 200))
        self.ingestion_manager._verify_results = MagicMock(return_value={
            'status': 'success',
            'message': 'Successfully ingested data',
            'total_rows_processed': 200,
            'total_files_processed': 2,
            'actual_documents': 200,
            'expected_documents': 200,
            'total_time_seconds': 1.5
        })
        
        # Call ingest_data method
        result = self.ingestion_manager.ingest_data(
            bucket='test-bucket',
            prefix='test-prefix/',
            index_name='test-index'
        )
        
        # Verify the result
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['total_rows_processed'], 200)
        self.assertEqual(result['total_files_processed'], 2)
        
        # Verify method calls
        self.ingestion_manager.index_manager.validate_and_cleanup_index.assert_called_once_with('test-index')
        self.ingestion_manager._process_files.assert_called_once()
        self.ingestion_manager._verify_results.assert_called_once()
    
    def test_ingest_data_local_files_success(self):
        """Test successful ingestion from local files."""
        # Mock the necessary methods
        self.ingestion_manager._process_files = MagicMock(return_value=(150, 2, 150))
        self.ingestion_manager._verify_results = MagicMock(return_value={
            'status': 'success',
            'message': 'Successfully ingested data',
            'total_rows_processed': 150,
            'total_files_processed': 2,
            'actual_documents': 150,
            'expected_documents': 150,
            'total_time_seconds': 1.2
        })
        
        # Call ingest_data method
        result = self.ingestion_manager.ingest_data(
            local_files=['file1.csv', 'file2.json'],
            index_name='test-index'
        )
        
        # Verify the result
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['total_rows_processed'], 150)
        self.assertEqual(result['total_files_processed'], 2)
        
        # Verify method calls
        self.ingestion_manager.index_manager.validate_and_cleanup_index.assert_called_once_with('test-index')
        self.ingestion_manager._process_files.assert_called_once()
        self.ingestion_manager._verify_results.assert_called_once()
    
    def test_ingest_data_cleanup_error(self):
        """Test ingestion when index cleanup fails."""
        # Mock the necessary methods
        self.ingestion_manager._process_s3_source = MagicMock(return_value=[
            {"bucket": "test-bucket", "key": "test-file.csv", "type": "csv"}
        ])
        self.ingestion_manager.index_manager.validate_and_cleanup_index = MagicMock(return_value={
            'status': 'error',
            'message': 'Failed to clean up index'
        })
        self.ingestion_manager._process_files = MagicMock()
        
        # Call ingest_data method
        result = self.ingestion_manager.ingest_data(
            bucket='test-bucket',
            prefix='test-prefix/',
            index_name='test-index'
        )
        
        # Verify the result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Failed to clean up index')
        
        # Verify method calls
        self.ingestion_manager.index_manager.validate_and_cleanup_index.assert_called_once_with('test-index')
        self.ingestion_manager._process_files.assert_not_called()
    
    def test_ingest_data_verification_error(self):
        """Test ingestion when verification fails."""
        # Mock the necessary methods
        self.ingestion_manager._process_s3_source = MagicMock(return_value=[
            {"bucket": "test-bucket", "key": "test-file.csv", "type": "csv"}
        ])
        self.ingestion_manager._process_files = MagicMock(return_value=(200, 2, 150))
        self.ingestion_manager._verify_results = MagicMock(return_value={
            'status': 'error',
            'message': 'Document count verification failed',
            'total_rows_processed': 200,
            'total_files_processed': 2,
            'expected_documents': 200,
            'actual_documents': 150,
            'total_time_seconds': 1.5
        })
        
        # Call ingest_data method
        result = self.ingestion_manager.ingest_data(
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
        self.ingestion_manager.index_manager.validate_and_cleanup_index.assert_called_once_with('test-index')
        self.ingestion_manager._process_files.assert_called_once()
        self.ingestion_manager._verify_results.assert_called_once()

    def test_clear_processed_files_all(self):
        """Test clearing all processed files tracking data."""
        # Mock the file operations
        mock_open = MagicMock()
        mock_file = MagicMock()
        mock_open.return_value.__enter__.return_value = mock_file
        
        # Mock json.dump to verify it's called with an empty dict
        with patch('builtins.open', mock_open), \
             patch('json.dump') as mock_json_dump, \
             patch('bulkupdate.logger') as mock_logger:
            
            # Call the method with index_name=None to clear all tracking data
            self.ingestion_manager._clear_processed_files(None)
            
            # Verify file operations
            mock_open.assert_called_once_with(TRACKING_FILE, 'w')
            
            # Verify json.dump was called with an empty dict
            mock_json_dump.assert_called_once_with({}, mock_file, indent=2)
            
            # Verify logger was called with the correct message
            mock_logger.info.assert_called_once_with("Cleared all processed files tracking data")

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