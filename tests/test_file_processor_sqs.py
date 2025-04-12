"""
Unit tests for the SQS error reporting functionality in the FileProcessor class.

This module contains tests for:
- SQS error reporting
- Message splitting for large error payloads
- Error handling in SQS operations
"""

import unittest
from unittest.mock import patch, MagicMock, call
import json
import os
import sys
from datetime import datetime

# Add the parent directory to the path so we can import the file_processor module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from file_processor import FileProcessor

class TestFileProcessorSQS(unittest.TestCase):
    """Test cases for the SQS error reporting functionality in FileProcessor."""
    
    def setUp(self):
        """Set up test environment."""
        # Set environment variables
        os.environ['OPENSEARCH_ENDPOINT'] = 'http://localhost:9200'
        os.environ['DLQ'] = 'enabled'
        os.environ['SQS-DLQ-ARN'] = 'arn:aws:sqs:region:account-id:queue-name'
        
        # Create mock for SQS client
        self.sqs_mock = MagicMock()
        self.sqs_mock.get_queue_url.return_value = {'QueueUrl': 'https://sqs.queue.url'}
        self.sqs_mock.send_message.return_value = {'MessageId': 'test-message-id'}
        
        # Create mock for boto3 client
        self.boto3_mock = MagicMock()
        self.boto3_mock.client.return_value = self.sqs_mock
        
        # Create mock for OpenSearch connection
        self.opensearch_mock = MagicMock()
        self.opensearch_mock.info.return_value = {'version': {'number': '7.10.2'}}
        self.opensearch_mock.indices.exists.return_value = True
        self.opensearch_mock.indices.get.return_value = {'test-index': {'mappings': {}}}
        self.opensearch_mock.indices.stats.return_value = {'indices': {'test-index': {'total': {'docs': {'count': 100}}}}}
        self.opensearch_mock.count.return_value = {'count': 100}
        
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
        self.manager_mock.opensearch_endpoint = 'http://localhost:9200'
        self.manager_mock._make_request.return_value = {
            'status': 'success',
            'response': MagicMock(
                status_code=200,
                json=lambda: {'version': {'number': '7.10.2'}}
            )
        }
        
        # Apply patches
        self.boto3_patcher = patch('boto3.client', return_value=self.sqs_mock)
        self.opensearch_patcher = patch('opensearchpy.OpenSearch', return_value=self.opensearch_mock)
        self.requests_patcher = patch('requests.get', return_value=self.requests_mock.get.return_value)
        self.manager_patcher = patch('opensearch_base_manager.OpenSearchBaseManager', return_value=self.manager_mock)
        
        self.boto3_patcher.start()
        self.opensearch_patcher.start()
        self.requests_patcher.start()
        self.manager_patcher.start()
        
        # Initialize file processor
        self.file_processor = FileProcessor()
        self.file_processor._make_request = self.manager_mock._make_request
    
    def tearDown(self):
        """Clean up after tests."""
        self.boto3_patcher.stop()
        self.opensearch_patcher.stop()
        self.requests_patcher.stop()
        self.manager_patcher.stop()
        
        # Clean up environment variables
        if 'DLQ' in os.environ:
            del os.environ['DLQ']
        if 'SQS-DLQ-ARN' in os.environ:
            del os.environ['SQS-DLQ-ARN']
    
    def test_init_with_sqs_arn(self):
        """Test initialization with SQS ARN."""
        self.assertIsNotNone(self.file_processor.sqs_client)
        self.assertEqual(self.file_processor.sqs_dlq_arn, 'arn:aws:sqs:region:account-id:queue-name')
        self.assertTrue(self.file_processor.dlq_enabled)
    
    def test_init_without_sqs_arn(self):
        """Test initialization without SQS ARN."""
        # Create mock credentials
        mock_credentials = MagicMock()
        mock_credentials.access_key = 'test-access-key'
        mock_credentials.secret_key = 'test-secret-key'
        mock_credentials.token = 'test-token'
        
        # Create mock session
        mock_session = MagicMock()
        mock_session.get_credentials.return_value = mock_credentials
        
        with patch.dict('os.environ', {
            'OPENSEARCH_ENDPOINT': 'http://localhost:9200',
            'DLQ': 'enabled',
            'AWS_REGION': 'us-east-1',
            'VERIFY_SSL': 'false'
        }, clear=True), \
             patch('boto3.Session', return_value=mock_session):
            processor = FileProcessor()
            self.assertIsNone(processor.sqs_dlq_arn)
            self.assertFalse(processor.dlq_enabled)
    
    def test_init_with_dlq_disabled(self):
        """Test initialization with DLQ disabled."""
        # Create mock credentials
        mock_credentials = MagicMock()
        mock_credentials.access_key = 'test-access-key'
        mock_credentials.secret_key = 'test-secret-key'
        mock_credentials.token = 'test-token'
        
        # Create mock session
        mock_session = MagicMock()
        mock_session.get_credentials.return_value = mock_credentials
        
        with patch.dict('os.environ', {
            'OPENSEARCH_ENDPOINT': 'http://localhost:9200',
            'SQS-DLQ-ARN': 'arn:aws:sqs:region:account-id:queue-name',
            'DLQ': 'disabled',
            'AWS_REGION': 'us-east-1',
            'VERIFY_SSL': 'false'
        }, clear=True), \
             patch('boto3.Session', return_value=mock_session):
            processor = FileProcessor()
            self.assertIsNone(processor.sqs_client)
            self.assertFalse(processor.dlq_enabled)
    
    def test_send_error_to_sqs_success(self):
        """Test successful error message sending to SQS."""
        error_payload = {
            'error_message': 'Test error',
            'failed_records': [],
            'file_key': 'test.csv',
            'source': 'test'
        }
        result = self.file_processor._send_error_to_sqs(error_payload)
        self.assertTrue(result)
        self.sqs_mock.send_message.assert_called_once()
    
    def test_send_error_to_sqs_without_arn(self):
        """Test sending error message to SQS when ARN is not configured."""
        self.file_processor.sqs_dlq_arn = None
        error_payload = {
            'error_message': 'Test error',
            'failed_records': []
        }
        result = self.file_processor._send_error_to_sqs(error_payload)
        self.assertFalse(result)
        self.sqs_mock.send_message.assert_not_called()
    
    def test_send_error_to_sqs_invalid_arn(self):
        """Test sending error message to SQS with invalid ARN."""
        self.file_processor.sqs_dlq_arn = 'invalid:arn'
        error_payload = {
            'error_message': 'Test error',
            'failed_records': []
        }
        result = self.file_processor._send_error_to_sqs(error_payload)
        self.assertFalse(result)
        self.sqs_mock.send_message.assert_not_called()
    
    def test_send_error_to_sqs_exception(self):
        """Test sending error message to SQS when an exception occurs."""
        self.sqs_mock.get_queue_url.side_effect = Exception('Test exception')
        error_payload = {
            'error_message': 'Test error',
            'failed_records': []
        }
        result = self.file_processor._send_error_to_sqs(error_payload)
        self.assertFalse(result)
    
    def test_send_error_to_sqs_large_message(self):
        """Test sending large error message to SQS."""
        error_payload = {
            'error_message': 'Test error',
            'failed_records': [{'data': 'x' * 100000}],  # Large record
            'file_key': 'test.csv',
            'source': 'test'
        }
        result = self.file_processor._send_error_to_sqs(error_payload)
        self.assertTrue(result)
        self.sqs_mock.send_message.assert_called()
    
    def test_print_error_records(self):
        """Test the _print_error_records method."""
        failed_records = [
            {
                'document_id': 'doc1',
                'error_type': 'test_error',
                'error_reason': 'Test reason 1',
                'document': {'id': 'doc1', 'name': 'Test Doc 1'}
            },
            {
                'document_id': 'doc2',
                'error_type': 'test_error',
                'error_reason': 'Test reason 2',
                'document': {'id': 'doc2', 'name': 'Test Doc 2'}
            }
        ]
        
        with patch.object(self.file_processor, '_send_error_to_sqs') as mock_send:
            self.file_processor._print_error_records(failed_records, 'test.csv')
            mock_send.assert_called_once()
            call_args = mock_send.call_args[0][0]
            self.assertEqual(call_args['error_message'], 'Bulk request had 2 failed records for file test.csv')
            self.assertEqual(call_args['file_key'], 'test.csv')
            self.assertEqual(call_args['source'], 'opensearch_ingestion')
            self.assertEqual(len(call_args['failed_records']), 2)
    
    def test_print_error_records_empty(self):
        """Test the _print_error_records method with empty failed records."""
        with patch.object(self.file_processor, '_send_error_to_sqs') as mock_send:
            self.file_processor._print_error_records([], 'test.csv')
            mock_send.assert_not_called()

if __name__ == '__main__':
    unittest.main() 