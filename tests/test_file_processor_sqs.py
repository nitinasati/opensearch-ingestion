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
        # Mock environment variables
        self.env_patcher = patch.dict('os.environ', {
            'SQS-DLQ-ARN': 'arn:aws:sqs:us-east-1:416449661344:mysamplequeue-dlq',
            'DLQ': 'enabled'  # Enable DLQ for tests
        })
        self.env_patcher.start()
        
        # Create a mock for boto3.client
        self.sqs_client_mock = MagicMock()
        self.sqs_client_mock.send_message.return_value = {'MessageId': 'test-message-id'}
        self.sqs_client_mock.get_queue_url.return_value = {'QueueUrl': 'https://sqs.us-east-1.amazonaws.com/416449661344/mysamplequeue-dlq'}
        
        # Create a mock for boto3.client
        self.boto3_patcher = patch('boto3.client', return_value=self.sqs_client_mock)
        self.boto3_patcher.start()
        
        # Initialize the FileProcessor
        self.file_processor = FileProcessor()
        
    def tearDown(self):
        """Clean up after tests."""
        self.env_patcher.stop()
        self.boto3_patcher.stop()
    
    def test_init_with_sqs_arn(self):
        """Test initialization with SQS ARN."""
        # Verify that the SQS client was created
        self.assertIsNotNone(self.file_processor.sqs_client)
        self.assertEqual(self.file_processor.sqs_dlq_arn, 'arn:aws:sqs:us-east-1:416449661344:mysamplequeue-dlq')
        self.assertTrue(self.file_processor.dlq_enabled)
    
    def test_init_without_sqs_arn(self):
        """Test initialization without SQS ARN."""
        # Mock environment variables without SQS ARN but with required OpenSearch endpoint
        with patch.dict('os.environ', {
            'OPENSEARCH_ENDPOINT': 'test-endpoint',
            'AWS_REGION': 'us-east-1',
            'VERIFY_SSL': 'false',
            'DLQ': 'enabled'  # DLQ is enabled but no ARN
        }, clear=True):
            # Mock AWS session and credentials
            mock_session = MagicMock()
            mock_credentials = MagicMock()
            mock_credentials.access_key = 'test-access-key'
            mock_credentials.secret_key = 'test-secret-key'
            mock_credentials.token = 'test-token'
            mock_session.get_credentials.return_value = mock_credentials
            
            # Mock boto3.Session
            with patch('boto3.Session', return_value=mock_session):
                # Mock AWS4Auth
                with patch('requests_aws4auth.AWS4Auth', return_value=MagicMock()):
                    # Mock requests to prevent actual HTTP calls
                    with patch('requests.get', return_value=MagicMock(status_code=200)):
                        with patch('requests.post', return_value=MagicMock(status_code=200)):
                            file_processor = FileProcessor()
                            self.assertIsNone(file_processor.sqs_dlq_arn)
                            self.assertFalse(file_processor.dlq_enabled)
    
    def test_init_with_dlq_disabled(self):
        """Test initialization with DLQ disabled."""
        # Mock environment variables with DLQ disabled
        with patch.dict('os.environ', {
            'SQS-DLQ-ARN': 'arn:aws:sqs:us-east-1:416449661344:mysamplequeue-dlq',
            'DLQ': 'disabled',
            'OPENSEARCH_ENDPOINT': 'test-endpoint',
            'AWS_REGION': 'us-east-1',
            'VERIFY_SSL': 'false'
        }, clear=True):
            # Mock AWS session and credentials
            mock_session = MagicMock()
            mock_credentials = MagicMock()
            mock_credentials.access_key = 'test-access-key'
            mock_credentials.secret_key = 'test-secret-key'
            mock_credentials.token = 'test-token'
            mock_session.get_credentials.return_value = mock_credentials
            
            # Mock boto3.Session
            with patch('boto3.Session', return_value=mock_session):
                # Mock AWS4Auth
                with patch('requests_aws4auth.AWS4Auth', return_value=MagicMock()):
                    # Mock requests to prevent actual HTTP calls
                    with patch('requests.get', return_value=MagicMock(status_code=200)):
                        with patch('requests.post', return_value=MagicMock(status_code=200)):
                            file_processor = FileProcessor()
                            self.assertIsNone(file_processor.sqs_client)
                            self.assertIsNone(file_processor.sqs_dlq_arn)
                            self.assertFalse(file_processor.dlq_enabled)
    
    def test_send_error_to_sqs_success(self):
        """Test successful sending of error message to SQS."""
        # Create a test error payload
        error_payload = {
            'error_message': 'Test error message',
            'failed_records': [
                {
                    'document_id': 'doc1',
                    'error_type': 'test_error',
                    'error_reason': 'Test reason',
                    'document': {'id': 'doc1', 'name': 'Test Doc'}
                }
            ],
            'file_key': 'test.csv',
            'source': 'opensearch_ingestion'
        }
        
        # Call the method
        result = self.file_processor._send_error_to_sqs(error_payload)
        
        # Verify the result
        self.assertTrue(result)
        
        # Verify that get_queue_url was called
        self.sqs_client_mock.get_queue_url.assert_called_once_with(QueueName='mysamplequeue-dlq')
        
        # Verify that send_message was called with the correct arguments
        self.sqs_client_mock.send_message.assert_called_once()
        call_args = self.sqs_client_mock.send_message.call_args[1]
        
        # Verify the queue URL
        self.assertEqual(call_args['QueueUrl'], 'https://sqs.us-east-1.amazonaws.com/416449661344/mysamplequeue-dlq')
        
        # Verify the message body contains the error payload
        message_body = json.loads(call_args['MessageBody'])
        self.assertEqual(message_body['error_message'], 'Test error message')
        self.assertEqual(message_body['file_key'], 'test.csv')
        self.assertEqual(message_body['source'], 'opensearch_ingestion')
        self.assertEqual(len(message_body['failed_records']), 1)
        self.assertEqual(message_body['failed_records'][0]['document_id'], 'doc1')
        self.assertIn('timestamp', message_body)
    
    def test_send_error_to_sqs_without_arn(self):
        """Test sending error message to SQS when ARN is not configured."""
        # Set sqs_dlq_arn to None
        self.file_processor.sqs_dlq_arn = None
        
        # Create a test error payload
        error_payload = {
            'error_message': 'Test error message',
            'failed_records': []
        }
        
        # Call the method
        result = self.file_processor._send_error_to_sqs(error_payload)
        
        # Verify the result
        self.assertFalse(result)
        
        # Verify that send_message was not called
        self.sqs_client_mock.send_message.assert_not_called()
    
    def test_send_error_to_sqs_invalid_arn(self):
        """Test sending error message to SQS with invalid ARN."""
        # Set an invalid ARN
        self.file_processor.sqs_dlq_arn = 'invalid:arn'
        
        # Create a test error payload
        error_payload = {
            'error_message': 'Test error message',
            'failed_records': []
        }
        
        # Call the method
        result = self.file_processor._send_error_to_sqs(error_payload)
        
        # Verify the result
        self.assertFalse(result)
        
        # Verify that send_message was not called
        self.sqs_client_mock.send_message.assert_not_called()
    
    def test_send_error_to_sqs_exception(self):
        """Test sending error message to SQS when an exception occurs."""
        # Make get_queue_url raise an exception
        self.sqs_client_mock.get_queue_url.side_effect = Exception('Test exception')
        
        # Create a test error payload
        error_payload = {
            'error_message': 'Test error message',
            'failed_records': []
        }
        
        # Call the method
        result = self.file_processor._send_error_to_sqs(error_payload)
        
        # Verify the result
        self.assertFalse(result)
    
    def test_send_error_to_sqs_large_message(self):
        """Test sending a large error message that needs to be split."""
        # Create a large error payload with fewer but larger records
        failed_records = []
        for i in range(10):  # Reduced from 1000 to 10 records
            failed_records.append({
                'document_id': f'doc{i}',
                'error_type': 'test_error',
                'error_reason': 'Test reason',
                'document': {'id': f'doc{i}', 'name': f'Test Doc {i}', 'data': 'x' * 10000}  # Increased size per record
            })
        
        error_payload = {
            'error_message': 'Test error message with many failed records',
            'failed_records': failed_records,
            'file_key': 'test.csv',
            'source': 'opensearch_ingestion'
        }
        
        # Mock the json.dumps to return a large string
        with patch('json.dumps', return_value='x' * 300000):  # 300 KB
            # Call the method
            result = self.file_processor._send_error_to_sqs(error_payload)
            
            # Verify the result
            self.assertTrue(result)
            
            # Verify that get_queue_url was called
            self.sqs_client_mock.get_queue_url.assert_called_once_with(QueueName='mysamplequeue-dlq')
            
            # Verify that send_message was called multiple times
            self.assertGreater(self.sqs_client_mock.send_message.call_count, 1)
            
            # Verify that each call includes the correct metadata
            for call_args in self.sqs_client_mock.send_message.call_args_list:
                # We can't parse the message body as JSON because it's a string of 'x' characters
                # Instead, we'll verify that the queue URL is correct
                self.assertEqual(call_args[1]['QueueUrl'], 'https://sqs.us-east-1.amazonaws.com/416449661344/mysamplequeue-dlq')
    
    def test_print_error_records(self):
        """Test the _print_error_records method."""
        # Create test failed records
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
        
        # Mock the _send_error_to_sqs method
        with patch.object(self.file_processor, '_send_error_to_sqs') as mock_send:
            # Call the method
            self.file_processor._print_error_records(failed_records, 'test.csv')
            
            # Verify that _send_error_to_sqs was called with the correct arguments
            mock_send.assert_called_once()
            call_args = mock_send.call_args[0][0]
            
            # Verify the error payload
            self.assertEqual(call_args['error_message'], 'Bulk request had 2 failed records for file test.csv')
            self.assertEqual(call_args['file_key'], 'test.csv')
            self.assertEqual(call_args['source'], 'opensearch_ingestion')
            self.assertEqual(len(call_args['failed_records']), 2)
            self.assertEqual(call_args['failed_records'][0]['document_id'], 'doc1')
            self.assertEqual(call_args['failed_records'][1]['document_id'], 'doc2')
    
    def test_print_error_records_empty(self):
        """Test the _print_error_records method with empty failed records."""
        # Mock the _send_error_to_sqs method
        with patch.object(self.file_processor, '_send_error_to_sqs') as mock_send:
            # Call the method with empty failed records
            self.file_processor._print_error_records([], 'test.csv')
            
            # Verify that _send_error_to_sqs was not called
            mock_send.assert_not_called()

if __name__ == '__main__':
    unittest.main() 