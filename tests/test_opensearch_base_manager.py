"""
Unit tests for the OpenSearchBaseManager class.

This module contains tests for base OpenSearch operations, including
authentication, request handling, and error handling.
"""

import unittest
from unittest.mock import patch, MagicMock, ANY
import requests
import json
import os
from opensearch_base_manager import OpenSearchBaseManager, OpenSearchException

class TestOpenSearchBaseManager(unittest.TestCase):
    """Test cases for the OpenSearchBaseManager class."""
    
    def setUp(self):
        """Set up test environment."""
        # Mock environment variables
        self.env_patcher = patch.dict('os.environ', {
            'OPENSEARCH_ENDPOINT': 'test-endpoint.com',
            'AWS_REGION': 'us-east-1',
            'VERIFY_SSL': 'false'
        })
        self.env_patcher.start()
        
        # Mock boto3 session and credentials
        self.mock_session = MagicMock()
        self.mock_credentials = MagicMock()
        self.mock_credentials.access_key = 'test-access-key'
        self.mock_credentials.secret_key = 'test-secret-key'
        self.mock_credentials.token = 'test-token'
        self.mock_session.get_credentials.return_value = self.mock_credentials
        
        self.session_patcher = patch('boto3.Session', return_value=self.mock_session)
        self.session_patcher.start()
        
        # Mock the test connection to avoid actual requests during initialization
        self.test_conn_patcher = patch.object(OpenSearchBaseManager, '_test_connection')
        self.test_conn_patcher.start()
        
        # Create an instance of OpenSearchBaseManager
        self.manager = OpenSearchBaseManager()
    
    def tearDown(self):
        """Clean up after tests."""
        self.env_patcher.stop()
        self.session_patcher.stop()
        self.test_conn_patcher.stop()
    
    def test_init_success(self):
        """Test successful initialization of OpenSearchBaseManager."""
        self.assertEqual(self.manager.opensearch_endpoint, 'test-endpoint.com')
        self.assertEqual(self.manager.aws_region, 'us-east-1')
        self.assertFalse(self.manager.verify_ssl)
    
    def test_init_no_endpoint(self):
        """Test initialization without OpenSearch endpoint."""
        # Create a clean environment without OPENSEARCH_ENDPOINT
        with patch.dict('os.environ', {
            'AWS_REGION': 'us-east-1',
            'VERIFY_SSL': 'false'
        }, clear=True):  # clear=True removes all other env vars
            with self.assertRaises(ValueError) as context:
                OpenSearchBaseManager()
            self.assertEqual(str(context.exception), "OpenSearch endpoint is required")
    
    
    @patch('requests.request')
    def test_make_request_success(self, mock_request):
        """Test successful request to OpenSearch."""
        # Mock successful response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'acknowledged': True}
        mock_response.raise_for_status.return_value = None
        mock_request.return_value = mock_response
        
        result = self.manager._make_request('GET', '/test-index')
        
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['message'], 'Request completed successfully')
        self.assertEqual(result['response'], mock_response)
    
    @patch('requests.request')
    def test_make_request_with_data(self, mock_request):
        """Test request with data payload."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status.return_value = None
        mock_request.return_value = mock_response
        
        data = {'query': {'match_all': {}}}
        result = self.manager._make_request('POST', '/test-index/_search', data=data)
        
        self.assertEqual(result['status'], 'success')
        mock_request.assert_called_once_with(
            method='POST',
            url='https://test-endpoint.com/test-index/_search',
            headers={
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            },
            json=data,
            auth=ANY,
            verify=False
        )
    
    @patch('requests.request')
    def test_make_request_retry_success(self, mock_request):
        """Test request retry mechanism."""
        # First call fails, second succeeds
        mock_success_response = MagicMock()
        mock_success_response.status_code = 200
        mock_success_response.raise_for_status.return_value = None
        
        mock_request.side_effect = [
            requests.exceptions.RequestException("Connection error"),
            mock_success_response
        ]
        
        result = self.manager._make_request('GET', '/test-index')
        
        self.assertEqual(result['status'], 'success')
        self.assertEqual(mock_request.call_count, 2)
    
    @patch('requests.request')
    def test_make_request_all_retries_fail(self, mock_request):
        """Test request when all retries fail."""
        mock_request.side_effect = requests.exceptions.RequestException("Connection error")
        
        result = self.manager._make_request('GET', '/test-index')
        
        self.assertEqual(result['status'], 'error')
        self.assertIn('Failed to make request to OpenSearch after 3 attempts', result['message'])
        self.assertEqual(mock_request.call_count, 3)
    
    def test_verify_index_exists_true(self):
        """Test index existence verification when index exists."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status.return_value = None
        
        self.manager._make_request = MagicMock(return_value={
            'status': 'success',
            'response': mock_response
        })
        
        result = self.manager._verify_index_exists('test-index')
        
        self.assertTrue(result)
        self.manager._make_request.assert_called_once_with('HEAD', '/test-index')
    
    def test_verify_index_exists_false(self):
        """Test index existence verification when index does not exist."""
        self.manager._make_request = MagicMock(return_value={
            'status': 'error',
            'message': 'Index does not exist'
        })
        
        result = self.manager._verify_index_exists('test-index')
        
        self.assertFalse(result)
    
    def test_get_index_count_success(self):
        """Test getting document count from an index."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'count': 100}
        mock_response.raise_for_status.return_value = None
        
        self.manager._make_request = MagicMock(return_value={
            'status': 'success',
            'response': mock_response
        })
        
        count = self.manager._get_index_count('test-index')
        
        self.assertEqual(count, 100)
        self.manager._make_request.assert_called_once_with('GET', '/test-index/_count')
    
    def test_get_index_count_error(self):
        """Test getting document count when request fails."""
        self.manager._make_request = MagicMock(return_value={
            'status': 'error',
            'message': 'Error getting count'
        })
        
        count = self.manager._get_index_count('test-index')
        
        self.assertEqual(count, 0)
    
    def test_check_index_aliases_success(self):
        """Test checking index aliases when aliases exist."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {'alias': 'alias1', 'index': 'test-index'},
            {'alias': 'alias2', 'index': 'test-index'}
        ]
        mock_response.raise_for_status.return_value = None
        
        self.manager._verify_index_exists = MagicMock(return_value=True)
        self.manager._make_request = MagicMock(return_value={
            'status': 'success',
            'response': mock_response
        })
        
        aliases = self.manager._check_index_aliases('test-index')
        
        self.assertEqual(len(aliases), 2)
        self.assertIn('alias1', aliases)
        self.assertIn('alias2', aliases)
    
    def test_check_index_aliases_no_aliases(self):
        """Test checking index aliases when no aliases exist."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = []
        mock_response.raise_for_status.return_value = None
        
        self.manager._verify_index_exists = MagicMock(return_value=True)
        self.manager._make_request = MagicMock(return_value={
            'status': 'success',
            'response': mock_response
        })
        
        aliases = self.manager._check_index_aliases('test-index')
        
        self.assertEqual(aliases, {})
    
    def test_delete_all_documents_success(self):
        """Test successful deletion of all documents from an index."""
        mock_delete_response = MagicMock()
        mock_delete_response.status_code = 200
        mock_delete_response.json.return_value = {'deleted': 100}
        mock_delete_response.raise_for_status.return_value = None
        
        mock_merge_response = MagicMock()
        mock_merge_response.status_code = 200
        mock_merge_response.raise_for_status.return_value = None
        
        self.manager._make_request = MagicMock(side_effect=[
            {
                'status': 'success',
                'response': mock_delete_response
            },
            {
                'status': 'success',
                'response': mock_merge_response
            }
        ])
        
        result = self.manager._delete_all_documents('test-index')
        
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['documents_deleted'], 100)
        self.assertEqual(self.manager._make_request.call_count, 2)
    
    def test_bulk_index_success(self):
        """Test successful bulk indexing of documents."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'items': [{'index': {'status': 201}} for _ in range(2)]}
        mock_response.raise_for_status.return_value = None
        
        self.manager._make_request = MagicMock(return_value={
            'status': 'success',
            'response': mock_response
        })
        
        documents = [
            {'id': 1, 'field': 'value1'},
            {'id': 2, 'field': 'value2'}
        ]
        
        result = self.manager.bulk_index('test-index', documents)
        
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['message'], 'Bulk indexing completed successfully')
        
        # Verify the bulk request format
        self.manager._make_request.assert_called_once_with(
            'POST',
            '/_bulk',
            data=ANY,
            headers={'Content-Type': 'application/x-ndjson'}
        )
    
    def test_bulk_index_error(self):
        """Test bulk indexing when request fails."""
        self.manager._make_request = MagicMock(return_value={
            'status': 'error',
            'message': 'Bulk indexing failed'
        })
        
        documents = [{'id': 1, 'field': 'value1'}]
        result = self.manager.bulk_index('test-index', documents)
        
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Bulk indexing failed')

if __name__ == '__main__':
    unittest.main() 