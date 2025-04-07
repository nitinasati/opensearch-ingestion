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
import time
from opensearch_base_manager import OpenSearchBaseManager, OpenSearchException

class TestOpenSearchBaseManager(unittest.TestCase):
    """Test cases for the OpenSearchBaseManager class."""
    
    @patch('requests.get')
    def setUp(self, mock_get):
        """Set up test environment."""
        # Configure the mock to return a successful response for initialization
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response
        
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
        
        # Create an instance of OpenSearchBaseManager
        self.manager = OpenSearchBaseManager()
        
        # Store the mock_get for use in test methods
        self.mock_get = mock_get
    
    def tearDown(self):
        """Clean up after tests."""
        self.env_patcher.stop()
        self.session_patcher.stop()
    
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
    def test_make_request_with_non_dict_data(self, mock_request):
        """Test request with non-dictionary data payload."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status.return_value = None
        mock_request.return_value = mock_response
        
        # Use a string as data (non-dictionary)
        data = '{"query": {"match_all": {}}}'
        result = self.manager._make_request('POST', '/test-index/_search', data=data)
        
        self.assertEqual(result['status'], 'success')
        mock_request.assert_called_once_with(
            method='POST',
            url='https://test-endpoint.com/test-index/_search',
            headers={
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            },
            data=data,  # Should use data parameter, not json
            auth=ANY,
            verify=False
        )
    
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
    
    def test_get_index_settings_error(self):
        """Test error handling when getting index settings."""
        # Mock the _make_request method
        self.manager._make_request = MagicMock(return_value={
            'status': 'error',
            'response': MagicMock(
                status_code=500,
                text='Internal server error'
            )
        })
        
        # Test data
        index_name = 'test-index'
        
        # Get index settings
        result = self.manager.get_index_settings(index_name)
        
        # Verify the result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Failed to get index settings: Internal server error')
        
        # Verify that _make_request was called with the correct parameters
        self.manager._make_request.assert_called_with(
            'GET',
            '/test-index/_settings'
        )
    
    def test_get_index_settings_success(self):
        """Test successful retrieval of index settings."""
        # Mock the _make_request method
        self.manager._make_request = MagicMock(return_value={
            'status': 'success',
            'response': MagicMock(
                status_code=200,
                json=lambda: {
                    'test-index': {
                        'settings': {
                            'index': {
                                'number_of_shards': '1',
                                'number_of_replicas': '1'
                            }
                        }
                    }
                }
            )
        })
        
        # Test data
        index_name = 'test-index'
        
        # Get index settings
        result = self.manager.get_index_settings(index_name)
        
        # Verify the result
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['message'], 'Index settings retrieved successfully')
        self.assertIn('response', result)
        
        # Verify that _make_request was called with the correct parameters
        self.manager._make_request.assert_called_with(
            'GET',
            '/test-index/_settings'
        )
    
    def test_get_index_settings_not_found(self):
        """Test getting settings for a non-existent index."""
        # Mock the _make_request method
        self.manager._make_request = MagicMock(return_value={
            'status': 'success',
            'response': MagicMock(
                status_code=404,
                json=lambda: {'error': {'type': 'index_not_found_exception'}}
            )
        })
        
        # Test data
        index_name = 'non-existent-index'
        
        # Get index settings
        result = self.manager.get_index_settings(index_name)
        
        # Verify the result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Index does not exist')
        self.assertIn('response', result)
        
        # Verify that _make_request was called with the correct parameters
        self.manager._make_request.assert_called_with(
            'GET',
            '/non-existent-index/_settings'
        )
    
    def test_get_index_settings_exception(self):
        """Test exception handling when getting index settings."""
        # Mock the _make_request method to raise an exception
        self.manager._make_request = MagicMock(side_effect=Exception("Test exception"))
        
        # Test data
        index_name = 'test-index'
        
        # Get index settings
        result = self.manager.get_index_settings(index_name)
        
        # Verify the result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Error getting index settings: Test exception')
        
        # Verify that _make_request was called with the correct parameters
        self.manager._make_request.assert_called_with(
            'GET',
            '/test-index/_settings'
        )

    def test_bulk_index_success(self):
        """Test successful bulk indexing of documents."""
        # This test is no longer applicable as the bulk_index method has been removed
        pass

    def test_bulk_index_error(self):
        """Test bulk indexing when the operation fails."""
        # This test is no longer applicable as the bulk_index method has been removed
        pass

    def test_delete_index_success(self):
        """Test successful deletion of an index."""
        # Mock the _verify_index_exists method to return True
        self.manager._verify_index_exists = MagicMock(return_value=True)
        
        # Mock the _make_request method
        self.manager._make_request = MagicMock(return_value={
            'status': 'success',
            'response': MagicMock(
                status_code=200,
                text='{"acknowledged": true}'
            )
        })
        
        # Test data
        index_name = 'test-index'
        
        # Perform deletion
        result = self.manager._delete_index(index_name)
        
        # Verify the result
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['message'], 'Successfully deleted index test-index')
        
        # Verify that _make_request was called with the correct parameters
        self.manager._make_request.assert_called_with(
            'DELETE',
            '/test-index'
        )

    def test_delete_index_not_exists(self):
        """Test deletion of a non-existent index."""
        # Mock the _verify_index_exists method to return False
        self.manager._verify_index_exists = MagicMock(return_value=False)
        
        # Mock the _make_request method
        self.manager._make_request = MagicMock()
        
        # Test data
        index_name = 'non-existent-index'
        
        # Perform deletion
        result = self.manager._delete_index(index_name)
        
        # Verify the result
        self.assertEqual(result['status'], 'warning')
        self.assertEqual(result['message'], 'Index non-existent-index does not exist')
        
        # Verify that _make_request was not called with DELETE
        self.manager._make_request.assert_not_called()

    def test_delete_index_error(self):
        """Test deleting an index when the operation fails."""
        # Mock the _verify_index_exists method to return True
        self.manager._verify_index_exists = MagicMock(return_value=True)
        
        # Create a response mock with proper text attribute
        response_mock = MagicMock()
        response_mock.status_code = 500
        response_mock.text = 'Internal server error'
        
        # Mock the _make_request method
        self.manager._make_request = MagicMock(return_value={
            'status': 'error',
            'response': response_mock
        })
        
        # Test data
        index_name = 'test-index'
        
        # Perform deletion
        result = self.manager._delete_index(index_name)
        
        # Verify the result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Failed to delete index test-index: Internal server error')
        
        # Verify that _make_request was called with the correct parameters
        self.manager._make_request.assert_called_with(
            'DELETE',
            '/test-index'
        )

    def test_get_index_mappings_success(self):
        """Test successful retrieval of index mappings."""
        # Mock the _make_request method
        self.manager._make_request = MagicMock(return_value={
            'status': 'success',
            'response': MagicMock(
                status_code=200,
                json=lambda: {
                    'test-index': {
                        'mappings': {
                            'properties': {
                                'field1': {'type': 'keyword'},
                                'field2': {'type': 'text'}
                            }
                        }
                    }
                }
            )
        })
        
        # Test data
        index_name = 'test-index'
        
        # Perform get
        result = self.manager._get_index_mappings(index_name)
        
        # Verify the result
        self.assertEqual(result, {
            'properties': {
                'field1': {'type': 'keyword'},
                'field2': {'type': 'text'}
            }
        })
        
        # Verify that _make_request was called with the correct parameters
        self.manager._make_request.assert_called_with(
            'GET',
            '/test-index/_mapping'
        )

    def test_get_index_mappings_not_exists(self):
        """Test getting mappings for a non-existent index."""
        # Mock the _make_request method
        self.manager._make_request = MagicMock(return_value={
            'status': 'error',
            'response': MagicMock(
                status_code=404,
                json=lambda: {'error': {'type': 'index_not_found_exception'}}
            )
        })
        
        # Test data
        index_name = 'non-existent-index'
        
        # Perform get
        result = self.manager._get_index_mappings(index_name)
        
        # Verify the result
        self.assertEqual(result, {})
        
        # Verify that _make_request was called with the correct parameters
        self.manager._make_request.assert_called_with(
            'GET',
            '/non-existent-index/_mapping'
        )

    def test_get_index_mappings_error(self):
        """Test getting mappings when the operation fails."""
        # Mock the _make_request method
        self.manager._make_request = MagicMock(return_value={
            'status': 'error',
            'response': MagicMock(
                status_code=500,
                text='Internal server error'
            )
        })
        
        # Test data
        index_name = 'test-index'
        
        # Perform get
        result = self.manager._get_index_mappings(index_name)
        
        # Verify the result
        self.assertEqual(result, {})
        
        # Verify that _make_request was called with the correct parameters
        self.manager._make_request.assert_called_with(
            'GET',
            '/test-index/_mapping'
        )

    def test_get_index_aliases_success(self):
        """Test successful retrieval of index aliases."""
        # Mock the _make_request method
        self.manager._make_request = MagicMock(return_value={
            'status': 'success',
            'response': MagicMock(
                status_code=200,
                json=lambda: {
                    'test-index': {
                        'aliases': {
                            'alias1': {},
                            'alias2': {}
                        }
                    }
                }
            )
        })
        
        # Test data
        index_name = 'test-index'
        
        # Perform get
        result = self.manager._get_index_aliases(index_name)
        
        # Verify the result
        self.assertEqual(result, ['alias1', 'alias2'])
        
        # Verify that _make_request was called with the correct parameters
        self.manager._make_request.assert_called_with(
            'GET',
            '/test-index/_alias'
        )

    def test_get_index_aliases_not_exists(self):
        """Test getting aliases for a non-existent index."""
        # Mock the _make_request method
        self.manager._make_request = MagicMock(return_value={
            'status': 'error',
            'response': MagicMock(
                status_code=404,
                json=lambda: {'error': {'type': 'index_not_found_exception'}}
            )
        })
        
        # Test data
        index_name = 'non-existent-index'
        
        # Perform get
        result = self.manager._get_index_aliases(index_name)
        
        # Verify the result
        self.assertEqual(result, [])
        
        # Verify that _make_request was called with the correct parameters
        self.manager._make_request.assert_called_with(
            'GET',
            '/non-existent-index/_alias'
        )

    def test_get_index_aliases_error(self):
        """Test getting aliases when the operation fails."""
        # Mock the _make_request method
        self.manager._make_request = MagicMock(return_value={
            'status': 'error',
            'response': MagicMock(
                status_code=500,
                text='Internal server error'
            )
        })
        
        # Test data
        index_name = 'test-index'
        
        # Perform get
        result = self.manager._get_index_aliases(index_name)
        
        # Verify the result
        self.assertEqual(result, [])
        
        # Verify that _make_request was called with the correct parameters
        self.manager._make_request.assert_called_with(
            'GET',
            '/test-index/_alias'
        )

    @patch('time.sleep')
    def test_test_connection_retry_success(self, mock_sleep):
        """Test that _test_connection retries on failure and succeeds eventually."""
        # Reset the mock_get after initialization
        self.mock_get.reset_mock()
        
        # Configure mock_get to fail twice and then succeed
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status.return_value = None
        
        # Create a new side_effect list that includes the initialization success
        # followed by the test scenario (2 failures + 1 success)
        self.mock_get.side_effect = [
            mock_response,  # For initialization
            requests.exceptions.ConnectionError("Connection error"),
            requests.exceptions.ConnectionError("Connection error"),
            mock_response
        ]
        
        # Create a new instance to trigger initialization
        manager = OpenSearchBaseManager()
        
        # Call the method directly
        manager._test_connection()
        
        # Verify that get was called 4 times (1 for init + 3 for test)
        self.assertEqual(self.mock_get.call_count, 4)
        
        # Verify that sleep was called twice with exponential backoff
        self.assertEqual(mock_sleep.call_count, 2)
        mock_sleep.assert_any_call(1)  # First retry: 2^0 = 1 second
        mock_sleep.assert_any_call(2)  # Second retry: 2^1 = 2 seconds
    
    @patch('time.sleep')
    def test_test_connection_all_retries_fail(self, mock_sleep):
        """Test that _test_connection raises an exception after all retries fail."""
        # Reset the mock_get after initialization
        self.mock_get.reset_mock()
        
        # Configure mock_get to succeed for initialization but fail for the test
        mock_response = MagicMock()
        mock_response.status_code = 200
        
        mock_response.raise_for_status.return_value = None
        
        # Create a new side_effect list that includes the initialization success
        # followed by the test scenario (3 failures)
        self.mock_get.side_effect = [
            mock_response,  # For initialization
            requests.exceptions.ConnectionError("Connection error"),
            requests.exceptions.ConnectionError("Connection error"),
            requests.exceptions.ConnectionError("Connection error")
        ]
        
        # Create a new instance to trigger initialization
        manager = OpenSearchBaseManager()
        
        # Expect an OpenSearchException to be raised
        with self.assertRaises(OpenSearchException) as context:
            manager._test_connection()
        
        # Verify the exception message
        self.assertIn("Failed to connect to OpenSearch after 3 attempts", str(context.exception))
        
        # Verify that get was called 4 times (1 for init + 3 for test)
        self.assertEqual(self.mock_get.call_count, 4)
        
        # Verify that sleep was called twice with exponential backoff
        self.assertEqual(mock_sleep.call_count, 2)
        mock_sleep.assert_any_call(1)  # First retry: 2^0 = 1 second
        mock_sleep.assert_any_call(2)  # Second retry: 2^1 = 2 seconds

    def test_delete_index_request_exception(self):
        """Test deleting an index when a request exception occurs."""
        # Mock the _verify_index_exists method to return True
        self.manager._verify_index_exists = MagicMock(return_value=True)
        
        # Mock the _make_request method to raise a RequestException
        self.manager._make_request = MagicMock(side_effect=requests.exceptions.RequestException("Connection error"))
        
        # Test data
        index_name = 'test-index'
        
        # Perform deletion
        result = self.manager._delete_index(index_name)
        
        # Verify the result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Error deleting index test-index: Connection error')
        
        # Verify that _make_request was called with the correct parameters
        self.manager._make_request.assert_called_with(
            'DELETE',
            '/test-index'
        )

if __name__ == '__main__':
    unittest.main() 