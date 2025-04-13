"""
Unit tests for the OpenSearchAliasManager class.

This module contains tests for alias management operations, including
alias creation, switching, and verification.
"""

import unittest
from unittest.mock import patch, MagicMock
import json
from switch_alias import OpenSearchAliasManager, ALIASES_ENDPOINT

class TestOpenSearchAliasManager(unittest.TestCase):
    """Test cases for the OpenSearchAliasManager class."""
    
    def setUp(self):
        """Set up test environment."""
        # Create mock for OpenSearch connection
        self.opensearch_mock = MagicMock()
        self.opensearch_mock.info.return_value = {'version': {'number': '7.10.2'}}
        self.opensearch_mock.indices.exists.return_value = True
        self.opensearch_mock.indices.get.return_value = {'test-index': {'mappings': {}}}
        self.opensearch_mock.indices.stats.return_value = {'indices': {'test-index': {'total': {'docs': {'count': 0}}}}}
        self.opensearch_mock.indices.get_alias.return_value = {'test-index': {'aliases': {'test-alias': {}}}}
        self.opensearch_mock.indices.update_aliases.return_value = {'acknowledged': True}
        
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
        
        # Apply patches
        self.opensearch_patcher = patch('opensearchpy.OpenSearch', return_value=self.opensearch_mock)
        self.requests_patcher = patch('requests.get', return_value=self.requests_mock.get.return_value)
        self.manager_patcher = patch('opensearch_base_manager.OpenSearchBaseManager', return_value=self.manager_mock)
        
        self.opensearch_patcher.start()
        self.requests_patcher.start()
        self.manager_patcher.start()
        
        # Initialize the alias manager
        self.alias_manager = OpenSearchAliasManager()
        self.alias_manager.opensearch_manager = self.manager_mock
    
    def tearDown(self):
        """Clean up after tests."""
        self.opensearch_patcher.stop()
        self.requests_patcher.stop()
        self.manager_patcher.stop()
    
    def test_init(self):
        """Test initialization of the OpenSearchAliasManager class."""
        self.assertIsNotNone(self.alias_manager)
    
    def test_switch_alias_success(self):
        """Test successful alias switching."""
        # Mock _make_request to return success for all requests
        mock_alias_response = MagicMock()
        mock_alias_response.status_code = 200
        mock_alias_response.json = MagicMock(return_value={
            'old-index': {
                'aliases': {
                    'test-alias': {}
                }
            }
        })
        
        mock_switch_response = MagicMock()
        mock_switch_response.status_code = 200
        
        mock_index_response = MagicMock()
        mock_index_response.status_code = 200
        
        mock_count_response = MagicMock()
        mock_count_response.status_code = 200
        mock_count_response.json = MagicMock(return_value={'count': 100})
        
        def mock_make_request(method, endpoint, data=None, headers=None):
            if endpoint == '/_alias/test-alias':
                return {
                    'status': 'success',
                    'response': mock_alias_response
                }
            elif endpoint == '/_aliases':
                return {
                    'status': 'success',
                    'response': mock_switch_response
                }
            elif endpoint == '/old-index' or endpoint == '/new-index':
                return {
                    'status': 'success',
                    'response': mock_index_response
                }
            elif endpoint == '/old-index/_count' or endpoint == '/new-index/_count':
                return {
                    'status': 'success',
                    'response': mock_count_response
                }
            return {
                'status': 'error',
                'message': 'Unexpected request'
            }
        
        self.alias_manager._make_request = MagicMock(side_effect=mock_make_request)

        # Switch alias
        result = self.alias_manager.switch_alias('test-alias', 'old-index', 'new-index')

        # Verify the result
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['message'], 'Successfully switched alias test-alias from old-index to new-index')
        self.assertEqual(result['source_count'], 100)
        self.assertEqual(result['target_count'], 100)
        self.assertEqual(result['percentage_diff'], 0)

        # Verify method calls
        self.alias_manager._make_request.assert_called()
    
    def test_switch_alias_index_not_exists(self):
        """Test switching alias when source index does not exist."""
        # Mock _get_alias_info to return valid alias info
        self.alias_manager._get_alias_info = MagicMock(return_value={
            'test-alias': {
                'aliases': {
                    'test-alias': {}
                }
            }
        })
        
        # Mock _verify_index_exists to return False for source index
        self.alias_manager._verify_index_exists = MagicMock(return_value=False)
        
        # Mock _make_request to return success for target index
        self.alias_manager._make_request = MagicMock(return_value={
            'status': 'success',
            'response': MagicMock(status_code=200)
        })
        
        # Mock _get_index_count to return 100 for both indices
        self.alias_manager._get_index_count = MagicMock(return_value=100)
        
        # Call switch_alias
        result = self.alias_manager.switch_alias('test-alias', 'test-index', 'test-index-new')
        
        # Verify result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Source index test-index does not exist')
        
        # Verify method calls
        self.alias_manager._get_alias_info.assert_called_once_with('test-alias')
        self.alias_manager._verify_index_exists.assert_called_once_with('test-index')
        self.alias_manager._make_request.assert_not_called()
    
    def test_switch_alias_request_error(self):
        """Test alias switching when request fails."""
        # Mock _get_alias_info to return valid alias info
        self.alias_manager._get_alias_info = MagicMock(return_value={
            'old-index': {
                'aliases': {
                    'test-alias': {}
                }
            }
        })
        
        # Mock _verify_index_exists to return True for both indices
        self.alias_manager._verify_index_exists = MagicMock(return_value=True)
        
        # Mock _make_request to return success for alias info but error for switch operation
        def mock_make_request(method, path, data=None, headers=None):
            if path == '/_alias/test-alias':
                return {
                    'status': 'success',
                    'response': MagicMock(
                        status_code=200,
                        json=lambda: {
                            'old-index': {
                                'aliases': {
                                    'test-alias': {}
                                }
                            }
                        }
                    )
                }
            elif path == '/_aliases':
                return {
                    'status': 'error',
                    'message': 'Request failed'
                }
            return {
                'status': 'success',
                'response': MagicMock(status_code=200)
            }
        
        self.alias_manager._make_request = MagicMock(side_effect=mock_make_request)
        
        # Mock _get_index_count to return 100 for both indices
        self.alias_manager._get_index_count = MagicMock(return_value=100)
        
        # Call switch_alias
        result = self.alias_manager.switch_alias('test-alias', 'old-index', 'new-index')
        
        # Verify result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Failed to switch alias: Request failed')
        
        # Verify method calls
        self.alias_manager._get_alias_info.assert_called_once_with('test-alias')
        self.alias_manager._verify_index_exists.assert_any_call('old-index')
        self.alias_manager._verify_index_exists.assert_any_call('new-index')
        self.alias_manager._make_request.assert_called()
    
    def test_get_alias_info_success(self):
        """Test successful alias info retrieval."""
        # Mock the _make_request method
        self.alias_manager._make_request = MagicMock(return_value={
            'status': 'success',
            'response': MagicMock(
                status_code=200,
                json=lambda: {
                    'test-index': {
                        'aliases': {
                            'test-alias': {}
                        }
                    }
                }
            )
        })
        
        # Get alias info
        result = self.alias_manager._get_alias_info('test-alias')
        
        # Verify the result
        self.assertIsNotNone(result)
        self.assertEqual(result['test-index']['aliases']['test-alias'], {})
        
        # Verify that _make_request was called with correct parameters
        self.alias_manager._make_request.assert_called_once_with('GET', '/_alias/test-alias')
    
    def test_get_alias_info_not_exists(self):
        """Test getting alias info when alias does not exist."""
        # Mock _make_request to return empty aliases
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json = MagicMock(return_value={})
        
        self.alias_manager._make_request = MagicMock(return_value={
            'status': 'success',
            'response': mock_response
        })
        
        result = self.alias_manager._get_alias_info('non-existent-alias')
        self.assertEqual(result, {}, "Should return an empty dictionary when alias does not exist")
    
    def test_get_alias_info_api_error(self):
        """Test getting alias info when _make_request returns an error."""
        # Mock _make_request to return an error status
        self.alias_manager._make_request = MagicMock(return_value={
            'status': 'error',
            'message': 'API request failed'
        })
        
        result = self.alias_manager._get_alias_info('test-alias')
        self.assertEqual(result, {}, "Should return an empty dictionary on API error")
        self.alias_manager._make_request.assert_called_once_with('GET', '/_alias/test-alias')

    def test_get_alias_info_non_200_status(self):
        """Test getting alias info when the API returns a non-200 status code."""
        # Mock _make_request to return success status but non-200 response code
        mock_response = MagicMock()
        mock_response.status_code = 404  # Example: Not Found
        
        self.alias_manager._make_request = MagicMock(return_value={
            'status': 'success',
            'response': mock_response
        })
        
        result = self.alias_manager._get_alias_info('not-found-alias')
        self.assertEqual(result, {}, "Should return an empty dictionary on non-200 status code")
        self.alias_manager._make_request.assert_called_once_with('GET', '/_alias/not-found-alias')

    def test_get_alias_info_request_exception(self):
        """Test getting alias info when _make_request raises an exception."""
        # Mock _make_request to raise an exception
        self.alias_manager._make_request = MagicMock(side_effect=Exception("Network Error"))
        
        result = self.alias_manager._get_alias_info('test-alias')
        self.assertEqual(result, {}, "Should return an empty dictionary on request exception")
        self.alias_manager._make_request.assert_called_once_with('GET', '/_alias/test-alias')

    def test_switch_alias_same_indices(self):
        """Test switching alias when source and target indices are the same."""
        alias_name = 'test-alias'
        source_index = 'test-index'
        target_index = 'test-index'

        # Mock _get_alias_info and _verify_index_exists
        self.alias_manager._get_alias_info = MagicMock()
        self.alias_manager._verify_index_exists = MagicMock()

        # Call the switch_alias method
        result = self.alias_manager.switch_alias(alias_name, source_index, target_index)

        # Verify the result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Source test-index and target index test-index are the same hence aborting the operation')

        # Verify that _verify_index_exists was not called since the method should return early
        self.alias_manager._verify_index_exists.assert_not_called()
        self.alias_manager._get_alias_info.assert_not_called()

    def test_switch_alias_same_indices_case_sensitive(self):
        """Test switching alias when source and target indices are the same but with different case."""
        # Call switch_alias with same source and target but different case
        result = self.alias_manager.switch_alias('test-alias', 'Test-Index', 'test-index')

        # Verify the result - should be treated as different indices
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Alias test-alias does not exist')

    @patch('argparse.ArgumentParser.parse_args')
    @patch('switch_alias.OpenSearchAliasManager')
    def test_main_success(self, mock_alias_manager_class, mock_parse_args):
        """Test the main function with successful alias switching."""
        # Set up mock arguments
        mock_args = MagicMock()
        mock_args.alias = 'test-alias'
        mock_args.source = 'source-index'
        mock_args.target = 'target-index'
        mock_parse_args.return_value = mock_args
        
        # Set up mock alias manager
        mock_alias_manager = MagicMock()
        mock_alias_manager_class.return_value = mock_alias_manager
        
        # Set up mock result
        mock_result = {
            'status': 'success',
            'message': 'Successfully switched alias test-alias from source-index to target-index',
            'source_count': 100,
            'target_count': 100,
            'percentage_diff': 0,
            'total_time_seconds': 1.5
        }
        mock_alias_manager.switch_alias.return_value = mock_result
        
        # Import and call main function
        from switch_alias import main
        result = main()
        
        # Verify result
        self.assertEqual(result, 0)
        
        # Verify alias manager was initialized
        mock_alias_manager_class.assert_called_once()
        
        # Verify switch_alias was called with correct arguments
        mock_alias_manager.switch_alias.assert_called_once_with('test-alias', 'source-index', 'target-index')

    @patch('argparse.ArgumentParser.parse_args')
    @patch('switch_alias.OpenSearchAliasManager')
    def test_main_error(self, mock_alias_manager_class, mock_parse_args):
        """Test the main function with error in alias switching."""
        # Set up mock arguments
        mock_args = MagicMock()
        mock_args.alias = 'test-alias'
        mock_args.source = 'source-index'
        mock_args.target = 'target-index'
        mock_parse_args.return_value = mock_args
        
        # Set up mock alias manager
        mock_alias_manager = MagicMock()
        mock_alias_manager_class.return_value = mock_alias_manager
        
        # Set up mock result with error
        mock_result = {
            'status': 'error',
            'message': 'Failed to switch alias: Source index does not exist'
        }
        mock_alias_manager.switch_alias.return_value = mock_result
        
        # Import and call main function
        from switch_alias import main
        result = main()
        
        # Verify result
        self.assertEqual(result, 0)  # Main returns 0 even for error status
        
        # Verify alias manager was initialized
        mock_alias_manager_class.assert_called_once()
        
        # Verify switch_alias was called with correct arguments
        mock_alias_manager.switch_alias.assert_called_once_with('test-alias', 'source-index', 'target-index')

    @patch('argparse.ArgumentParser.parse_args')
    def test_main_exception(self, mock_parse_args):
        """Test the main function with exception."""
        # Set up mock arguments
        mock_args = MagicMock()
        mock_args.alias = 'test-alias'
        mock_args.source = 'source-index'
        mock_args.target = 'target-index'
        mock_parse_args.return_value = mock_args
        
        # Set up mock to raise exception
        with patch('switch_alias.OpenSearchAliasManager', side_effect=ValueError("Configuration error")):
            # Import and call main function
            from switch_alias import main
            result = main()
            
            # Verify result
            self.assertEqual(result, 1)  # Main returns 1 for exceptions

    def test_switch_alias_target_index_not_exists(self):
        """Test switching alias when target index does not exist."""
        # Mock the necessary methods
        self.alias_manager._get_alias_info = MagicMock(return_value={
            "source-index": {
                "aliases": {
                    "test-alias": {}
                }
            }
        })
        self.alias_manager._verify_index_exists = MagicMock(side_effect=lambda index: index == "source-index")
        self.alias_manager._validate_document_count_difference = MagicMock()
        
        # Call switch_alias method
        result = self.alias_manager.switch_alias(
            alias_name="test-alias",
            source_index="source-index",
            target_index="non-existent-index"
        )
        
        # Verify the result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Target index non-existent-index does not exist')
        
        # Verify method calls
        self.alias_manager._get_alias_info.assert_called_once_with('test-alias')
        self.alias_manager._verify_index_exists.assert_any_call('source-index')
        self.alias_manager._verify_index_exists.assert_any_call('non-existent-index')
        self.alias_manager._validate_document_count_difference.assert_not_called()

    def test_create_alias_error_status_code(self):
        """Test creating an alias when the response status code is not 200."""
        # Mock the _make_request method to return a response with status code 400
        mock_response = MagicMock()
        mock_response.status_code = 400
        
        self.alias_manager._make_request = MagicMock(return_value=mock_response)
        
        # Call _create_alias method
        result = self.alias_manager._create_alias(
            alias_name="test-alias",
            index_name="test-index"
        )
        
        # Verify the result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Failed to create alias. Status code: 400')
        
        # Verify method calls
        self.alias_manager._make_request.assert_called_once_with(
            'POST',
            ALIASES_ENDPOINT,
            data={
                "actions": [
                    {
                        "add": {
                            "index": "test-index",
                            "alias": "test-alias"
                        }
                    }
                ]
            }
        )

    def test_create_alias_exception(self):
        """Test creating an alias when an exception occurs."""
        # Mock the _make_request method to raise an exception
        self.alias_manager._make_request = MagicMock(side_effect=Exception("Connection error"))
        
        # Call _create_alias method
        result = self.alias_manager._create_alias(
            alias_name="test-alias",
            index_name="test-index"
        )
        
        # Verify the result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Error creating alias: Connection error')
        
        # Verify method calls
        self.alias_manager._make_request.assert_called_once_with(
            'POST',
            ALIASES_ENDPOINT,
            data={
                "actions": [
                    {
                        "add": {
                            "index": "test-index",
                            "alias": "test-alias"
                        }
                    }
                ]
            }
        )

    def test_switch_alias_non_200_status_code(self):
        """Test alias switching when the response status code is not 200."""
        # Mock _get_alias_info to return valid alias info
        self.alias_manager._get_alias_info = MagicMock(return_value={
            'old-index': {
                'aliases': {
                    'test-alias': {}
                }
            }
        })
        
        # Mock _verify_index_exists to return True for both indices
        self.alias_manager._verify_index_exists = MagicMock(return_value=True)
        
        # Mock _get_index_count to return 100 for both indices
        self.alias_manager._get_index_count = MagicMock(return_value=100)
        
        # Create a mock response with non-200 status code
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        
        # Mock _make_request to return success for alias info but non-200 status for switch operation
        def mock_make_request(method, path, data=None, headers=None):
            if path == '/_alias/test-alias':
                return {
                    'status': 'success',
                    'response': MagicMock(
                        status_code=200,
                        json=lambda: {
                            'old-index': {
                                'aliases': {
                                    'test-alias': {}
                                }
                            }
                        }
                    )
                }
            elif path == '/_aliases':
                return {
                    'status': 'success',
                    'response': mock_response
                }
            return {
                'status': 'success',
                'response': MagicMock(status_code=200)
            }
        
        self.alias_manager._make_request = MagicMock(side_effect=mock_make_request)
        
        # Call switch_alias
        result = self.alias_manager.switch_alias('test-alias', 'old-index', 'new-index')
        
        # Verify result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Failed to switch alias. Status code: 500')
        self.assertEqual(result['response'], 'Internal Server Error')
        
        # Verify method calls
        self.alias_manager._get_alias_info.assert_called_once_with('test-alias')
        self.alias_manager._verify_index_exists.assert_any_call('old-index')
        self.alias_manager._verify_index_exists.assert_any_call('new-index')
        self.alias_manager._make_request.assert_called()

    def test_switch_alias_exception_handling(self):
        """Test exception handling in the switch_alias method."""
        # Mock _get_alias_info to return valid alias info
        self.alias_manager._get_alias_info = MagicMock(return_value={
            'old-index': {
                'aliases': {
                    'test-alias': {}
                }
            }
        })
        
        # Mock _verify_index_exists to return True for both indices
        self.alias_manager._verify_index_exists = MagicMock(return_value=True)
        
        # Mock _get_index_count to return 100 for both indices
        self.alias_manager._get_index_count = MagicMock(return_value=100)
        
        # Mock _make_request to raise an exception
        self.alias_manager._make_request = MagicMock(side_effect=Exception("Test exception"))
        
        # Call switch_alias
        result = self.alias_manager.switch_alias('test-alias', 'old-index', 'new-index')
        
        # Verify result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Error during alias switch: Test exception')
        
        # Verify method calls
        self.alias_manager._get_alias_info.assert_called_once_with('test-alias')
        self.alias_manager._verify_index_exists.assert_any_call('old-index')
        self.alias_manager._verify_index_exists.assert_any_call('new-index')
        self.alias_manager._make_request.assert_called()

    def test_switch_alias_response_handling(self):
        """Test handling of different response scenarios in switch_alias method."""
        # Mock _get_alias_info to return valid alias info
        self.alias_manager._get_alias_info = MagicMock(return_value={
            'old-index': {
                'aliases': {
                    'test-alias': {}
                }
            }
        })
        
        # Mock _verify_index_exists to return True for both indices
        self.alias_manager._verify_index_exists = MagicMock(return_value=True)
        
        # Mock _get_index_count to return 100 for both indices
        self.alias_manager._get_index_count = MagicMock(return_value=100)
        
        # Test case 1: Successful response (status code 200)
        mock_success_response = {
            'status': 'success',
            'response': MagicMock(status_code=200)
        }
        
        # Test case 2: Error response (status code 500)
        mock_error_response = {
            'status': 'success',
            'response': MagicMock(status_code=500)
        }
        
        # Test case 3: Exception during request
        mock_exception_response = MagicMock(side_effect=Exception("Connection error"))
        
        # Test successful case
        self.alias_manager._make_request = MagicMock(return_value=mock_success_response)
        result = self.alias_manager.switch_alias('test-alias', 'old-index', 'new-index')
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['message'], 'Successfully switched alias test-alias from old-index to new-index')
        
        # Test error status code case
        self.alias_manager._make_request = MagicMock(return_value=mock_error_response)
        result = self.alias_manager.switch_alias('test-alias', 'old-index', 'new-index')
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Failed to switch alias. Status code: 500')
        
        # Test exception case
        self.alias_manager._make_request = mock_exception_response
        result = self.alias_manager.switch_alias('test-alias', 'old-index', 'new-index')
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Error during alias switch: Connection error')
        
        # Verify that _make_request was called with correct parameters
        expected_data = {
            "actions": [
                {
                    "remove": {
                        "index": "old-index",
                        "alias": "test-alias"
                    }
                },
                {
                    "add": {
                        "index": "new-index",
                        "alias": "test-alias"
                    }
                }
            ]
        }
        self.alias_manager._make_request.assert_any_call('POST', ALIASES_ENDPOINT, data=expected_data)

    def test_validate_document_count_difference_success(self):
        """Test successful document count validation when difference is within threshold."""
        # Mock _get_index_count to return specific values
        self.alias_manager._get_index_count = MagicMock(side_effect=[100, 105])
        
        # Set threshold to 10%
        with patch.dict('os.environ', {'DOCUMENT_COUNT_THRESHOLD': '10'}):
            result = self.alias_manager._validate_document_count_difference('source-index', 'target-index')
        
        # Verify result
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['source_count'], 100)
        self.assertEqual(result['target_count'], 105)
        self.assertAlmostEqual(result['percentage_diff'], 5.0)
        self.assertEqual(result['threshold'], 10.0)
        
        # Verify _get_index_count was called with correct arguments
        self.alias_manager._get_index_count.assert_any_call('source-index')
        self.alias_manager._get_index_count.assert_any_call('target-index')
    
    def test_validate_document_count_difference_threshold_exceeded(self):
        """Test document count validation when difference exceeds threshold."""
        # Mock _get_index_count to return values with large difference
        self.alias_manager._get_index_count = MagicMock(side_effect=[100, 120])
        
        # Set threshold to 10%
        with patch.dict('os.environ', {'DOCUMENT_COUNT_THRESHOLD': '10'}):
            result = self.alias_manager._validate_document_count_difference('source-index', 'target-index')
        
        # Verify result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Document count difference (20.00%) exceeds threshold (10.0%)')
        
        # Verify _get_index_count was called with correct arguments
        self.alias_manager._get_index_count.assert_any_call('source-index')
        self.alias_manager._get_index_count.assert_any_call('target-index')
    
    def test_validate_document_count_difference_empty_target(self):
        """Test document count validation when target index is empty."""
        # Mock _get_index_count to return values with empty target
        self.alias_manager._get_index_count = MagicMock(side_effect=[100, 0])
        
        # Call _validate_document_count_difference
        result = self.alias_manager._validate_document_count_difference('source-index', 'target-index')
        
        # Verify result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], "Target index is empty, can't switch alias")
        
        # Verify _get_index_count was called with correct arguments
        self.alias_manager._get_index_count.assert_any_call('source-index')
        self.alias_manager._get_index_count.assert_any_call('target-index')
    
    def test_validate_document_count_difference_zero_source(self):
        """Test document count validation when source index is empty."""
        # Mock _get_index_count to return values with empty source
        self.alias_manager._get_index_count = MagicMock(side_effect=[0, 50])

        # Set threshold to 10%
        with patch.dict('os.environ', {'DOCUMENT_COUNT_THRESHOLD': '10'}):
            result = self.alias_manager._validate_document_count_difference('source-index', 'target-index')

        # Verify result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Error validating document count difference: division by zero')
        
        # Verify _get_index_count was called with correct arguments
        self.alias_manager._get_index_count.assert_any_call('source-index')
        self.alias_manager._get_index_count.assert_any_call('target-index')

    def test_validate_document_count_difference_both_empty(self):
        """Test document count validation when both indices are empty."""
        # Mock _get_index_count to return 0 for both indices
        self.alias_manager._get_index_count = MagicMock(side_effect=[0, 0])
        
        # Call _validate_document_count_difference
        result = self.alias_manager._validate_document_count_difference('source-index', 'target-index')
        
        # Verify result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Error validating document count difference: division by zero')
        
        # Verify _get_index_count was called with correct arguments
        self.alias_manager._get_index_count.assert_any_call('source-index')
        self.alias_manager._get_index_count.assert_any_call('target-index')

    def test_validate_document_count_difference_default_threshold(self):
        """Test document count validation with default threshold."""
        # Mock _get_index_count to return specific values
        self.alias_manager._get_index_count = MagicMock(side_effect=[100, 105])
        
        # Call _validate_document_count_difference without setting threshold
        result = self.alias_manager._validate_document_count_difference('source-index', 'target-index')
        
        # Verify result
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['message'], 'Document count difference (5.00%) is within threshold (1000000.0%)')
        
        # Verify _get_index_count was called with correct arguments
        self.alias_manager._get_index_count.assert_any_call('source-index')
        self.alias_manager._get_index_count.assert_any_call('target-index')

    def test_validate_document_count_difference_exception(self):
        """Test document count validation when an exception occurs."""
        # Mock _get_index_count to raise an exception
        self.alias_manager._get_index_count = MagicMock(side_effect=Exception("Test exception"))
        
        # Call _validate_document_count_difference
        result = self.alias_manager._validate_document_count_difference('source-index', 'target-index')
        
        # Verify result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Error validating document count difference: Test exception')
        
        # Verify _get_index_count was called with correct arguments
        self.alias_manager._get_index_count.assert_any_call('source-index')

    def test_validate_document_count_difference_custom_threshold(self):
        """Test document count validation with a custom threshold."""
        # Mock _get_index_count to return specific values
        self.alias_manager._get_index_count = MagicMock(side_effect=[100, 105])

        # Set custom threshold to 5%
        with patch.dict('os.environ', {'DOCUMENT_COUNT_THRESHOLD': '5'}):
            result = self.alias_manager._validate_document_count_difference('source-index', 'target-index')

        # Verify result
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['message'], 'Document count difference (5.00%) is within threshold (5.0%)')
        
        # Verify _get_index_count was called with correct arguments
        self.alias_manager._get_index_count.assert_any_call('source-index')
        self.alias_manager._get_index_count.assert_any_call('target-index')

    def test_validate_document_count_difference_negative_difference(self):
        """Test document count validation with negative difference (target has fewer documents)."""
        # Mock _get_index_count to return values with target having fewer documents
        self.alias_manager._get_index_count = MagicMock(side_effect=[100, 90])
        
        # Set threshold to 10%
        with patch.dict('os.environ', {'DOCUMENT_COUNT_THRESHOLD': '10'}):
            result = self.alias_manager._validate_document_count_difference('source-index', 'target-index')
        
        # Verify result
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['message'], 'Document count difference (10.00%) is within threshold (10.0%)')
        
        # Verify _get_index_count was called with correct arguments
        self.alias_manager._get_index_count.assert_any_call('source-index')
        self.alias_manager._get_index_count.assert_any_call('target-index')

    def test_validate_document_count_difference_large_numbers(self):
        """Test document count validation with large document counts."""
        # Mock _get_index_count to return large values
        self.alias_manager._get_index_count = MagicMock(side_effect=[1000000, 1050000])
        
        # Set threshold to 10%
        with patch.dict('os.environ', {'DOCUMENT_COUNT_THRESHOLD': '10'}):
            result = self.alias_manager._validate_document_count_difference('source-index', 'target-index')
        
        # Verify result
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['message'], 'Document count difference (5.00%) is within threshold (10.0%)')
        
        # Verify _get_index_count was called with correct arguments
        self.alias_manager._get_index_count.assert_any_call('source-index')
        self.alias_manager._get_index_count.assert_any_call('target-index')

if __name__ == '__main__':
    unittest.main() 