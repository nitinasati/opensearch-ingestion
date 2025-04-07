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
        # Initialize the manager
        self.manager = OpenSearchAliasManager()
        
        # Set up common mock methods
        self.manager._verify_index_exists = MagicMock(return_value=True)
        self.manager._get_index_count = MagicMock(return_value=100)
        self.manager._make_request = MagicMock(return_value={
            'status': 'success',
            'response': MagicMock(status_code=200)
        })
    
    def tearDown(self):
        """Clean up after tests."""
        pass
    
    def test_init(self):
        """Test initialization of the OpenSearchAliasManager class."""
        self.assertIsNotNone(self.manager)
    
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
        
        self.manager._make_request = MagicMock(side_effect=mock_make_request)

        # Switch alias
        result = self.manager.switch_alias('test-alias', 'old-index', 'new-index')

        # Verify the result
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['message'], 'Successfully switched alias test-alias from old-index to new-index')
        self.assertEqual(result['source_count'], 100)
        self.assertEqual(result['target_count'], 100)
        self.assertEqual(result['percentage_diff'], 0)

        # Verify method calls
        self.manager._make_request.assert_called()
    
    def test_switch_alias_index_not_exists(self):
        """Test switching alias when source index does not exist."""
        # Mock _get_alias_info to return valid alias info
        self.manager._get_alias_info = MagicMock(return_value={
            'test-alias': {
                'aliases': {
                    'test-alias': {}
                }
            }
        })
        
        # Mock _verify_index_exists to return False for source index
        self.manager._verify_index_exists = MagicMock(return_value=False)
        
        # Mock _make_request to return success for target index
        self.manager._make_request = MagicMock(return_value={
            'status': 'success',
            'response': MagicMock(status_code=200)
        })
        
        # Mock _get_index_count to return 100 for both indices
        self.manager._get_index_count = MagicMock(return_value=100)
        
        # Call switch_alias
        result = self.manager.switch_alias('test-alias', 'test-index', 'test-index-new')
        
        # Verify result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Source index test-index does not exist')
        
        # Verify method calls
        self.manager._get_alias_info.assert_called_once_with('test-alias')
        self.manager._verify_index_exists.assert_called_once_with('test-index')
        self.manager._make_request.assert_not_called()
    
    def test_switch_alias_request_error(self):
        """Test alias switching when request fails."""
        # Mock _get_alias_info to return valid alias info
        self.manager._get_alias_info = MagicMock(return_value={
            'old-index': {
                'aliases': {
                    'test-alias': {}
                }
            }
        })
        
        # Mock _verify_index_exists to return True for both indices
        self.manager._verify_index_exists = MagicMock(return_value=True)
        
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
        
        self.manager._make_request = MagicMock(side_effect=mock_make_request)
        
        # Mock _get_index_count to return 100 for both indices
        self.manager._get_index_count = MagicMock(return_value=100)
        
        # Call switch_alias
        result = self.manager.switch_alias('test-alias', 'old-index', 'new-index')
        
        # Verify result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Failed to switch alias: Request failed')
        
        # Verify method calls
        self.manager._get_alias_info.assert_called_once_with('test-alias')
        self.manager._verify_index_exists.assert_any_call('old-index')
        self.manager._verify_index_exists.assert_any_call('new-index')
        self.manager._make_request.assert_called()
    
    def test_get_alias_info_success(self):
        """Test successful alias info retrieval."""
        # Mock the _make_request method
        self.manager._make_request = MagicMock(return_value={
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
        result = self.manager._get_alias_info('test-alias')
        
        # Verify the result
        self.assertIsNotNone(result)
        self.assertEqual(result['test-index']['aliases']['test-alias'], {})
        
        # Verify that _make_request was called with correct parameters
        self.manager._make_request.assert_called_once_with('GET', '/_alias/test-alias')
    
    def test_get_alias_info_not_exists(self):
        """Test getting alias info when alias does not exist."""
        # Mock _make_request to return empty aliases
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json = MagicMock(return_value={})
        
        self.manager._make_request = MagicMock(return_value={
            'status': 'success',
            'response': mock_response
        })
        
        result = self.manager._get_alias_info('non-existent-alias')
        self.assertEqual(result, {}, "Should return an empty dictionary when alias does not exist")
    
    def test_switch_alias_same_indices(self):
        """Test switching alias when source and target indices are the same."""
        # Set up test parameters
        alias_name = 'test-alias'
        source_index = 'test-index'
        target_index = 'test-index'  # Same as source_index
        
        # Mock _get_alias_info
        self.manager._get_alias_info = MagicMock()
        
        # Call the switch_alias method
        result = self.manager.switch_alias(alias_name, source_index, target_index)
        
        # Verify the result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], f"Source {source_index} and target index {target_index} are the same hence aborting the operation")
        
        # Verify that _verify_index_exists was not called since the method should return early
        self.manager._verify_index_exists.assert_not_called()
        
        # Verify that _get_alias_info was not called since the method should return early
        self.manager._get_alias_info.assert_not_called()
        
        # Verify that _make_request was not called since the method should return early
        self.manager._make_request.assert_not_called()

    def test_switch_alias_same_indices_case_sensitive(self):
        """Test switching alias when source and target indices are the same but with different case."""
        # Call switch_alias with same source and target but different case
        result = self.manager.switch_alias('test-alias', 'Test-Index', 'test-index')
        
        # Verify the result - should still be treated as different indices
        self.assertNotEqual(result['status'], 'error')
        self.manager._verify_index_exists.assert_called()
        self.manager._get_index_count.assert_called()

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
        self.manager._get_alias_info = MagicMock(return_value={
            "source-index": {
                "aliases": {
                    "test-alias": {}
                }
            }
        })
        self.manager._verify_index_exists = MagicMock(side_effect=lambda index: index == "source-index")
        self.manager._validate_document_count_difference = MagicMock()
        
        # Call switch_alias method
        result = self.manager.switch_alias(
            alias_name="test-alias",
            source_index="source-index",
            target_index="non-existent-index"
        )
        
        # Verify the result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Target index non-existent-index does not exist')
        
        # Verify method calls
        self.manager._get_alias_info.assert_called_once_with('test-alias')
        self.manager._verify_index_exists.assert_any_call('source-index')
        self.manager._verify_index_exists.assert_any_call('non-existent-index')
        self.manager._validate_document_count_difference.assert_not_called()

    def test_create_alias_error_status_code(self):
        """Test creating an alias when the response status code is not 200."""
        # Mock the _make_request method to return a response with status code 400
        mock_response = MagicMock()
        mock_response.status_code = 400
        
        self.manager._make_request = MagicMock(return_value=mock_response)
        
        # Call _create_alias method
        result = self.manager._create_alias(
            alias_name="test-alias",
            index_name="test-index"
        )
        
        # Verify the result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Failed to create alias. Status code: 400')
        
        # Verify method calls
        self.manager._make_request.assert_called_once_with(
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
        self.manager._make_request = MagicMock(side_effect=Exception("Connection error"))
        
        # Call _create_alias method
        result = self.manager._create_alias(
            alias_name="test-alias",
            index_name="test-index"
        )
        
        # Verify the result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Error creating alias: Connection error')
        
        # Verify method calls
        self.manager._make_request.assert_called_once_with(
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
        self.manager._get_alias_info = MagicMock(return_value={
            'old-index': {
                'aliases': {
                    'test-alias': {}
                }
            }
        })
        
        # Mock _verify_index_exists to return True for both indices
        self.manager._verify_index_exists = MagicMock(return_value=True)
        
        # Mock _get_index_count to return 100 for both indices
        self.manager._get_index_count = MagicMock(return_value=100)
        
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
        
        self.manager._make_request = MagicMock(side_effect=mock_make_request)
        
        # Call switch_alias
        result = self.manager.switch_alias('test-alias', 'old-index', 'new-index')
        
        # Verify result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Failed to switch alias. Status code: 500')
        self.assertEqual(result['response'], 'Internal Server Error')
        
        # Verify method calls
        self.manager._get_alias_info.assert_called_once_with('test-alias')
        self.manager._verify_index_exists.assert_any_call('old-index')
        self.manager._verify_index_exists.assert_any_call('new-index')
        self.manager._make_request.assert_called()

    def test_switch_alias_exception_handling(self):
        """Test exception handling in the switch_alias method."""
        # Mock _get_alias_info to return valid alias info
        self.manager._get_alias_info = MagicMock(return_value={
            'old-index': {
                'aliases': {
                    'test-alias': {}
                }
            }
        })
        
        # Mock _verify_index_exists to return True for both indices
        self.manager._verify_index_exists = MagicMock(return_value=True)
        
        # Mock _get_index_count to return 100 for both indices
        self.manager._get_index_count = MagicMock(return_value=100)
        
        # Mock _make_request to raise an exception
        self.manager._make_request = MagicMock(side_effect=Exception("Test exception"))
        
        # Call switch_alias
        result = self.manager.switch_alias('test-alias', 'old-index', 'new-index')
        
        # Verify result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Error during alias switch: Test exception')
        
        # Verify method calls
        self.manager._get_alias_info.assert_called_once_with('test-alias')
        self.manager._verify_index_exists.assert_any_call('old-index')
        self.manager._verify_index_exists.assert_any_call('new-index')
        self.manager._make_request.assert_called()

    def test_switch_alias_response_handling(self):
        """Test handling of different response scenarios in switch_alias method."""
        # Mock _get_alias_info to return valid alias info
        self.manager._get_alias_info = MagicMock(return_value={
            'old-index': {
                'aliases': {
                    'test-alias': {}
                }
            }
        })
        
        # Mock _verify_index_exists to return True for both indices
        self.manager._verify_index_exists = MagicMock(return_value=True)
        
        # Mock _get_index_count to return 100 for both indices
        self.manager._get_index_count = MagicMock(return_value=100)
        
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
        self.manager._make_request = MagicMock(return_value=mock_success_response)
        result = self.manager.switch_alias('test-alias', 'old-index', 'new-index')
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['message'], 'Successfully switched alias test-alias from old-index to new-index')
        
        # Test error status code case
        self.manager._make_request = MagicMock(return_value=mock_error_response)
        result = self.manager.switch_alias('test-alias', 'old-index', 'new-index')
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Failed to switch alias. Status code: 500')
        
        # Test exception case
        self.manager._make_request = mock_exception_response
        result = self.manager.switch_alias('test-alias', 'old-index', 'new-index')
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
        self.manager._make_request.assert_any_call('POST', ALIASES_ENDPOINT, data=expected_data)

if __name__ == '__main__':
    unittest.main() 