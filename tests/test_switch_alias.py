"""
Unit tests for the OpenSearchAliasManager class.

This module contains tests for alias management operations, including
alias creation, switching, and verification.
"""

import unittest
from unittest.mock import patch, MagicMock
import json
from switch_alias import OpenSearchAliasManager

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
        """Test alias info retrieval when alias does not exist."""
        # Mock the _make_request method
        self.manager._make_request = MagicMock(return_value={
            'status': 'success',
            'response': MagicMock(
                status_code=200,
                json=lambda: {}
            )
        })
        
        # Get alias info
        result = self.manager._get_alias_info('test-alias')
        
        # Verify the result
        self.assertEqual(result, {})
        
        # Verify that _make_request was called with correct parameters
        self.manager._make_request.assert_called_once_with('GET', '/_alias/test-alias')

if __name__ == '__main__':
    unittest.main() 