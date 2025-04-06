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
        # Create mock for OpenSearchBaseManager
        self.base_manager_mock = MagicMock()
        
        # Apply patches
        self.base_manager_patcher = patch('switch_alias.OpenSearchBaseManager', return_value=self.base_manager_mock)
        self.base_manager_patcher.start()
        
        # Initialize the manager
        self.manager = OpenSearchAliasManager()
    
    def tearDown(self):
        """Clean up after tests."""
        self.base_manager_patcher.stop()
    
    def test_init(self):
        """Test initialization of the OpenSearchAliasManager class."""
        self.assertIsNotNone(self.manager)
        self.base_manager_mock.assert_called_once()
    
    def test_create_alias_success(self):
        """Test successful alias creation."""
        # Mock the necessary methods
        self.base_manager_mock.verify_index_exists.return_value = True
        self.base_manager_mock._make_request.return_value = {
            'status': 'success',
            'response': MagicMock(status_code=200)
        }
        
        # Create alias
        result = self.manager.create_alias('test-index', 'test-alias')
        
        # Verify the result
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['message'], 'Successfully created alias test-alias for index test-index')
        
        # Verify that the necessary methods were called
        self.base_manager_mock.verify_index_exists.assert_called_with('test-index')
        self.base_manager_mock._make_request.assert_called()
    
    def test_create_alias_index_not_exists(self):
        """Test alias creation when index does not exist."""
        # Mock the verify_index_exists method
        self.base_manager_mock.verify_index_exists.return_value = False
        
        # Create alias
        result = self.manager.create_alias('test-index', 'test-alias')
        
        # Verify the result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Index test-index does not exist')
        
        # Verify that _make_request was not called
        self.base_manager_mock._make_request.assert_not_called()
    
    def test_create_alias_request_error(self):
        """Test alias creation when request fails."""
        # Mock the necessary methods
        self.base_manager_mock.verify_index_exists.return_value = True
        self.base_manager_mock._make_request.return_value = {
            'status': 'error',
            'message': 'Request failed'
        }
        
        # Create alias
        result = self.manager.create_alias('test-index', 'test-alias')
        
        # Verify the result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Failed to create alias: Request failed')
    
    def test_switch_alias_success(self):
        """Test successful alias switching."""
        # Mock the necessary methods
        self.base_manager_mock.verify_index_exists.return_value = True
        self.base_manager_mock._make_request.return_value = {
            'status': 'success',
            'response': MagicMock(status_code=200)
        }
        
        # Switch alias
        result = self.manager.switch_alias('test-alias', 'old-index', 'new-index')
        
        # Verify the result
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['message'], 'Successfully switched alias test-alias from old-index to new-index')
        
        # Verify that the necessary methods were called
        self.base_manager_mock.verify_index_exists.assert_any_call('old-index')
        self.base_manager_mock.verify_index_exists.assert_any_call('new-index')
        self.base_manager_mock._make_request.assert_called()
    
    def test_switch_alias_index_not_exists(self):
        """Test alias switching when index does not exist."""
        # Mock the verify_index_exists method
        self.base_manager_mock.verify_index_exists.return_value = False
        
        # Switch alias
        result = self.manager.switch_alias('test-alias', 'old-index', 'new-index')
        
        # Verify the result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Index old-index or new-index does not exist')
        
        # Verify that _make_request was not called
        self.base_manager_mock._make_request.assert_not_called()
    
    def test_switch_alias_request_error(self):
        """Test alias switching when request fails."""
        # Mock the necessary methods
        self.base_manager_mock.verify_index_exists.return_value = True
        self.base_manager_mock._make_request.return_value = {
            'status': 'error',
            'message': 'Request failed'
        }
        
        # Switch alias
        result = self.manager.switch_alias('test-alias', 'old-index', 'new-index')
        
        # Verify the result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Failed to switch alias: Request failed')
    
    def test_delete_alias_success(self):
        """Test successful alias deletion."""
        # Mock the necessary methods
        self.base_manager_mock._make_request.return_value = {
            'status': 'success',
            'response': MagicMock(status_code=200)
        }
        
        # Delete alias
        result = self.manager.delete_alias('test-index', 'test-alias')
        
        # Verify the result
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['message'], 'Successfully deleted alias test-alias from index test-index')
        
        # Verify that _make_request was called
        self.base_manager_mock._make_request.assert_called()
    
    def test_delete_alias_request_error(self):
        """Test alias deletion when request fails."""
        # Mock the _make_request method
        self.base_manager_mock._make_request.return_value = {
            'status': 'error',
            'message': 'Request failed'
        }
        
        # Delete alias
        result = self.manager.delete_alias('test-index', 'test-alias')
        
        # Verify the result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Failed to delete alias: Request failed')
    
    def test_get_alias_success(self):
        """Test successful alias retrieval."""
        # Mock the _make_request method
        self.base_manager_mock._make_request.return_value = {
            'status': 'success',
            'response': MagicMock(
                json=lambda: {
                    'test-index': {
                        'aliases': {
                            'test-alias': {}
                        }
                    }
                }
            )
        }
        
        # Get alias
        result = self.manager.get_alias('test-index', 'test-alias')
        
        # Verify the result
        self.assertEqual(result['status'], 'success')
        self.assertTrue(result['exists'])
        
        # Verify that _make_request was called
        self.base_manager_mock._make_request.assert_called()
    
    def test_get_alias_not_exists(self):
        """Test alias retrieval when alias does not exist."""
        # Mock the _make_request method
        self.base_manager_mock._make_request.return_value = {
            'status': 'success',
            'response': MagicMock(
                json=lambda: {
                    'test-index': {
                        'aliases': {}
                    }
                }
            )
        }
        
        # Get alias
        result = self.manager.get_alias('test-index', 'test-alias')
        
        # Verify the result
        self.assertEqual(result['status'], 'success')
        self.assertFalse(result['exists'])
    
    def test_get_alias_request_error(self):
        """Test alias retrieval when request fails."""
        # Mock the _make_request method
        self.base_manager_mock._make_request.return_value = {
            'status': 'error',
            'message': 'Request failed'
        }
        
        # Get alias
        result = self.manager.get_alias('test-index', 'test-alias')
        
        # Verify the result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Failed to get alias: Request failed')

if __name__ == '__main__':
    unittest.main() 