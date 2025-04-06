"""
Unit tests for the OpenSearchIndexManager class.

This module contains tests for index cleanup operations, including
index validation, recreation, and error handling.
"""

import unittest
from unittest.mock import patch, MagicMock
import json
from index_cleanup import OpenSearchIndexManager

class TestOpenSearchIndexManager(unittest.TestCase):
    """Test cases for the OpenSearchIndexManager class."""
    
    def setUp(self):
        """Set up test environment."""
        # Create mock for OpenSearchBaseManager
        self.manager_mock = MagicMock()
        
        # Apply patches
        self.manager_patcher = patch('index_cleanup.OpenSearchBaseManager', return_value=self.manager_mock)
        self.manager_patcher.start()
        
        # Create an instance of OpenSearchIndexManager
        self.index_manager = OpenSearchIndexManager()
    
    def tearDown(self):
        """Clean up after tests."""
        self.manager_patcher.stop()
    
    def test_init(self):
        """Test initialization of the OpenSearchIndexManager class."""
        self.assertIsNotNone(self.index_manager)
    
    def test_validate_and_cleanup_index_success(self):
        """Test successful index validation and cleanup."""
        # Mock the necessary methods
        self.index_manager._verify_index_exists = MagicMock(return_value=True)
        self.index_manager._get_index_count = MagicMock(return_value=100)
        self.index_manager._check_index_aliases = MagicMock(return_value={})
        self.index_manager._delete_all_documents = MagicMock(return_value={
            'status': 'success',
            'message': 'Deleted 100 documents',
            'documents_deleted': 100
        })
        
        # Perform validation and cleanup
        result = self.index_manager.validate_and_cleanup_index('test-index')
        
        # Verify the result
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['message'], 'Successfully cleaned up index test-index')
        self.assertEqual(result['documents_deleted'], 100)
        
        # Verify that the necessary methods were called
        self.index_manager._verify_index_exists.assert_called_once_with('test-index')
        self.index_manager._get_index_count.assert_called_once_with('test-index')
        self.index_manager._check_index_aliases.assert_called_once_with('test-index')
        self.index_manager._delete_all_documents.assert_called_once_with('test-index')
    
    def test_validate_and_cleanup_index_not_exists(self):
        """Test index validation when index does not exist."""
        # Mock the necessary methods
        self.index_manager._verify_index_exists = MagicMock(return_value=False)
        self.index_manager._get_index_count = MagicMock()
        self.index_manager._check_index_aliases = MagicMock()
        
        # Perform validation and cleanup
        result = self.index_manager.validate_and_cleanup_index('test-index')
        
        # Verify the result
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['message'], 'Index test-index does not exist')
        
        # Verify that the necessary methods were called
        self.index_manager._verify_index_exists.assert_called_once_with('test-index')
        self.assertEqual(self.index_manager._get_index_count.call_count, 0)
        self.assertEqual(self.index_manager._check_index_aliases.call_count, 0)
    
    def test_validate_and_cleanup_index_with_aliases(self):
        """Test index validation when index has aliases."""
        # Mock the necessary methods
        self.index_manager._verify_index_exists = MagicMock(return_value=True)
        self.index_manager._get_index_count = MagicMock()
        self.index_manager._check_index_aliases = MagicMock(return_value={
            'test-alias': {'index': 'test-index', 'is_write_index': True}
        })
        
        # Perform validation and cleanup
        result = self.index_manager.validate_and_cleanup_index('test-index')
        
        # Verify the result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Index test-index is part of alias(es): test-alias. Cannot remove data from an aliased index.')
        self.assertEqual(result['aliases'], ['test-alias'])
        
        # Verify that the necessary methods were called
        self.index_manager._verify_index_exists.assert_called_once_with('test-index')
        self.index_manager._check_index_aliases.assert_called_once_with('test-index')
        # _get_index_count should not be called when aliases are detected
        self.assertEqual(self.index_manager._get_index_count.call_count, 0)
    
    def test_validate_and_cleanup_index_delete_error(self):
        """Test index validation when document deletion fails."""
        # Mock the necessary methods
        self.index_manager._verify_index_exists = MagicMock(return_value=True)
        self.index_manager._get_index_count = MagicMock(return_value=100)
        self.index_manager._check_index_aliases = MagicMock(return_value={})
        self.index_manager._delete_all_documents = MagicMock(return_value={
            'status': 'error',
            'message': 'Failed to delete documents'
        })
        
        # Perform validation and cleanup
        result = self.index_manager.validate_and_cleanup_index('test-index')
        
        # Verify the result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Failed to delete documents')
        
        # Verify that the necessary methods were called
        self.index_manager._verify_index_exists.assert_called_once_with('test-index')
        self.index_manager._get_index_count.assert_called_once_with('test-index')
        self.index_manager._check_index_aliases.assert_called_once_with('test-index')
        self.index_manager._delete_all_documents.assert_called_once_with('test-index')
    
    def test_recreate_index_success(self):
        """Test successful index recreation."""
        # Mock the _make_request method to return successful responses
        self.index_manager._make_request = MagicMock(side_effect=[
            # First call: GET /test-index/_settings
            {
                'status': 'success',
                'response': MagicMock(
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
            },
            # Second call: GET /test-index/_mappings
            {
                'status': 'success',
                'response': MagicMock(
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
            },
            # Third call: DELETE /test-index
            {
                'status': 'success',
                'message': 'Index deleted successfully'
            },
            # Fourth call: PUT /test-index
            {
                'status': 'success',
                'message': 'Index created successfully'
            }
        ])
        
        # Perform index recreation
        result = self.index_manager._recreate_index('test-index')
        
        # Verify the result
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['message'], 'Successfully recreated index test-index')
        
        # Verify that the _make_request method was called with the correct arguments
        self.assertEqual(self.index_manager._make_request.call_count, 4)
        self.index_manager._make_request.assert_any_call('GET', '/test-index/_settings')
        self.index_manager._make_request.assert_any_call('GET', '/test-index/_mappings')
        self.index_manager._make_request.assert_any_call('DELETE', '/test-index')
        self.index_manager._make_request.assert_any_call('PUT', '/test-index', data=unittest.mock.ANY)
    
    def test_recreate_index_not_exists(self):
        """Test index recreation when index does not exist."""
        # Mock the _make_request method to return error for settings
        self.index_manager._make_request = MagicMock(return_value={
            'status': 'error',
            'message': 'Index test-index does not exist'
        })
        
        # Perform index recreation
        result = self.index_manager._recreate_index('test-index')
        
        # Verify the result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Failed to get index settings: Index test-index does not exist')
        
        # Verify that the _make_request method was called with the correct arguments
        self.index_manager._make_request.assert_called_once_with('GET', '/test-index/_settings')
    
    def test_recreate_index_delete_error(self):
        """Test index recreation when index deletion fails."""
        # Mock the _make_request method to return successful responses for settings and mappings,
        # but error for delete
        self.index_manager._make_request = MagicMock(side_effect=[
            # First call: GET /test-index/_settings
            {
                'status': 'success',
                'response': MagicMock(
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
            },
            # Second call: GET /test-index/_mappings
            {
                'status': 'success',
                'response': MagicMock(
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
            },
            # Third call: DELETE /test-index
            {
                'status': 'error',
                'message': 'Failed to delete index'
            }
        ])
        
        # Perform index recreation
        result = self.index_manager._recreate_index('test-index')
        
        # Verify the result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Failed to drop index: Failed to delete index')
        
        # Verify that the _make_request method was called with the correct arguments
        self.assertEqual(self.index_manager._make_request.call_count, 3)
        self.index_manager._make_request.assert_any_call('GET', '/test-index/_settings')
        self.index_manager._make_request.assert_any_call('GET', '/test-index/_mappings')
        self.index_manager._make_request.assert_any_call('DELETE', '/test-index')
    
    def test_recreate_index_create_error(self):
        """Test index recreation when index creation fails."""
        # Mock the _make_request method to return successful responses for settings, mappings, and delete,
        # but error for create
        self.index_manager._make_request = MagicMock(side_effect=[
            # First call: GET /test-index/_settings
            {
                'status': 'success',
                'response': MagicMock(
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
            },
            # Second call: GET /test-index/_mappings
            {
                'status': 'success',
                'response': MagicMock(
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
            },
            # Third call: DELETE /test-index
            {
                'status': 'success',
                'message': 'Index deleted successfully'
            },
            # Fourth call: PUT /test-index
            {
                'status': 'error',
                'message': 'Failed to create index'
            }
        ])
        
        # Perform index recreation
        result = self.index_manager._recreate_index('test-index')
        
        # Verify the result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Failed to create index: Failed to create index')
        
        # Verify that the _make_request method was called with the correct arguments
        self.assertEqual(self.index_manager._make_request.call_count, 4)
        self.index_manager._make_request.assert_any_call('GET', '/test-index/_settings')
        self.index_manager._make_request.assert_any_call('GET', '/test-index/_mappings')
        self.index_manager._make_request.assert_any_call('DELETE', '/test-index')
        self.index_manager._make_request.assert_any_call('PUT', '/test-index', data=unittest.mock.ANY)

if __name__ == '__main__':
    unittest.main() 