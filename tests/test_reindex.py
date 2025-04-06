"""
Unit tests for the reindex functionality.

This module contains tests for the reindexing operations, including
index creation, document reindexing, and error handling.
"""

import unittest
from unittest.mock import patch, MagicMock
import json
from reindex import OpenSearchReindexManager

class TestReindex(unittest.TestCase):
    """Test cases for the reindex functionality."""
    
    def setUp(self):
        """Set up test environment."""
        # Create mock for OpenSearchBaseManager
        self.manager_mock = MagicMock()
        
        # Apply patches
        self.manager_patcher = patch('reindex.OpenSearchBaseManager', return_value=self.manager_mock)
        self.manager_patcher.start()
        
        # Create an instance of OpenSearchReindexManager
        self.reindex_manager = OpenSearchReindexManager()
    
    def tearDown(self):
        """Clean up after tests."""
        self.manager_patcher.stop()
    
    def test_reindex_index_success(self):
        """Test successful reindexing of an index."""
        # Mock the necessary methods
        self.manager_mock.verify_index_exists.return_value = True
        self.manager_mock.get_index_count.return_value = 100
        self.manager_mock.scroll_index.return_value = [
            {'_source': {'id': 1, 'name': 'test1'}},
            {'_source': {'id': 2, 'name': 'test2'}}
        ]
        self.manager_mock.bulk_index.return_value = {'status': 'success'}
        
        # Perform reindexing
        result = self.reindex_manager.reindex('source-index', 'target-index')
        
        # Verify the result
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['message'], 'Successfully reindexed 100 documents from source-index to target-index')
        
        # Verify that the necessary methods were called
        self.manager_mock.verify_index_exists.assert_called_with('source-index')
        self.manager_mock.get_index_count.assert_called_with('source-index')
        self.manager_mock.scroll_index.assert_called_with('source-index')
        self.manager_mock.bulk_index.assert_called()
    
    def test_reindex_index_source_not_exists(self):
        """Test reindexing when source index does not exist."""
        # Mock the verify_index_exists method
        self.manager_mock.verify_index_exists.return_value = False
        
        # Perform reindexing
        result = self.reindex_manager.reindex('source-index', 'target-index')
        
        # Verify the result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Source index source-index does not exist')
        
        # Verify that get_index_count was not called
        self.manager_mock.get_index_count.assert_not_called()
    
    def test_reindex_index_empty_source(self):
        """Test reindexing when source index is empty."""
        # Mock the necessary methods
        self.manager_mock.verify_index_exists.return_value = True
        self.manager_mock.get_index_count.return_value = 0
        
        # Perform reindexing
        result = self.reindex_manager.reindex('source-index', 'target-index')
        
        # Verify the result
        self.assertEqual(result['status'], 'warning')
        self.assertEqual(result['message'], 'Source index source-index is empty')
        
        # Verify that scroll_index was not called
        self.manager_mock.scroll_index.assert_not_called()
    
    def test_reindex_index_bulk_error(self):
        """Test reindexing when bulk indexing fails."""
        # Mock the necessary methods
        self.manager_mock.verify_index_exists.return_value = True
        self.manager_mock.get_index_count.return_value = 100
        self.manager_mock.scroll_index.return_value = [
            {'_source': {'id': 1, 'name': 'test1'}},
            {'_source': {'id': 2, 'name': 'test2'}}
        ]
        self.manager_mock.bulk_index.return_value = {
            'status': 'error',
            'message': 'Bulk indexing failed'
        }
        
        # Perform reindexing
        result = self.reindex_manager.reindex('source-index', 'target-index')
        
        # Verify the result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Failed to reindex documents: Bulk indexing failed')
    
    def test_reindex_index_scroll_error(self):
        """Test reindexing when scrolling fails."""
        # Mock the necessary methods
        self.manager_mock.verify_index_exists.return_value = True
        self.manager_mock.get_index_count.return_value = 100
        self.manager_mock.scroll_index.side_effect = Exception('Scroll failed')
        
        # Perform reindexing
        result = self.reindex_manager.reindex('source-index', 'target-index')
        
        # Verify the result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Failed to reindex documents: Scroll failed')
        
        # Verify that bulk_index was not called
        self.manager_mock.bulk_index.assert_not_called()

if __name__ == '__main__':
    unittest.main() 