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
        self.reindex_manager._verify_index_exists = MagicMock(return_value=True)
        self.reindex_manager._get_index_count = MagicMock(return_value=100)
        self.reindex_manager._make_request = MagicMock(return_value={
            'status': 'success',
            'response': MagicMock(
                json=lambda: {'total': 100}
            )
        })
        
        # Perform reindexing
        result = self.reindex_manager.reindex('source-index', 'target-index')
        
        # Verify the result
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['message'], 'Successfully reindexed 100 documents from source-index to target-index')
        
        # Verify that the necessary methods were called
        self.reindex_manager._verify_index_exists.assert_called_with('source-index')
        self.reindex_manager._get_index_count.assert_called_with('source-index')
        self.reindex_manager._make_request.assert_called_with(
            'POST',
            '/_reindex',
            data={
                "source": {"index": "source-index"},
                "dest": {"index": "target-index"}
            }
        )
    
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
        self.reindex_manager._verify_index_exists = MagicMock(return_value=True)
        self.reindex_manager._get_index_count = MagicMock(return_value=0)
        
        # Perform reindexing
        result = self.reindex_manager.reindex('source-index', 'target-index')
        
        # Verify the result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Source index source-index is empty')
        
        # Verify that _make_request was not called
        self.manager_mock._make_request.assert_not_called()
    
    def test_reindex_index_bulk_error(self):
        """Test reindexing when reindex operation fails."""
        # Mock the necessary methods
        self.reindex_manager._verify_index_exists = MagicMock(return_value=True)
        self.reindex_manager._get_index_count = MagicMock(return_value=100)
        self.reindex_manager._make_request = MagicMock(return_value={
            'status': 'error',
            'message': 'Failed to reindex documents'
        })
        
        # Perform reindexing
        result = self.reindex_manager.reindex('source-index', 'target-index')
        
        # Verify the result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Failed to reindex documents: Failed to reindex documents')
        
        # Verify that the necessary methods were called
        self.reindex_manager._verify_index_exists.assert_called_with('source-index')
        self.reindex_manager._get_index_count.assert_called_with('source-index')
        self.reindex_manager._make_request.assert_called_with(
            'POST',
            '/_reindex',
            data={
                "source": {"index": "source-index"},
                "dest": {"index": "target-index"}
            }
        )
    
    def test_reindex_index_scroll_error(self):
        """Test reindexing when the reindex operation fails."""
        # Mock the necessary methods
        self.reindex_manager._verify_index_exists = MagicMock(return_value=True)
        self.reindex_manager._get_index_count = MagicMock(return_value=100)
        self.reindex_manager.index_manager.validate_and_cleanup_index = MagicMock(return_value={'status': 'success'})
        self.reindex_manager._make_request = MagicMock(return_value={
            'status': 'error',
            'message': 'Failed to make request to OpenSearch after 3 attempts: 404 Client Error: Not Found for url: https://search-mynewdomain-ovgab6nu4xfggw52b77plmruhm.us-east-1.es.amazonaws.com/_reindex'
        })

        # Perform reindexing
        result = self.reindex_manager.reindex('source-index', 'target-index')

        # Verify the result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Failed to reindex documents: Failed to make request to OpenSearch after 3 attempts: 404 Client Error: Not Found for url: https://search-mynewdomain-ovgab6nu4xfggw52b77plmruhm.us-east-1.es.amazonaws.com/_reindex')

if __name__ == '__main__':
    unittest.main() 