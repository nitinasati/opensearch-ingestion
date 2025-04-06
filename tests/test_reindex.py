"""
Unit tests for the OpenSearchReindexManager class and main function.

This module contains tests for reindexing operations, including
the main function that handles command line arguments and orchestrates
the reindexing process.
"""

import unittest
from unittest.mock import patch, MagicMock
import json
from reindex import OpenSearchReindexManager, main
import sys
import logging

class TestOpenSearchReindexManager(unittest.TestCase):
    """Test cases for the OpenSearchReindexManager class."""
    
    def setUp(self):
        """Set up test environment."""
        # Initialize the manager
        self.manager = OpenSearchReindexManager()
        
        # Set up common mock methods
        self.manager._verify_index_exists = MagicMock(return_value=True)
        self.manager._get_index_count = MagicMock(return_value=100)
        self.manager._make_request = MagicMock(return_value={
            'status': 'success',
            'response': MagicMock(
                status_code=200,
                json=lambda: {'total': 100}
            )
        })
        
        # Mock the index manager
        self.manager.index_manager = MagicMock()
        self.manager.index_manager.validate_and_cleanup_index = MagicMock(return_value={
            'status': 'success',
            'message': 'Index validated and cleaned up'
        })
    
    def tearDown(self):
        """Clean up after tests."""
        pass
    
    def test_init(self):
        """Test initialization of the OpenSearchReindexManager class."""
        self.assertIsNotNone(self.manager)
    
    def test_reindex_success(self):
        """Test successful reindexing operation."""
        # Call reindex method
        result = self.manager.reindex('source-index', 'target-index')
        
        # Verify the result
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['message'], 'Successfully reindexed 100 documents from source-index to target-index')
        
        # Verify method calls
        self.manager._verify_index_exists.assert_called_once_with('source-index')
        self.manager._get_index_count.assert_called_once_with('source-index')
        self.manager.index_manager.validate_and_cleanup_index.assert_called_once_with('target-index')
        self.manager._make_request.assert_called_once()
    
    def test_reindex_source_not_exists(self):
        """Test reindexing when source index does not exist."""
        # Mock _verify_index_exists to return False
        self.manager._verify_index_exists = MagicMock(return_value=False)
        
        # Call reindex method
        result = self.manager.reindex('non-existent-index', 'target-index')
        
        # Verify the result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Source index non-existent-index does not exist')
        
        # Verify method calls
        self.manager._verify_index_exists.assert_called_once_with('non-existent-index')
        self.manager._get_index_count.assert_not_called()
        self.manager.index_manager.validate_and_cleanup_index.assert_not_called()
        self.manager._make_request.assert_not_called()
    
    def test_reindex_empty_source(self):
        """Test reindexing when source index is empty."""
        # Mock _get_index_count to return 0
        self.manager._get_index_count = MagicMock(return_value=0)
        
        # Call reindex method
        result = self.manager.reindex('empty-index', 'target-index')
        
        # Verify the result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Source index empty-index is empty')
        
        # Verify method calls
        self.manager._verify_index_exists.assert_called_once_with('empty-index')
        self.manager._get_index_count.assert_called_once_with('empty-index')
        self.manager.index_manager.validate_and_cleanup_index.assert_not_called()
        self.manager._make_request.assert_not_called()
    
    def test_reindex_cleanup_error(self):
        """Test reindexing when target index cleanup fails."""
        # Mock validate_and_cleanup_index to return error
        self.manager.index_manager.validate_and_cleanup_index = MagicMock(return_value={
            'status': 'error',
            'message': 'Failed to clean up target index'
        })
        
        # Call reindex method
        result = self.manager.reindex('source-index', 'target-index')
        
        # Verify the result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Failed to clean up target index')
        
        # Verify method calls
        self.manager._verify_index_exists.assert_called_once_with('source-index')
        self.manager._get_index_count.assert_called_once_with('source-index')
        self.manager.index_manager.validate_and_cleanup_index.assert_called_once_with('target-index')
        self.manager._make_request.assert_not_called()
    
    def test_reindex_request_error(self):
        """Test reindexing when the reindex request fails."""
        # Mock _make_request to return error
        self.manager._make_request = MagicMock(return_value={
            'status': 'error',
            'message': 'Failed to reindex documents'
        })
        
        # Call reindex method
        result = self.manager.reindex('source-index', 'target-index')
        
        # Verify the result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Failed to reindex documents: Failed to reindex documents')
        
        # Verify method calls
        self.manager._verify_index_exists.assert_called_once_with('source-index')
        self.manager._get_index_count.assert_called_once_with('source-index')
        self.manager.index_manager.validate_and_cleanup_index.assert_called_once_with('target-index')
        self.manager._make_request.assert_called_once()

class TestReindexMain(unittest.TestCase):
    """Test cases for the main() function in reindex.py."""
    
    def setUp(self):
        """Set up test environment."""
        # Disable logging during tests
        logging.disable(logging.CRITICAL)
        
        # Save original sys.argv
        self.original_argv = sys.argv
    
    def tearDown(self):
        """Clean up after tests."""
        # Re-enable logging
        logging.disable(logging.NOTSET)
        
        # Restore original sys.argv
        sys.argv = self.original_argv
    
    @patch('reindex.OpenSearchReindexManager')
    def test_main_success(self, mock_reindex_manager_class):
        """Test the main function with successful reindexing."""
        # Set up mock arguments
        sys.argv = ['reindex.py', '--source', 'source_index', '--target', 'target_index']
        
        # Set up mock reindex manager
        mock_reindex_manager = MagicMock()
        mock_reindex_manager_class.return_value = mock_reindex_manager
        
        # Set up mock result
        mock_result = {
            'status': 'success',
            'message': 'Successfully reindexed 100 documents from source_index to target_index'
        }
        mock_reindex_manager.reindex.return_value = mock_result
        
        # Call main function
        result = main()
        
        # Verify result
        self.assertEqual(result, 0)
        
        # Verify reindex manager was initialized
        mock_reindex_manager_class.assert_called_once()
        
        # Verify reindex was called with correct arguments
        mock_reindex_manager.reindex.assert_called_once_with('source_index', 'target_index')
    
    @patch('reindex.OpenSearchReindexManager')
    def test_main_reindex_error(self, mock_reindex_manager_class):
        """Test the main function with reindexing error."""
        # Set up mock arguments
        sys.argv = ['reindex.py', '--source', 'source_index', '--target', 'target_index']
        
        # Set up mock reindex manager
        mock_reindex_manager = MagicMock()
        mock_reindex_manager_class.return_value = mock_reindex_manager
        
        # Set up mock result with error
        mock_result = {
            'status': 'error',
            'message': 'Failed to reindex: Source index does not exist'
        }
        mock_reindex_manager.reindex.return_value = mock_result
        
        # Call main function
        result = main()
        
        # Verify result
        self.assertEqual(result, 0)  # Main returns 0 even for error status
        
        # Verify reindex manager was initialized
        mock_reindex_manager_class.assert_called_once()
        
        # Verify reindex was called with correct arguments
        mock_reindex_manager.reindex.assert_called_once_with('source_index', 'target_index')
    
    @patch('reindex.OpenSearchReindexManager')
    def test_main_configuration_error(self, mock_reindex_manager_class):
        """Test the main function with configuration error."""
        # Set up mock arguments
        sys.argv = ['reindex.py', '--source', 'source_index', '--target', 'target_index']
        
        # Set up mock reindex manager to raise ValueError
        mock_reindex_manager_class.side_effect = ValueError("Missing OpenSearch endpoint")
        
        # Call main function
        result = main()
        
        # Verify result
        self.assertEqual(result, 1)  # Main returns 1 for configuration errors
    
    @patch('reindex.OpenSearchReindexManager')
    def test_main_unexpected_error(self, mock_reindex_manager_class):
        """Test the main function with unexpected error."""
        # Set up mock arguments
        sys.argv = ['reindex.py', '--source', 'source_index', '--target', 'target_index']
        
        # Set up mock reindex manager to raise an unexpected exception
        mock_reindex_manager_class.side_effect = Exception("Unexpected error")
        
        # Call main function
        result = main()
        
        # Verify result
        self.assertEqual(result, 1)  # Main returns 1 for unexpected errors

if __name__ == '__main__':
    unittest.main() 