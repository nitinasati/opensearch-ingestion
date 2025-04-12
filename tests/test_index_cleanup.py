"""
Unit tests for the OpenSearchIndexManager class.

This module contains tests for index cleanup operations, including
index validation, recreation, and error handling.
"""

import unittest
from unittest.mock import patch, MagicMock
import json
import logging
from index_cleanup import OpenSearchIndexManager, main

class TestOpenSearchIndexManager(unittest.TestCase):
    """Test cases for the OpenSearchIndexManager class."""
    
    def setUp(self):
        """Set up test environment."""
        # Create mock for OpenSearch connection
        self.opensearch_mock = MagicMock()
        self.opensearch_mock.info.return_value = {'version': {'number': '7.10.2'}}
        self.opensearch_mock.indices.exists.return_value = True
        self.opensearch_mock.indices.get.return_value = {'test-index': {'mappings': {}}}
        self.opensearch_mock.indices.stats.return_value = {'indices': {'test-index': {'total': {'docs': {'count': 0}}}}}
        self.opensearch_mock.indices.delete.return_value = {'acknowledged': True}
        self.opensearch_mock.indices.create.return_value = {'acknowledged': True}
        self.opensearch_mock.indices.put_mapping.return_value = {'acknowledged': True}
        self.opensearch_mock.indices.put_settings.return_value = {'acknowledged': True}
        
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
        
        # Initialize the index manager
        self.index_manager = OpenSearchIndexManager()
        self.index_manager.opensearch_manager = self.manager_mock
    
    def tearDown(self):
        """Clean up after tests."""
        self.opensearch_patcher.stop()
        self.requests_patcher.stop()
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
            'message': 'Successfully cleaned up index test-index',
            'documents_deleted': 100
        })
        
        # Perform validation and cleanup
        result = self.index_manager.validate_and_cleanup_index('test-index')
        
        # Verify the result
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['message'], 'Successfully cleaned up index test-index')
        self.assertEqual(result['documents_deleted'], 100)
        
        # Verify method calls
        self.index_manager._verify_index_exists.assert_called_once_with('test-index')
        self.index_manager._check_index_aliases.assert_called_once_with('test-index')
        self.index_manager._get_index_count.assert_called_once_with('test-index')
        self.index_manager._delete_all_documents.assert_called_once_with('test-index')
    
    def test_validate_and_cleanup_index_not_exists(self):
        """Test validation and cleanup when index does not exist."""
        # Mock the necessary methods
        self.index_manager._verify_index_exists = MagicMock(return_value=False)
        
        # Perform validation and cleanup
        result = self.index_manager.validate_and_cleanup_index('non-existent-index')
        
        # Verify the result
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['message'], 'Index non-existent-index does not exist')
        
        # Verify method calls
        self.index_manager._verify_index_exists.assert_called_once_with('non-existent-index')
    
    def test_validate_and_cleanup_index_with_aliases(self):
        """Test validation and cleanup when index has aliases."""
        # Mock the necessary methods
        self.index_manager._verify_index_exists = MagicMock(return_value=True)
        self.index_manager._check_index_aliases = MagicMock(return_value={'test-alias': {}})
        
        # Perform validation and cleanup
        result = self.index_manager.validate_and_cleanup_index('test-index')
        
        # Verify the result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Index test-index is part of alias(es): test-alias. Cannot remove data from an aliased index.')
        self.assertEqual(result['aliases'], ['test-alias'])
        
        # Verify method calls
        self.index_manager._verify_index_exists.assert_called_once_with('test-index')
        self.index_manager._check_index_aliases.assert_called_once_with('test-index')
    
    def test_validate_and_cleanup_index_delete_error(self):
        """Test validation and cleanup when document deletion fails."""
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
        
        # Verify method calls
        self.index_manager._verify_index_exists.assert_called_once_with('test-index')
        self.index_manager._check_index_aliases.assert_called_once_with('test-index')
        self.index_manager._get_index_count.assert_called_once_with('test-index')
        self.index_manager._delete_all_documents.assert_called_once_with('test-index')
    
    def test_validate_and_cleanup_index_exception(self):
        """Test exception handling in the validate_and_cleanup_index method."""
        # Mock the necessary methods to raise an exception
        self.index_manager._verify_index_exists = MagicMock(side_effect=Exception("Test exception"))
        
        # Perform validation and cleanup
        result = self.index_manager.validate_and_cleanup_index('test-index')
        
        # Verify the result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Error during index validation and cleanup: Test exception')
        
        # Verify method calls
        self.index_manager._verify_index_exists.assert_called_once_with('test-index')
    
    def test_recreate_index_success(self):
        """Test successful index recreation."""
        # Mock the necessary methods
        self.index_manager._verify_index_exists = MagicMock(return_value=True)
        self.index_manager._get_index_settings = MagicMock(return_value={
            'status': 'success',
            'response': {'test-index': {'settings': {'index': {'number_of_shards': '1'}}}}
        })
        self.index_manager._get_index_mappings = MagicMock(return_value={
            'properties': {'field1': {'type': 'keyword'}}
        })
        
        # Mock _make_request with simpler responses
        self.index_manager._make_request = MagicMock(side_effect=[
            {'status': 'success', 'response': MagicMock(status_code=200, json=lambda: {'test-index': {'settings': {'index': {'number_of_shards': '1'}}}})},
            {'status': 'success', 'response': MagicMock(status_code=200, json=lambda: {'test-index': {'mappings': {'properties': {'field1': {'type': 'keyword'}}}}})},
            {'status': 'success', 'message': 'Index deleted successfully'},
            {'status': 'success', 'message': 'Index created successfully'}
        ])
        
        # Perform index recreation
        result = self.index_manager._recreate_index('test-index')
        
        # Verify the result
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['message'], 'Successfully recreated index test-index')
        
        # Verify method calls
        self.assertEqual(self.index_manager._make_request.call_count, 4)
    
    def test_recreate_index_not_exists(self):
        """Test index recreation when index does not exist."""
        # Mock the necessary methods
        self.index_manager._verify_index_exists = MagicMock(return_value=True)
        self.index_manager._make_request = MagicMock(return_value={
            'status': 'error',
            'message': 'Failed to make request to OpenSearch: 404 Client Error: Not Found for url: https://search-mynewdomain-ovgab6nu4xfggw52b77plmruhm.us-east-1.es.amazonaws.com/non-existent-index/_settings'
        })
        
        # Perform index recreation
        result = self.index_manager._recreate_index('non-existent-index')
        
        # Verify the result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Failed to get index settings: Failed to make request to OpenSearch: 404 Client Error: Not Found for url: https://search-mynewdomain-ovgab6nu4xfggw52b77plmruhm.us-east-1.es.amazonaws.com/non-existent-index/_settings')
        
        # Verify method calls
        self.index_manager._make_request.assert_called_once_with('GET', '/non-existent-index/_settings')
    
    def test_recreate_index_delete_error(self):
        """Test index recreation when index deletion fails."""
        # Mock the necessary methods
        self.index_manager._verify_index_exists = MagicMock(return_value=True)
        self.index_manager._get_index_settings = MagicMock(return_value={
            'status': 'success',
            'response': {'test-index': {'settings': {'index': {'number_of_shards': '1'}}}}
        })
        self.index_manager._get_index_mappings = MagicMock(return_value={
            'properties': {'field1': {'type': 'keyword'}}
        })
        
        # Mock _make_request with simpler responses
        self.index_manager._make_request = MagicMock(side_effect=[
            {'status': 'success', 'response': MagicMock(status_code=200, json=lambda: {'test-index': {'settings': {'index': {'number_of_shards': '1'}}}})},
            {'status': 'success', 'response': MagicMock(status_code=200, json=lambda: {'test-index': {'mappings': {'properties': {'field1': {'type': 'keyword'}}}}})},
            {'status': 'error', 'message': 'Failed to delete index'}
        ])
        
        # Perform index recreation
        result = self.index_manager._recreate_index('test-index')
        
        # Verify the result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Failed to drop index: Failed to delete index')
        
        # Verify method calls
        self.assertEqual(self.index_manager._make_request.call_count, 3)
    
    def test_recreate_index_create_error(self):
        """Test index recreation when index creation fails."""
        # Mock the necessary methods
        self.index_manager._verify_index_exists = MagicMock(return_value=True)
        self.index_manager._get_index_settings = MagicMock(return_value={
            'status': 'success',
            'response': {'test-index': {'settings': {'index': {'number_of_shards': '1'}}}}
        })
        self.index_manager._get_index_mappings = MagicMock(return_value={
            'properties': {'field1': {'type': 'keyword'}}
        })
        
        # Mock _make_request with simpler responses
        self.index_manager._make_request = MagicMock(side_effect=[
            {'status': 'success', 'response': MagicMock(status_code=200, json=lambda: {'test-index': {'settings': {'index': {'number_of_shards': '1'}}}})},
            {'status': 'success', 'response': MagicMock(status_code=200, json=lambda: {'test-index': {'mappings': {'properties': {'field1': {'type': 'keyword'}}}}})},
            {'status': 'success', 'message': 'Index deleted successfully'},
            {'status': 'error', 'message': 'Failed to create index'}
        ])
        
        # Perform index recreation
        result = self.index_manager._recreate_index('test-index')
        
        # Verify the result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Failed to create index: Failed to create index')
        
        # Verify method calls
        self.assertEqual(self.index_manager._make_request.call_count, 4)

class TestIndexCleanupMain(unittest.TestCase):
    """Test cases for the main() function in index_cleanup.py."""
    
    def setUp(self):
        """Set up test environment."""
        # Disable logging during tests
        logging.disable(logging.CRITICAL)
    
    def tearDown(self):
        """Clean up after tests."""
        # Re-enable logging
        logging.disable(logging.NOTSET)
    
    @patch('argparse.ArgumentParser.parse_args')
    @patch('index_cleanup.OpenSearchIndexManager')
    def test_main_success(self, mock_index_manager_class, mock_parse_args):
        """Test the main function with successful index cleanup."""
        mock_args = MagicMock(index='test-index')
        mock_parse_args.return_value = mock_args
        
        mock_index_manager = MagicMock()
        mock_index_manager_class.return_value = mock_index_manager
        mock_index_manager.validate_and_cleanup_index.return_value = {
            'status': 'success',
            'message': 'Successfully cleaned up index test-index'
        }
        
        self.assertEqual(main(), 0)
        mock_index_manager.validate_and_cleanup_index.assert_called_once_with('test-index')
    
    @patch('argparse.ArgumentParser.parse_args')
    @patch('index_cleanup.OpenSearchIndexManager')
    def test_main_error(self, mock_index_manager_class, mock_parse_args):
        """Test the main function with error in index cleanup."""
        mock_args = MagicMock(index='test-index')
        mock_parse_args.return_value = mock_args
        
        mock_index_manager = MagicMock()
        mock_index_manager_class.return_value = mock_index_manager
        mock_index_manager.validate_and_cleanup_index.return_value = {
            'status': 'error',
            'message': 'Failed to clean up index: Index is part of an alias'
        }
        
        self.assertEqual(main(), 0)
        mock_index_manager.validate_and_cleanup_index.assert_called_once_with('test-index')
    
    @patch('argparse.ArgumentParser.parse_args')
    def test_main_exception(self, mock_parse_args):
        """Test the main function with exception."""
        mock_args = MagicMock(index='test-index')
        mock_parse_args.return_value = mock_args
        
        with patch('index_cleanup.OpenSearchIndexManager', side_effect=ValueError("Configuration error")):
            self.assertEqual(main(), 1)

if __name__ == '__main__':
    unittest.main() 