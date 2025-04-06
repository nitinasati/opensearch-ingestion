"""
Unit tests for the OpenSearchBaseManager class.

This module contains tests for the base OpenSearch operations, including
authentication, request handling, and index operations.
"""

import unittest
from unittest.mock import patch, MagicMock
import json
import os
from opensearch_base_manager import OpenSearchBaseManager, OpenSearchException

class TestOpenSearchBaseManager(unittest.TestCase):
    """Test cases for the OpenSearchBaseManager class."""
    
    def setUp(self):
        """Set up test environment."""
        # Mock environment variables
        self.env_patcher = patch.dict('os.environ', {
            'OPENSEARCH_ENDPOINT': 'https://test-endpoint',
            'AWS_REGION': 'us-west-2'
        })
        self.env_patcher.start()
        
        # Mock boto3 client
        self.iam_client_mock = MagicMock()
        self.boto3_patcher = patch('boto3.client', return_value=self.iam_client_mock)
        self.boto3_patcher.start()
        
        # Initialize the manager
        self.manager = OpenSearchBaseManager()
    
    def tearDown(self):
        """Clean up after tests."""
        self.env_patcher.stop()
        self.boto3_patcher.stop()
    
    def test_init(self):
        """Test initialization of the OpenSearchBaseManager class."""
        self.assertIsNotNone(self.manager)
        self.assertEqual(self.manager.endpoint, 'https://test-endpoint')
    
    def test_init_custom_endpoint(self):
        """Test initialization with a custom endpoint."""
        manager = OpenSearchBaseManager('https://custom-endpoint')
        self.assertEqual(manager.endpoint, 'https://custom-endpoint')
    
    def test_make_request_success(self):
        """Test successful request making."""
        # Mock the requests.post method
        with patch('requests.post') as mock_post:
            mock_post.return_value = MagicMock(
                status_code=200,
                json=lambda: {'result': 'success'}
            )
            
            # Make request
            result = self.manager._make_request('POST', '/test', {'data': 'test'})
            
            # Verify the result
            self.assertEqual(result['status'], 'success')
            self.assertEqual(result['response'].json(), {'result': 'success'})
            
            # Verify that the request was made correctly
            mock_post.assert_called_once()
            args, kwargs = mock_post.call_args
            self.assertEqual(args[0], 'https://test-endpoint/test')
            self.assertEqual(kwargs['json'], {'data': 'test'})
    
    def test_make_request_error(self):
        """Test request making with an error."""
        # Mock the requests.post method
        with patch('requests.post') as mock_post:
            mock_post.side_effect = Exception('Request failed')
            
            # Make request
            result = self.manager._make_request('POST', '/test', {'data': 'test'})
            
            # Verify the result
            self.assertEqual(result['status'], 'error')
            self.assertEqual(result['message'], 'Request failed')
    
    def test_verify_index_exists_success(self):
        """Test successful index verification."""
        # Mock the _make_request method
        with patch.object(self.manager, '_make_request', return_value={
            'status': 'success',
            'response': MagicMock(status_code=200)
        }):
            # Verify index
            result = self.manager.verify_index_exists('test-index')
            
            # Verify the result
            self.assertTrue(result)
            
            # Verify that _make_request was called
            self.manager._make_request.assert_called_with('HEAD', '/test-index')
    
    def test_verify_index_exists_not_exists(self):
        """Test index verification when index does not exist."""
        # Mock the _make_request method
        with patch.object(self.manager, '_make_request', return_value={
            'status': 'error',
            'message': 'Index does not exist'
        }):
            # Verify index
            result = self.manager.verify_index_exists('test-index')
            
            # Verify the result
            self.assertFalse(result)
    
    def test_get_index_count_success(self):
        """Test successful index count retrieval."""
        # Mock the _make_request method
        with patch.object(self.manager, '_make_request', return_value={
            'status': 'success',
            'response': MagicMock(
                json=lambda: {'count': 100}
            )
        }):
            # Get index count
            result = self.manager.get_index_count('test-index')
            
            # Verify the result
            self.assertEqual(result, 100)
            
            # Verify that _make_request was called
            self.manager._make_request.assert_called_with('GET', '/test-index/_count')
    
    def test_get_index_count_error(self):
        """Test index count retrieval with an error."""
        # Mock the _make_request method
        with patch.object(self.manager, '_make_request', return_value={
            'status': 'error',
            'message': 'Failed to get count'
        }):
            # Get index count
            with self.assertRaises(OpenSearchException):
                self.manager.get_index_count('test-index')
    
    def test_check_index_alias_success(self):
        """Test successful index alias check."""
        # Mock the _make_request method
        with patch.object(self.manager, '_make_request', return_value={
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
        }):
            # Check index alias
            result = self.manager.check_index_alias('test-index', 'test-alias')
            
            # Verify the result
            self.assertTrue(result)
            
            # Verify that _make_request was called
            self.manager._make_request.assert_called_with('GET', '/_alias/test-alias')
    
    def test_check_index_alias_not_exists(self):
        """Test index alias check when alias does not exist."""
        # Mock the _make_request method
        with patch.object(self.manager, '_make_request', return_value={
            'status': 'success',
            'response': MagicMock(
                json=lambda: {
                    'test-index': {
                        'aliases': {}
                    }
                }
            )
        }):
            # Check index alias
            result = self.manager.check_index_alias('test-index', 'test-alias')
            
            # Verify the result
            self.assertFalse(result)
    
    def test_delete_documents_success(self):
        """Test successful document deletion."""
        # Mock the _make_request method
        with patch.object(self.manager, '_make_request', return_value={
            'status': 'success',
            'response': MagicMock(
                json=lambda: {'deleted': 10}
            )
        }):
            # Delete documents
            result = self.manager.delete_documents('test-index', {'query': {'match_all': {}}})
            
            # Verify the result
            self.assertEqual(result['status'], 'success')
            self.assertEqual(result['deleted'], 10)
            
            # Verify that _make_request was called
            self.manager._make_request.assert_called_with('POST', '/test-index/_delete_by_query')
    
    def test_delete_documents_error(self):
        """Test document deletion with an error."""
        # Mock the _make_request method
        with patch.object(self.manager, '_make_request', return_value={
            'status': 'error',
            'message': 'Failed to delete documents'
        }):
            # Delete documents
            result = self.manager.delete_documents('test-index', {'query': {'match_all': {}}})
            
            # Verify the result
            self.assertEqual(result['status'], 'error')
            self.assertEqual(result['message'], 'Failed to delete documents')
    
    def test_bulk_index_success(self):
        """Test successful bulk indexing."""
        # Mock the _make_request method
        with patch.object(self.manager, '_make_request', return_value={
            'status': 'success',
            'response': MagicMock(
                json=lambda: {'errors': False}
            )
        }):
            # Bulk index
            result = self.manager.bulk_index('test-index', [
                {'id': 1, 'name': 'test1'},
                {'id': 2, 'name': 'test2'}
            ])
            
            # Verify the result
            self.assertEqual(result['status'], 'success')
            
            # Verify that _make_request was called
            self.manager._make_request.assert_called_with('POST', '/_bulk')
    
    def test_bulk_index_error(self):
        """Test bulk indexing with an error."""
        # Mock the _make_request method
        with patch.object(self.manager, '_make_request', return_value={
            'status': 'error',
            'message': 'Failed to bulk index'
        }):
            # Bulk index
            result = self.manager.bulk_index('test-index', [
                {'id': 1, 'name': 'test1'},
                {'id': 2, 'name': 'test2'}
            ])
            
            # Verify the result
            self.assertEqual(result['status'], 'error')
            self.assertEqual(result['message'], 'Failed to bulk index')
    
    def test_create_index_success(self):
        """Test successful index creation."""
        # Mock the _make_request method
        with patch.object(self.manager, '_make_request', return_value={
            'status': 'success',
            'response': MagicMock(status_code=200)
        }):
            # Create index
            result = self.manager.create_index('test-index', {'settings': {'number_of_shards': 1}})
            
            # Verify the result
            self.assertEqual(result['status'], 'success')
            
            # Verify that _make_request was called
            self.manager._make_request.assert_called_with('PUT', '/test-index')
    
    def test_create_index_error(self):
        """Test index creation with an error."""
        # Mock the _make_request method
        with patch.object(self.manager, '_make_request', return_value={
            'status': 'error',
            'message': 'Failed to create index'
        }):
            # Create index
            result = self.manager.create_index('test-index', {'settings': {'number_of_shards': 1}})
            
            # Verify the result
            self.assertEqual(result['status'], 'error')
            self.assertEqual(result['message'], 'Failed to create index')
    
    def test_delete_index_success(self):
        """Test successful index deletion."""
        # Mock the _make_request method
        with patch.object(self.manager, '_make_request', return_value={
            'status': 'success',
            'response': MagicMock(status_code=200)
        }):
            # Delete index
            result = self.manager.delete_index('test-index')
            
            # Verify the result
            self.assertEqual(result['status'], 'success')
            
            # Verify that _make_request was called
            self.manager._make_request.assert_called_with('DELETE', '/test-index')
    
    def test_delete_index_error(self):
        """Test index deletion with an error."""
        # Mock the _make_request method
        with patch.object(self.manager, '_make_request', return_value={
            'status': 'error',
            'message': 'Failed to delete index'
        }):
            # Delete index
            result = self.manager.delete_index('test-index')
            
            # Verify the result
            self.assertEqual(result['status'], 'error')
            self.assertEqual(result['message'], 'Failed to delete index')
    
    def test_get_index_settings_success(self):
        """Test successful index settings retrieval."""
        # Mock the _make_request method
        with patch.object(self.manager, '_make_request', return_value={
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
        }):
            # Get index settings
            result = self.manager.get_index_settings('test-index')
            
            # Verify the result
            self.assertEqual(result['status'], 'success')
            self.assertEqual(result['settings']['index']['number_of_shards'], '1')
            
            # Verify that _make_request was called
            self.manager._make_request.assert_called_with('GET', '/test-index/_settings')
    
    def test_get_index_settings_error(self):
        """Test index settings retrieval with an error."""
        # Mock the _make_request method
        with patch.object(self.manager, '_make_request', return_value={
            'status': 'error',
            'message': 'Failed to get settings'
        }):
            # Get index settings
            result = self.manager.get_index_settings('test-index')
            
            # Verify the result
            self.assertEqual(result['status'], 'error')
            self.assertEqual(result['message'], 'Failed to get settings')
    
    def test_update_index_settings_success(self):
        """Test successful index settings update."""
        # Mock the _make_request method
        with patch.object(self.manager, '_make_request', return_value={
            'status': 'success',
            'response': MagicMock(status_code=200)
        }):
            # Update index settings
            result = self.manager.update_index_settings('test-index', {'index': {'number_of_replicas': 2}})
            
            # Verify the result
            self.assertEqual(result['status'], 'success')
            
            # Verify that _make_request was called
            self.manager._make_request.assert_called_with('PUT', '/test-index/_settings')
    
    def test_update_index_settings_error(self):
        """Test index settings update with an error."""
        # Mock the _make_request method
        with patch.object(self.manager, '_make_request', return_value={
            'status': 'error',
            'message': 'Failed to update settings'
        }):
            # Update index settings
            result = self.manager.update_index_settings('test-index', {'index': {'number_of_replicas': 2}})
            
            # Verify the result
            self.assertEqual(result['status'], 'error')
            self.assertEqual(result['message'], 'Failed to update settings')
    
    def test_search_success(self):
        """Test successful search."""
        # Mock the _make_request method
        with patch.object(self.manager, '_make_request', return_value={
            'status': 'success',
            'response': MagicMock(
                json=lambda: {
                    'hits': {
                        'total': {'value': 2},
                        'hits': [
                            {'_source': {'id': 1, 'name': 'test1'}},
                            {'_source': {'id': 2, 'name': 'test2'}}
                        ]
                    }
                }
            )
        }):
            # Search
            result = self.manager.search('test-index', {'query': {'match_all': {}}})
            
            # Verify the result
            self.assertEqual(result['status'], 'success')
            self.assertEqual(result['total'], 2)
            self.assertEqual(len(result['hits']), 2)
            
            # Verify that _make_request was called
            self.manager._make_request.assert_called_with('GET', '/test-index/_search')
    
    def test_search_error(self):
        """Test search with an error."""
        # Mock the _make_request method
        with patch.object(self.manager, '_make_request', return_value={
            'status': 'error',
            'message': 'Failed to search'
        }):
            # Search
            result = self.manager.search('test-index', {'query': {'match_all': {}}})
            
            # Verify the result
            self.assertEqual(result['status'], 'error')
            self.assertEqual(result['message'], 'Failed to search')
    
    def test_count_success(self):
        """Test successful count."""
        # Mock the _make_request method
        with patch.object(self.manager, '_make_request', return_value={
            'status': 'success',
            'response': MagicMock(
                json=lambda: {'count': 100}
            )
        }):
            # Count
            result = self.manager.count('test-index', {'query': {'match_all': {}}})
            
            # Verify the result
            self.assertEqual(result, 100)
            
            # Verify that _make_request was called
            self.manager._make_request.assert_called_with('GET', '/test-index/_count')
    
    def test_count_error(self):
        """Test count with an error."""
        # Mock the _make_request method
        with patch.object(self.manager, '_make_request', return_value={
            'status': 'error',
            'message': 'Failed to count'
        }):
            # Count
            with self.assertRaises(OpenSearchException):
                self.manager.count('test-index', {'query': {'match_all': {}}})
    
    def test_scroll_index_success(self):
        """Test successful index scrolling."""
        # Mock the _make_request method
        with patch.object(self.manager, '_make_request', return_value={
            'status': 'success',
            'response': MagicMock(
                json=lambda: {
                    '_scroll_id': 'test-scroll-id',
                    'hits': {
                        'hits': [
                            {'_source': {'id': 1, 'name': 'test1'}},
                            {'_source': {'id': 2, 'name': 'test2'}}
                        ]
                    }
                }
            )
        }):
            # Scroll index
            result = self.manager.scroll_index('test-index', size=2)
            
            # Verify the result
            self.assertEqual(len(result), 2)
            self.assertEqual(result[0]['id'], 1)
            self.assertEqual(result[1]['id'], 2)
            
            # Verify that _make_request was called
            self.manager._make_request.assert_called_with('GET', '/test-index/_search?scroll=1m')
    
    def test_scroll_index_error(self):
        """Test index scrolling with an error."""
        # Mock the _make_request method
        with patch.object(self.manager, '_make_request', return_value={
            'status': 'error',
            'message': 'Failed to scroll'
        }):
            # Scroll index
            with self.assertRaises(OpenSearchException):
                self.manager.scroll_index('test-index', size=2)

if __name__ == '__main__':
    unittest.main() 