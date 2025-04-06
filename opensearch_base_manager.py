"""
OpenSearch Base Manager

This module provides base functionality for OpenSearch operations including
authentication, SSL handling, and common operations.
"""

import requests
import logging
import os
from typing import Dict, Any, Optional, List
from dotenv import load_dotenv
import urllib3
import boto3
from datetime import datetime, timezone
from requests_aws4auth import AWS4Auth
import json

# Load environment variables
load_dotenv()

# Constants
ALIASES_ENDPOINT = '/_aliases'
INDEX_NOT_EXIST_MESSAGE = 'Index does not exist'

logger = logging.getLogger(__name__)

# Disable SSL verification warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class OpenSearchException(Exception):
    """Custom exception for OpenSearch operations."""
    pass

class OpenSearchBaseManager:
    """
    Base class for OpenSearch operations with support for AWS IAM authentication.
    """
    
    # Content type constant
    CONTENT_TYPE_JSON = 'application/json'
    
    def __init__(self, opensearch_endpoint: Optional[str] = None):
        """
        Initialize the OpenSearch base manager.
        
        Args:
            opensearch_endpoint (str, optional): The OpenSearch cluster endpoint URL
            
        Raises:
            ValueError: If OpenSearch endpoint is not provided
            OpenSearchException: If connection to OpenSearch fails after maximum retries
        """
        self.opensearch_endpoint = opensearch_endpoint or os.getenv('OPENSEARCH_ENDPOINT')
        self.verify_ssl = os.getenv('VERIFY_SSL', 'false').lower() == 'true'
        
        if not self.opensearch_endpoint:
            raise ValueError("OpenSearch endpoint is required")
        
        # Remove https:// prefix if present
        self.opensearch_endpoint = self.opensearch_endpoint.replace('https://', '')
        
        # Initialize AWS session and credentials
        self.aws_region = os.getenv('AWS_REGION', 'us-east-1')
        
        logger.info(f"Initializing OpenSearch connection with endpoint: {self.opensearch_endpoint}")
        logger.info(f"Using AWS region: {self.aws_region}")
        logger.info(f"Using SSL verification: {self.verify_ssl} (from VERIFY_SSL environment variable)")
        
        # Initialize AWS session and auth
        self.session = boto3.Session()
        self.credentials = self.session.get_credentials()
        self.auth = AWS4Auth(
            self.credentials.access_key,
            self.credentials.secret_key,
            self.aws_region,
            'es',
            session_token=self.credentials.token
        )
        
        # Set up logging
        self._setup_logging()
        
        # Test connection with retry logic
        self._test_connection()

    def _test_connection(self) -> Dict[str, Any]:
        """
        Test the connection to OpenSearch with retry logic.
        
        Returns:
            Dict[str, Any]: Response with status and message
            
        Raises:
            OpenSearchException: If connection to OpenSearch fails after maximum retries
        """
        max_retries = 3
        retry_count = 0
        last_exception = None
        
        while retry_count < max_retries:
            try:
                logger.info(f"Testing connection to OpenSearch (Attempt {retry_count + 1}/{max_retries})")
                # Make a simple request to test the connection
                response = requests.get(
                    f"https://{self.opensearch_endpoint}",
                    auth=self.auth,
                    verify=self.verify_ssl,
                    timeout=10
                )
                response.raise_for_status()
                logger.info("Successfully connected to OpenSearch")
                return {
                    'status': 'success',
                    'message': 'Successfully connected to OpenSearch',
                    'response': response.json()
                }
                
            except requests.exceptions.RequestException as e:
                last_exception = e
                retry_count += 1
                self._log_connection_error(e, retry_count, max_retries)
                
                if retry_count < max_retries:
                    # Exponential backoff: 1s, 2s, 4s
                    wait_time = 2 ** (retry_count - 1)
                    logger.info(f"Retrying in {wait_time} seconds...")
                    import time
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to connect to OpenSearch after {max_retries} attempts. Giving up.")
                    raise OpenSearchException(f"Failed to connect to OpenSearch after {max_retries} attempts: {str(last_exception)}")
    
    def _log_connection_error(self, exception, retry_count, max_retries):
        """Log connection error details."""
        logger.error(f"Error connecting to OpenSearch (Attempt {retry_count}/{max_retries}): {str(exception)}")
        
        if hasattr(exception, 'response') and exception.response is not None:
            if hasattr(exception.response, 'text'):
                logger.error(f"Response text: {exception.response.text}")
            if hasattr(exception.response, 'headers'):
                logger.error(f"Response headers: {exception.response.headers}")

    def _make_request(self, method: str, path: str, data: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Make an HTTP request to OpenSearch with AWS IAM authentication.
        
        Args:
            method (str): HTTP method (GET, POST, etc.)
            path (str): API path
            data (dict, optional): Request data
            headers (dict, optional): Additional headers to include
            
        Returns:
            Dict[str, Any]: Response with status and message
            
        Raises:
            requests.exceptions.RequestException: If all retry attempts fail
        """
        url = f"https://{self.opensearch_endpoint}{path}"
        request_headers = self._prepare_headers(headers)
        
        max_retries = 3
        retry_count = 0
        last_exception = None
        
        while retry_count < max_retries:
            try:
                logger.debug(f"Making request to OpenSearch: {method} {url} (Attempt {retry_count + 1}/{max_retries})")
                
                response = self._execute_request(method, url, request_headers, data)
                response.raise_for_status()
                return {
                    'status': 'success',
                    'message': 'Request completed successfully',
                    'response': response
                }
                
            except requests.exceptions.RequestException as e:
                last_exception = e
                retry_count += 1
                self._log_request_error(e, retry_count, max_retries)
                
                if retry_count < max_retries:
                    # Exponential backoff: 1s, 2s, 4s
                    wait_time = 2 ** (retry_count - 1)
                    logger.info(f"Retrying in {wait_time} seconds...")
                    import time
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to make request to OpenSearch after {max_retries} attempts. Giving up.")
                    return {
                        'status': 'error',
                        'message': f"Failed to make request to OpenSearch after {max_retries} attempts: {str(last_exception)}"
                    }
    
    def _prepare_headers(self, headers: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        """Prepare request headers."""
        request_headers = {
            'Content-Type': self.CONTENT_TYPE_JSON,
            'Accept': self.CONTENT_TYPE_JSON
        }
        
        # Update headers with any additional headers provided
        if headers:
            request_headers.update(headers)
            
        return request_headers
    
    def _execute_request(self, method: str, url: str, headers: Dict[str, str], data: Optional[Any] = None) -> requests.Response:
        """Execute the HTTP request."""
        if data is not None:
            if isinstance(data, dict):
                return requests.request(
                    method=method,
                    url=url,
                    headers=headers,
                    json=data,
                    auth=self.auth,
                    verify=self.verify_ssl
                )
            else:
                return requests.request(
                    method=method,
                    url=url,
                    headers=headers,
                    data=data,
                    auth=self.auth,
                    verify=self.verify_ssl
                )
        else:
            return requests.request(
                method=method,
                url=url,
                headers=headers,
                auth=self.auth,
                verify=self.verify_ssl
            )
    
    def _log_request_error(self, exception, retry_count, max_retries):
        """Log request error details."""
        logger.error(f"Error making request to OpenSearch (Attempt {retry_count}/{max_retries}): {str(exception)}")
        
        if hasattr(exception, 'response') and exception.response is not None:
            if hasattr(exception.response, 'text'):
                logger.error(f"Response text: {exception.response.text}")
            if hasattr(exception.response, 'headers'):
                logger.error(f"Response headers: {exception.response.headers}")

    def _setup_logging(self):
        """Set up logging configuration."""
        # Create log directory if it doesn't exist
        log_dir = 'log'
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        # Create timestamp for log file
        timestamp = datetime.now().strftime('%Y%m%d')
        
        # Set up logging to both file and console
        log_file = os.path.join(log_dir, f'opensearch_base_{timestamp}.log')
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        logger.info("Logging initialized")

    def _verify_index_exists(self, index_name: str) -> bool:
        """
        Verify that an index exists.
        
        Args:
            index_name (str): Name of the index
            
        Returns:
            bool: True if the index exists, False otherwise
        """
        try:
            response = self._make_request('HEAD', f'/{index_name}')
            
            if response['status'] == 'error':
                if response['message'] == INDEX_NOT_EXIST_MESSAGE:
                    logger.warning(f"Index {index_name} does not exist")
                    return False
                logger.error(f"Error verifying index exists: {response['message']}")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error verifying index exists: {str(e)}")
            return False

    def _get_index_count(self, index_name: str) -> int:
        """
        Get the document count for an index.
        
        Args:
            index_name (str): Name of the index
            
        Returns:
            int: Document count
        """
        try:
            response = self._make_request('GET', f'/{index_name}/_count')
            
            if response['status'] == 'error':
                if response['message'] == INDEX_NOT_EXIST_MESSAGE:
                    logger.warning(f"Index {index_name} does not exist")
                    return 0
                logger.error(f"Error getting index count: {response['message']}")
                return 0
            
            return response['response'].json().get('count', 0)
            
        except Exception as e:
            logger.error(f"Error getting index count: {str(e)}")
            return 0

    def _check_index_aliases(self, index_name: str) -> Dict[str, Any]:
        """
        Check if an index has any aliases.
        
        Args:
            index_name (str): Name of the index to check
            
        Returns:
            dict: Alias information for the index
        """
        try:
            # First check if the index exists
            if not self._verify_index_exists(index_name):
                return {}
                
            # Get all aliases
            result = self._make_request('GET', '/_cat/aliases?format=json')
            if result['status'] == 'error':
                return {}
            
            response = result['response']
            if response.status_code == 200:
                aliases = response.json()
                # Filter aliases for the given index
                index_aliases = {
                    alias['alias']: alias
                    for alias in aliases
                    if alias.get('index') == index_name
                }
                return index_aliases
            return {}
            
        except Exception as e:
            logger.error(f"Error checking index aliases: {str(e)}")
            return {}

    def _delete_all_documents(self, index_name: str) -> Dict[str, Any]:
        """
        Delete all documents from an index while preserving the index structure.
        
        Args:
            index_name (str): Name of the index to clean
            
        Returns:
            Dict[str, Any]: Result containing status and details
        """
        try:
            # Delete all documents using _delete_by_query
            result = self._make_request(
                'POST',
                f'/{index_name}/_delete_by_query',
                data={
                    "query": {
                        "match_all": {}
                    }
                }
            )
                        
            if result['status'] == 'error':
                return {
                    "status": "error",
                    "message": f"Failed to delete documents: {result['message']}"
                }
                
            response = result['response']
            if response.status_code == 200:
                response_data = response.json()
                deleted_count = response_data.get('deleted', 0)
                logger.info(f"Successfully deleted {deleted_count} documents from index {index_name}")
                
                # Force merge to remove deleted documents
                merge_result = self._make_request(
                    'POST',
                    f'/{index_name}/_forcemerge'
                )
                
                if merge_result['status'] == 'success' and merge_result['response'].status_code == 200:
                    logger.info(f"Successfully force merged index {index_name}")
                else:
                    logger.warning(f"Force merge failed for index {index_name}")
                
                return {
                    "status": "success",
                    "message": f"Deleted {deleted_count} documents",
                    "documents_deleted": deleted_count
                }
            
            return {
                "status": "error",
                "message": f"Failed to delete documents. Status code: {response.status_code}"
            }
            
        except Exception as e:
            return {
                "status": "error",
                "message": f"Error deleting documents: {str(e)}"
            }

    def bulk_index(self, index_name: str, documents: list) -> Dict[str, Any]:
        """
        Bulk index documents into OpenSearch.
        
        Args:
            index_name (str): Name of the index
            documents (list): List of documents to index
            
        Returns:
            Dict[str, Any]: Response from OpenSearch
        """
        try:
            # Create bulk request body
            bulk_request = []
            for doc in documents:
                bulk_request.append(json.dumps({"index": {"_index": index_name}}))
                bulk_request.append(json.dumps(doc))
            bulk_request = '\n'.join(bulk_request) + '\n'
            
            # Send bulk request
            result = self._make_request(
                'POST',
                '/_bulk',
                data=bulk_request,
                headers={'Content-Type': 'application/x-ndjson'}
            )
            
            if result['status'] == 'error':
                return result
                
            return {
                'status': 'success',
                'message': 'Bulk indexing completed successfully',
                'response': result['response'].json()
            }
            
        except Exception as e:
            logger.error(f"Error in bulk index: {str(e)}")
            return {
                'status': 'error',
                'message': f"Error in bulk index: {str(e)}"
            }
    
    def create_index(self, index_name: str, settings: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create an index in OpenSearch.
        
        Args:
            index_name (str): Name of the index
            settings (Dict[str, Any]): Index settings and mappings
            
        Returns:
            Dict[str, Any]: Response from OpenSearch
        """
        try:
            response = self._make_request('PUT', f'/{index_name}', data=settings)
            if response.status_code == 200:
                return {
                    'status': 'success',
                    'message': 'Index created successfully',
                    'response': response.json()
                }
            elif response.status_code == 400:
                error = response.json()
                if error.get('error', {}).get('type') == 'resource_already_exists_exception':
                    return {
                        'status': 'warning',
                        'message': 'Index already exists',
                        'response': error
                    }
                else:
                    return {
                        'status': 'error',
                        'message': f"Failed to create index: {error.get('error', {}).get('reason', 'Unknown error')}",
                        'response': error
                    }
            else:
                return {
                    'status': 'error',
                    'message': f"Failed to create index: {response.text}",
                    'response': response.json()
                }
        except Exception as e:
            logger.error(f"Error creating index: {str(e)}")
            return {
                'status': 'error',
                'message': f"Error creating index: {str(e)}"
            }
    
    def delete_index(self, index_name: str) -> Dict[str, Any]:
        """
        Delete an index from OpenSearch.
        
        Args:
            index_name (str): Name of the index
            
        Returns:
            Dict[str, Any]: Response from OpenSearch
        """
        try:
            response = self._make_request('DELETE', f'/{index_name}')
            if response.status_code == 200:
                return {
                    'status': 'success',
                    'message': 'Index deleted successfully',
                    'response': response.json()
                }
            elif response.status_code == 404:
                return {
                    'status': 'warning',
                    'message': 'Index does not exist',
                    'response': response.json()
                }
            else:
                return {
                    'status': 'error',
                    'message': f"Failed to delete index: {response.text}",
                    'response': response.json()
                }
        except Exception as e:
            logger.error(f"Error deleting index: {str(e)}")
            return {
                'status': 'error',
                'message': f"Error deleting index: {str(e)}"
            }
    
    def get_index_settings(self, index_name: str) -> Dict[str, Any]:
        """
        Get settings for an index.
        
        Args:
            index_name (str): Name of the index
            
        Returns:
            Dict[str, Any]: Index settings
        """
        try:
            response = self._make_request('GET', f'/{index_name}/_settings')
            if response.status_code == 200:
                return {
                    'status': 'success',
                    'message': 'Index settings retrieved successfully',
                    'response': response.json()
                }
            elif response.status_code == 404:
                return {
                    'status': 'error',
                    'message': INDEX_NOT_EXIST_MESSAGE,
                    'response': response.json()
                }
            else:
                return {
                    'status': 'error',
                    'message': f"Failed to get index settings: {response.text}",
                    'response': response.json()
                }
        except Exception as e:
            logger.error(f"Error getting index settings: {str(e)}")
            return {
                'status': 'error',
                'message': f"Error getting index settings: {str(e)}"
            }
    
    def update_index_settings(self, index_name: str, settings: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update settings for an index.
        
        Args:
            index_name (str): Name of the index
            settings (Dict[str, Any]): New settings
            
        Returns:
            Dict[str, Any]: Response from OpenSearch
        """
        try:
            response = self._make_request('PUT', f'/{index_name}/_settings', data=settings)
            if response.status_code == 200:
                return {
                    'status': 'success',
                    'message': 'Index settings updated successfully',
                    'response': response.json()
                }
            elif response.status_code == 404:
                return {
                    'status': 'error',
                    'message': INDEX_NOT_EXIST_MESSAGE,
                    'response': response.json()
                }
            else:
                return {
                    'status': 'error',
                    'message': f"Failed to update index settings: {response.text}",
                    'response': response.json()
                }
        except Exception as e:
            logger.error(f"Error updating index settings: {str(e)}")
            return {
                'status': 'error',
                'message': f"Error updating index settings: {str(e)}"
            }
    
    def search(self, index_name: str, query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Search documents in an index.
        
        Args:
            index_name (str): Name of the index
            query (Dict[str, Any]): Search query
            
        Returns:
            Dict[str, Any]: Search results
        """
        try:
            response = self._make_request('POST', f'/{index_name}/_search', data=query)
            if response.status_code == 200:
                return {
                    'status': 'success',
                    'message': 'Search completed successfully',
                    'response': response.json()
                }
            elif response.status_code == 404:
                return {
                    'status': 'error',
                    'message': INDEX_NOT_EXIST_MESSAGE,
                    'response': response.json()
                }
            else:
                return {
                    'status': 'error',
                    'message': f"Failed to search documents: {response.text}",
                    'response': response.json()
                }
        except Exception as e:
            logger.error(f"Error searching documents: {str(e)}")
            return {
                'status': 'error',
                'message': f"Error searching documents: {str(e)}"
            }
    
    def count(self, index_name: str, query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Count documents in an index.
        
        Args:
            index_name (str): Name of the index
            query (Dict[str, Any]): Count query
            
        Returns:
            Dict[str, Any]: Count results
        """
        try:
            response = self._make_request('POST', f'/{index_name}/_count', data=query)
            if response.status_code == 200:
                return {
                    'status': 'success',
                    'message': 'Count completed successfully',
                    'response': response.json()
                }
            elif response.status_code == 404:
                return {
                    'status': 'error',
                    'message': INDEX_NOT_EXIST_MESSAGE,
                    'response': response.json()
                }
            else:
                return {
                    'status': 'error',
                    'message': f"Failed to count documents: {response.text}",
                    'response': response.json()
                }
        except Exception as e:
            logger.error(f"Error counting documents: {str(e)}")
            return {
                'status': 'error',
                'message': f"Error counting documents: {str(e)}"
            }
    
    def scroll(self, index_name: str, query: Dict[str, Any], batch_size: int = 1000) -> Dict[str, Any]:
        """
        Scroll through documents in an index.
        
        Args:
            index_name (str): Name of the index
            query (Dict[str, Any]): Search query
            batch_size (int): Number of documents per batch
            
        Returns:
            Dict[str, Any]: Scroll results
        """
        try:
            # Initialize scroll
            query['size'] = batch_size
            result = self._make_request(
                'POST',
                f'/{index_name}/_search?scroll=1m',
                data=query
            )
            
            if result['status'] == 'error':
                return result
                
            return {
                'status': 'success',
                'message': 'Scroll initialized successfully',
                'response': result['response']
            }
            
        except Exception as e:
            logger.error(f"Error scrolling documents: {str(e)}")
            return {
                'status': 'error',
                'message': f"Error scrolling documents: {str(e)}"
            }

    def _delete_index(self, index_name: str) -> Dict[str, Any]:
        """
        Delete an index from OpenSearch.
        
        Args:
            index_name (str): Name of the index to delete
            
        Returns:
            dict: Response from OpenSearch with status and message
        """
        try:
            # First check if the index exists
            if not self._verify_index_exists(index_name):
                return {
                    'status': 'warning',
                    'message': f"Index {index_name} does not exist"
                }
            
            # Delete the index
            response = self._make_request('DELETE', f'/{index_name}')
            if response.status_code == 200:
                return {
                    'status': 'success',
                    'message': f"Successfully deleted index {index_name}"
                }
            else:
                return {
                    'status': 'error',
                    'message': f"Failed to delete index {index_name}: {response.text}"
                }
        except requests.exceptions.RequestException as e:
            return {
                'status': 'error',
                'message': f"Error deleting index {index_name}: {str(e)}"
            }

    def _get_index_mappings(self, index_name: str) -> Dict[str, Any]:
        """
        Get the mappings for an index.
        
        Args:
            index_name (str): Name of the index
            
        Returns:
            Dict[str, Any]: Index mappings
        """
        try:
            response = self._make_request('GET', f'/{index_name}/_mapping')
            
            if response['status'] == 'error':
                if response['message'] == INDEX_NOT_EXIST_MESSAGE:
                    logger.warning(f"Index {index_name} does not exist")
                    return {}
                logger.error(f"Error getting index mappings: {response['message']}")
                return {}
            
            return response['response'].json().get(index_name, {}).get('mappings', {})
            
        except Exception as e:
            logger.error(f"Error getting index mappings: {str(e)}")
            return {}
    
    def _get_index_aliases(self, index_name: str) -> List[str]:
        """
        Get the aliases for an index.
        
        Args:
            index_name (str): Name of the index
            
        Returns:
            List[str]: List of alias names
        """
        try:
            response = self._make_request('GET', f'/{index_name}/_alias')
            
            if response['status'] == 'error':
                if response['message'] == INDEX_NOT_EXIST_MESSAGE:
                    logger.warning(f"Index {index_name} does not exist")
                    return []
                logger.error(f"Error getting index aliases: {response['message']}")
                return []
            
            return list(response['response'].json().get(index_name, {}).get('aliases', {}).keys())
            
        except Exception as e:
            logger.error(f"Error getting index aliases: {str(e)}")
            return [] 