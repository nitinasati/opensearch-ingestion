"""
OpenSearch Base Manager

This module provides base functionality for OpenSearch operations including
authentication, SSL handling, and common operations.
"""

import requests
import logging
import os
from typing import Dict, Any, Optional
from dotenv import load_dotenv
import urllib3
import boto3
from datetime import datetime

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

# Disable SSL verification warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class OpenSearchBaseManager:
    """
    Base class for OpenSearch operations providing common functionality.
    """
    
    # Content type constant
    CONTENT_TYPE_JSON = 'application/json'
    
    def __init__(self, opensearch_endpoint: Optional[str] = None, 
                 username: Optional[str] = None, 
                 password: Optional[str] = None,
                 verify_ssl: bool = False):
        """
        Initialize the base manager with OpenSearch credentials.
        
        Args:
            opensearch_endpoint (str, optional): OpenSearch endpoint URL
            username (str, optional): OpenSearch username
            password (str, optional): OpenSearch password
            verify_ssl (bool): Whether to verify SSL certificates
        """
        self.verify_ssl = verify_ssl
        
        # Try to get credentials from AWS Parameter Store first
        try:
            credentials = self._get_aws_credentials()
            self.opensearch_endpoint = opensearch_endpoint or credentials['endpoint']
            self.username = username or credentials['username']
            self.password = password or credentials['password']
        except Exception as e:
            logger.error(f"Failed to get credentials from AWS Parameter Store: {str(e)}")
        
        # Validate credentials
        if not all([self.opensearch_endpoint, self.username, self.password]):
            raise ValueError("OpenSearch credentials must be provided either through AWS Parameter Store, environment variables, or constructor arguments")
        
        # Create authentication header
        self.auth_header = self._create_auth_header()
        
        # Set up logging
        self._setup_logging()
    
    def _get_aws_credentials(self) -> Dict[str, str]:
        """
        Retrieve OpenSearch credentials from AWS Parameter Store.
        
        Returns:
            Dict[str, str]: Dictionary containing OpenSearch credentials
        """
        try:
            region = os.getenv('AWS_REGION')
            if not region:
                raise ValueError("AWS_REGION environment variable is required for Parameter Store access")
            
            # Get parameter paths from environment variables
            endpoint_path = os.getenv('OPENSEARCH_ENDPOINT', '/opensearch/endpoint')
            username_path = os.getenv('OPENSEARCH_USERNAME', '/opensearch/username')
            password_path = os.getenv('OPENSEARCH_PASSWORD', '/opensearch/password')
            
            ssm_client = boto3.client('ssm', region_name=region)
            
            # Get parameters in a single call
            response = ssm_client.get_parameters(
                Names=[
                    endpoint_path,
                    username_path,
                    password_path
                ],
                WithDecryption=True
            )
            
            # Extract parameters
            parameters = {param['Name'].split('/')[-1]: param['Value'] 
                         for param in response['Parameters']}
            
            # Validate required parameters
            required_params = ['endpoint', 'username', 'password']
            missing_params = [param for param in required_params 
                            if param not in parameters]
            
            if missing_params:
                raise ValueError(f"Missing required parameters: {', '.join(missing_params)}")
            
            logger.info("Successfully retrieved OpenSearch credentials from Parameter Store")
            return parameters
            
        except Exception as e:
            error_msg = f"Error retrieving credentials from Parameter Store: {str(e)}"
            logger.error(error_msg)
            raise
    
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
    
    def _create_auth_header(self) -> str:
        """
        Create Basic Authentication header for OpenSearch requests.
        
        Returns:
            str: Basic Authentication header
        """
        import base64
        auth_string = f"{self.username}:{self.password}"
        auth_bytes = auth_string.encode('ascii')
        base64_bytes = base64.b64encode(auth_bytes)
        base64_auth = base64_bytes.decode('ascii')
        return f"Basic {base64_auth}"
    
    def _verify_index_exists(self, index_name: str) -> bool:
        """
        Verify if an index exists in OpenSearch.
        
        Args:
            index_name (str): Name of the index to verify
            
        Returns:
            bool: True if index exists, False otherwise
        """
        try:
            response = requests.head(
                f"{self.opensearch_endpoint}/{index_name}",
                headers={'Authorization': self.auth_header},
                verify=self.verify_ssl
            )
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Error verifying index existence: {str(e)}")
            return False
    
    def _get_index_count(self, index_name: str) -> int:
        """
        Get the document count for an index.
        
        Args:
            index_name (str): Name of the index to count documents
            
        Returns:
            int: Number of documents in the index, 0 if error occurs
        """
        try:
            response = requests.get(
                f"{self.opensearch_endpoint}/{index_name}/_count",
                headers={
                    'Authorization': self.auth_header,
                    'Accept': self.CONTENT_TYPE_JSON
                },
                verify=self.verify_ssl
            )
            
            if response.status_code == 200:
                return response.json().get('count', 0)
            
            logger.error(f"Failed to get document count. Status code: {response.status_code}")
            return 0
            
        except Exception as e:
            logger.error(f"Error getting document count: {str(e)}")
            return 0
    
    def _check_index_aliases(self, index_name: str) -> Dict[str, Any]:
        """
        Check if an index is part of any aliases.
        
        Args:
            index_name (str): Name of the index to check
            
        Returns:
            Dict[str, Any]: Result containing status and alias information
        """
        try:
            # First check if the index exists
            if not self._verify_index_exists(index_name):
                return {
                    "status": "error",
                    "message": f"Index {index_name} does not exist"
                }
            
            # Get all aliases for the index
            response = requests.get(
                f"{self.opensearch_endpoint}/{index_name}/_alias",
                headers={'Authorization': self.auth_header},
                verify=self.verify_ssl
            )
            
            if response.status_code == 200:
                aliases = response.json()
                logger.info(f"Aliases for index {index_name}: {aliases}")
                
                # Check if the index has any aliases
                if index_name in aliases and aliases[index_name].get('aliases'):
                    alias_names = list(aliases[index_name]['aliases'].keys())
                    return {
                        "status": "error",
                        "message": f"Index {index_name} is part of aliases: {alias_names}"
                    }
                return {"status": "success", "message": "No aliases found"}
            
            error_msg = f"Failed to check aliases. Status code: {response.status_code}"
            logger.error(error_msg)
            return {
                "status": "error",
                "message": error_msg
            }
            
        except Exception as e:
            error_msg = f"Error checking aliases: {str(e)}"
            logger.error(error_msg)
            return {
                "status": "error",
                "message": error_msg
            }
    
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
            response = requests.post(
                f"{self.opensearch_endpoint}/{index_name}/_delete_by_query",
                headers={
                    'Authorization': self.auth_header,
                    'Content-Type': self.CONTENT_TYPE_JSON
                },
                json={
                    "query": {
                        "match_all": {}
                    }
                },
                verify=self.verify_ssl
            )
            
            if response.status_code == 200:
                result = response.json()
                deleted_count = result.get('deleted', 0)
                logger.info(f"Successfully deleted {deleted_count} documents from index {index_name}")
                
                # Force merge to remove deleted documents
                merge_response = requests.post(
                    f"{self.opensearch_endpoint}/{index_name}/_forcemerge",
                    headers={'Authorization': self.auth_header},
                    verify=self.verify_ssl
                )
                
                if merge_response.status_code == 200:
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