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
from datetime import datetime, timezone
from botocore.config import Config
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.exceptions import ClientError

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

# Disable SSL verification warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class OpenSearchBaseManager:
    """
    Base class for OpenSearch operations with support for AWS IAM authentication.
    """
    
    # Content type constant
    CONTENT_TYPE_JSON = 'application/json'
    
    def __init__(self, opensearch_endpoint: Optional[str] = None, 
                 verify_ssl: bool = False):
        """
        Initialize the OpenSearch base manager.
        
        Args:
            opensearch_endpoint (str, optional): The OpenSearch cluster endpoint URL
            verify_ssl (bool): Whether to verify SSL certificates
        """
        self.opensearch_endpoint = opensearch_endpoint or os.getenv('OPENSEARCH_ENDPOINT')
        self.verify_ssl = verify_ssl
        
        if not self.opensearch_endpoint:
            raise ValueError("OpenSearch endpoint is required")
        
        # Remove https:// prefix if present
        self.opensearch_endpoint = self.opensearch_endpoint.replace('https://', '')
        
        # Initialize AWS session and credentials
        self.aws_region = os.getenv('AWS_REGION', 'us-east-1')
        self.role_arn = os.getenv('AWS_ROLE_ARN')
        
        if not self.role_arn:
            raise ValueError("AWS_ROLE_ARN environment variable is required")
        
        logger.info(f"Initializing OpenSearch connection with endpoint: {self.opensearch_endpoint}")
        logger.info(f"Using AWS region: {self.aws_region}")
        logger.info(f"Using role ARN: {self.role_arn}")
        
        # Initialize AWS session with role assumption
        self.session = self._get_aws_session()
        self.credentials = self.session.get_credentials()
        self.config = Config(
            region_name=self.aws_region,
            retries={'max_attempts': 3}
        )
        
        # Set up logging
        self._setup_logging()
    
    def _get_aws_session(self) -> boto3.Session:
        """
        Get AWS session with role assumption.
        
        Returns:
            boto3.Session: AWS session with assumed role credentials
        """
        try:
            # Create STS client
            sts_client = boto3.client('sts')
            
            # Get current identity
            current_identity = sts_client.get_caller_identity()
            logger.info(f"Current AWS identity: {current_identity}")
            
            # Check if we're already running as the target role
            current_role_arn = current_identity['Arn']
            target_role_name = self.role_arn.split('/')[-1]
            
            # If we're already running as the target role or a role with the same name,
            # just return the current session
            if target_role_name in current_role_arn:
                logger.info(f"Already running as role containing '{target_role_name}', skipping role assumption")
                return boto3.Session()
            
            # Assume the role
            assumed_role = sts_client.assume_role(
                RoleArn=self.role_arn,
                RoleSessionName=f'OpenSearchIngestion_{datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")}'
            )
            
            # Log the assumed role details
            logger.info(f"Successfully assumed role: {self.role_arn}")
            logger.info(f"Assumed role session name: {assumed_role['AssumedRoleUser']['AssumedRoleId']}")
            logger.info(f"Assumed role credentials will expire at: {assumed_role['Credentials']['Expiration']}")
            
            # Create a new session with the temporary credentials
            session = boto3.Session(
                aws_access_key_id=assumed_role['Credentials']['AccessKeyId'],
                aws_secret_access_key=assumed_role['Credentials']['SecretAccessKey'],
                aws_session_token=assumed_role['Credentials']['SessionToken'],
                region_name=self.aws_region
            )
            
            # Verify the new session's identity
            new_identity = session.client('sts').get_caller_identity()
            logger.info(f"New AWS identity after role assumption: {new_identity}")
            
            return session
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_msg = e.response['Error']['Message']
            logger.error(f"AWS API error: {error_code} - {error_msg}")
            
            if error_code == 'AccessDenied':
                logger.error("Access denied. Please check:")
                logger.error("1. The role ARN is correct")
                logger.error("2. Your current identity has permission to assume the role")
                logger.error("3. The role trust relationship allows your identity to assume it")
                logger.error("4. The role has the necessary permissions for OpenSearch")
            
            raise ValueError(f"Failed to assume role {self.role_arn}: {error_msg}")
        except Exception as e:
            logger.error(f"Unexpected error during role assumption: {str(e)}")
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
    
    def _get_aws_auth_header(self, method: str, path: str, data: Optional[Dict[str, Any]] = None) -> str:
        """
        Get AWS authentication header for OpenSearch using IAM credentials.
        
        Args:
            method (str): HTTP method
            path (str): API path
            data (dict, optional): Request data
            
        Returns:
            str: AWS Signature V4 Authorization header
        """
        try:
            credentials = self.credentials.get_frozen_credentials()
            if not credentials:
                raise ValueError("AWS credentials not found")
            
            # Log the request details
            url = f"https://{self.opensearch_endpoint}{path}"
            logger.debug(f"Generating AWS auth header for request: {method} {url}")
            
            request = AWSRequest(
                method=method,
                url=url,
                data=data,
                headers={
                    'Host': self.opensearch_endpoint,
                    'X-Amz-Date': datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
                }
            )
            
            # Use 'es' service name for Amazon OpenSearch Service
            auth = SigV4Auth(self.credentials, 'es', self.aws_region)
            auth.add_auth(request)
            
            auth_header = request.headers['Authorization']
            logger.debug(f"Generated AWS auth header: {auth_header[:50]}...")
            
            return auth_header
            
        except Exception as e:
            logger.error(f"Error getting AWS auth header: {str(e)}")
            raise

    def _make_request(self, method: str, path: str, data: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) -> requests.Response:
        """
        Make an HTTP request to OpenSearch with AWS IAM authentication.
        
        Args:
            method (str): HTTP method (GET, POST, etc.)
            path (str): API path
            data (dict, optional): Request data
            headers (dict, optional): Additional headers to include
            
        Returns:
            requests.Response: Response from OpenSearch
        """
        url = f"https://{self.opensearch_endpoint}{path}"
        request_headers = {
            'Content-Type': self.CONTENT_TYPE_JSON,
            'Accept': self.CONTENT_TYPE_JSON
        }
        
        # Update headers with any additional headers provided
        if headers:
            request_headers.update(headers)
        
        # Get AWS auth header
        request_headers['Authorization'] = self._get_aws_auth_header(method, path, data)
        
        try:
            logger.debug(f"Making request to OpenSearch: {method} {url}")
            response = requests.request(
                method=method,
                url=url,
                headers=request_headers,
                json=data if isinstance(data, dict) else None,
                data=data if isinstance(data, str) else None,
                verify=self.verify_ssl
            )
            response.raise_for_status()
            return response
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error making request to OpenSearch: {str(e)}")
            if hasattr(e.response, 'text'):
                logger.error(f"Response text: {e.response.text}")
            if hasattr(e.response, 'headers'):
                logger.error(f"Response headers: {e.response.headers}")
            raise

    def _verify_index_exists(self, index_name: str) -> bool:
        """
        Verify if an index exists in OpenSearch.
        
        Args:
            index_name (str): Name of the index to verify
            
        Returns:
            bool: True if index exists, False otherwise
        """
        try:
            response = self._make_request('HEAD', f'/{index_name}')
            return response.status_code == 200
        except requests.exceptions.RequestException:
            return False

    def _get_index_count(self, index_name: str) -> int:
        """
        Get the document count for an index.
        
        Args:
            index_name (str): Name of the index
            
        Returns:
            int: Number of documents in the index
        """
        try:
            response = self._make_request('GET', f'/{index_name}/_count')
            return response.json()['count']
        except Exception as e:
            logger.error(f"Error getting index count: {str(e)}")
            raise

    def _check_index_aliases(self, index_name: str) -> Dict[str, Any]:
        """
        Check if an index has any aliases.
        
        Args:
            index_name (str): Name of the index to check
            
        Returns:
            dict: Alias information for the index
        """
        try:
            response = self._make_request('GET', f'/_alias/{index_name}')
            return response.json()
        except Exception as e:
            logger.error(f"Error checking index aliases: {str(e)}")
            raise

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
                    'Authorization': self._get_aws_auth_header('POST', f'/{index_name}/_delete_by_query'),
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
                    headers={
                        'Authorization': self._get_aws_auth_header('POST', f'/{index_name}/_forcemerge'),
                        'Content-Type': self.CONTENT_TYPE_JSON
                    },
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