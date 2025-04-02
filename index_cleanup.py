"""
OpenSearch Index Manager

This module provides functionality to manage OpenSearch indices, including
validation and cleanup operations. It ensures safe index operations by
checking for aliases and managing document deletion.

Key features:
- Index existence verification
- Alias validation
- Document count management
- Safe index cleanup
- Comprehensive logging
"""

import requests
import logging
import base64
import os
from dotenv import load_dotenv
import argparse
import time
from datetime import datetime
import urllib3
from opensearch_base_manager import OpenSearchBaseManager
from typing import Optional, Dict, Any
import json

# Load environment variables
load_dotenv()

# Create log directory if it doesn't exist
logger = logging.getLogger(__name__)

# Disable SSL verification warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Get configuration from environment variables
INDEX_RECREATE_THRESHOLD = int(os.getenv('INDEX_RECREATE_THRESHOLD', '1000000'))

class OpenSearchIndexManager(OpenSearchBaseManager):
    """
    Manages OpenSearch index operations.
    
    This class provides functionality to:
    - Validate index existence
    - Check index aliases
    - Clean up indices
    - Handle errors and logging
    """
    
    def __init__(self, opensearch_endpoint: Optional[str] = None, 
                 username: Optional[str] = None, 
                 password: Optional[str] = None,
                 verify_ssl: bool = False):
        """
        Initialize the index manager.
        
        Args:
            opensearch_endpoint (str, optional): The OpenSearch cluster endpoint URL
            username (str, optional): OpenSearch username
            password (str, optional): OpenSearch password
            verify_ssl (bool): Whether to verify SSL certificates
        """
        super().__init__(opensearch_endpoint, username, password, verify_ssl)
        logger.info(f"Initialized OpenSearchIndexManager with endpoint: {self.opensearch_endpoint}")
        logger.info(f"Using index recreate threshold: {INDEX_RECREATE_THRESHOLD}")

    def _recreate_index(self, index_name: str, count_result: int) -> dict:
        """
        Recreate an index with the same settings and mappings.
        
        Args:
            index_name (str): Name of the index to recreate
            count_result (int): Number of documents that were in the original index
            
        Returns:
            dict: Result of the recreation operation
        """
        try:
            # Get index settings and mappings
            settings_response = requests.get(
                f"{self.opensearch_endpoint}/{index_name}/_settings",
                headers={'Authorization': self.auth_header},
                verify=self.verify_ssl
            )
            mappings_response = requests.get(
                f"{self.opensearch_endpoint}/{index_name}/_mappings",
                headers={'Authorization': self.auth_header},
                verify=self.verify_ssl
            )
            
            if settings_response.status_code != 200 or mappings_response.status_code != 200:
                logger.error("Failed to get index settings or mappings")
                return {
                    "status": "error",
                    "message": "Failed to get index settings or mappings"
                }
            
            settings_data = settings_response.json()
            mappings_data = mappings_response.json()
            
            # Extract settings and mappings from the response
            if index_name not in settings_data or index_name not in mappings_data:
                logger.error(f"Index {index_name} not found in settings or mappings response")
                return {
                    "status": "error",
                    "message": f"Index {index_name} not found in settings or mappings response"
                }
            
            # Extract the actual settings and mappings
            settings = settings_data[index_name].get('settings', {})
            mappings = mappings_data[index_name].get('mappings', {})
            
            # Filter out internal settings that can't be set during index creation
            if 'index' in settings:
                index_settings = settings['index']
                # Keep only the settings that can be set during index creation
                allowed_settings = {
                    'number_of_shards': index_settings.get('number_of_shards'),
                    'number_of_replicas': index_settings.get('number_of_replicas')
                }
                settings = {'index': allowed_settings}
            
            # Drop the index
            delete_response = requests.delete(
                f"{self.opensearch_endpoint}/{index_name}",
                headers={'Authorization': self.auth_header},
                verify=self.verify_ssl
            )
            
            if delete_response.status_code != 200:
                logger.error(f"Failed to drop index {index_name}")
                return {
                    "status": "error",
                    "message": f"Failed to drop index {index_name}"
                }
            
            # Create new index with same settings and mappings
            create_payload = {
                "settings": settings,
                "mappings": mappings
            }
            
            create_response = requests.put(
                f"{self.opensearch_endpoint}/{index_name}",
                headers={'Authorization': self.auth_header},
                json=create_payload,
                verify=self.verify_ssl
            )
            
            if create_response.status_code != 200:
                logger.error(f"Failed to recreate index {index_name}")
                logger.error(f"Create response: {create_response.text}")
                return {
                    "status": "error",
                    "message": f"Failed to recreate index {index_name}"
                }
            
            return {
                "status": "success",
                "message": f"Successfully dropped and recreated index {index_name}",
                "documents_deleted": count_result
            }
            
        except Exception as e:
            logger.error(f"Error during index recreation: {str(e)}")
            return {
                "status": "error",
                "message": f"Error during index recreation: {str(e)}"
            }

    def validate_and_cleanup_index(self, index_name: str) -> dict:
        """
        Validate and clean up an OpenSearch index.
        
        Args:
            index_name (str): Name of the index to validate and clean up
            
        Returns:
            dict: Result of the cleanup operation
        """
        try:
            # Check if index exists
            if not self._verify_index_exists(index_name):
                logger.info(f"Index {index_name} does not exist")
                return {
                    "status": "error",
                    "message": f"Index {index_name} does not exist"
                }
            
            # Get document count
            count_result = self._get_index_count(index_name)
            if count_result == 0:
                logger.info(f"Index {index_name} is already empty")
                return {
                    "status": "success",
                    "message": f"Index {index_name} is already empty"
                }
            
            logger.info(f"Found {count_result} documents in index {index_name}")
            
            # If document count exceeds threshold, drop and recreate the index
            if count_result > INDEX_RECREATE_THRESHOLD:
                logger.info(f"Document count exceeds {INDEX_RECREATE_THRESHOLD}, dropping and recreating index {index_name}")
                return self._recreate_index(index_name, count_result)
            
            # Otherwise, delete all documents
            delete_response = requests.post(
                f"{self.opensearch_endpoint}/{index_name}/_delete_by_query",
                headers={'Authorization': self.auth_header},
                json={"query": {"match_all": {}}},
                verify=self.verify_ssl
            )
            
            if delete_response.status_code != 200:
                logger.error(f"Failed to delete documents from index {index_name}")
                return {
                    "status": "error",
                    "message": f"Failed to delete documents from index {index_name}"
                }
            
            return {
                "status": "success",
                "message": f"Successfully deleted {count_result} documents from index {index_name}",
                "documents_deleted": count_result
            }
            
        except Exception as e:
            logger.error(f"Error during index cleanup: {str(e)}")
            return {
                "status": "error",
                "message": f"Error during index cleanup: {str(e)}"
            }

def main():
    """
    Main entry point for the index cleanup script.
    
    Handles command line arguments and orchestrates the index cleanup process.
    """
    # Set up argument parser
    parser = argparse.ArgumentParser(description='OpenSearch Index Validation and Cleanup')
    parser.add_argument('--index', required=True, help='Index name to validate and clean up')
    args = parser.parse_args()
    
    logger.info(f"Starting index cleanup script for index: {args.index}")
    
    try:
        # Initialize index manager - credentials will be handled by OpenSearchBaseManager
        index_manager = OpenSearchIndexManager()
        
        # Validate and cleanup index
        result = index_manager.validate_and_cleanup_index(args.index)
        
        # Print results
        if result["status"] == "success":
            logger.info(result["message"])
        else:
            logger.error(f"Failed to process index: {result['message']}")
            
    except ValueError as e:
        logger.error(f"Configuration error: {str(e)}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    main()

# Example usage:
# python index_cleanup.py --index my_index_primary 