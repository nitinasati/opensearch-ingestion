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
    
    def __init__(self, opensearch_endpoint: Optional[str] = None, verify_ssl: bool = False):
        """
        Initialize the index manager.
        
        Args:
            opensearch_endpoint (str, optional): The OpenSearch cluster endpoint URL
            verify_ssl (bool): Whether to verify SSL certificates
        """
        super().__init__(opensearch_endpoint, verify_ssl)
        logger.info("Initialized OpenSearchIndexManager")

    def validate_and_cleanup_index(self, index_name: str) -> Dict[str, Any]:
        """
        Validate and clean up an index.
        
        This method checks if the index exists, verifies its document count,
        and cleans up documents if necessary. It also checks if the index
        is part of an alias and prevents deletion if it is.
        
        Args:
            index_name (str): Name of the index to validate and clean up
            
        Returns:
            Dict[str, Any]: Result containing status and details
        """
        try:
            # Check if index exists
            if not self._verify_index_exists(index_name):
                logger.info(f"Index {index_name} does not exist")
                return {
                    "status": "success",
                    "message": f"Index {index_name} does not exist"
                }
            
            # Check if index is part of an alias
            alias_info = self._check_index_aliases(index_name)
            if alias_info and len(alias_info) > 0:
                alias_names = list(alias_info.keys())
                error_msg = f"Index {index_name} is part of alias(es): {', '.join(alias_names)}. Cannot remove data from an aliased index."
                logger.error(error_msg)
                return {
                    "status": "error",
                    "message": error_msg,
                    "aliases": alias_names
                }
            
            # Get current document count
            current_count = self._get_index_count(index_name)
            logger.info(f"Current document count for {index_name}: {current_count}")
            
            # Check if document count is within threshold
            threshold = int(os.getenv('DOCUMENT_COUNT_THRESHOLD', '1000000'))
            if current_count <= threshold:
                logger.info(f"Document count ({current_count}) is within threshold ({threshold}). Cleaning up documents...")
                cleanup_result = self._delete_all_documents(index_name)
                if cleanup_result["status"] == "error":
                    return cleanup_result
                return {
                    "status": "success",
                    "message": f"Successfully cleaned up index {index_name}",
                    "documents_deleted": current_count
                }
            else:
                logger.info(f"Document count ({current_count}) exceeds threshold ({threshold}). Recreating index...")
                return self._recreate_index(index_name)
                
        except Exception as e:
            error_msg = f"Error during index validation and cleanup: {str(e)}"
            logger.error(error_msg)
            return {
                "status": "error",
                "message": error_msg
            }

    def _recreate_index(self, index_name: str) -> Dict[str, Any]:
        """
        Recreate an index by preserving settings and mappings.
        
        Args:
            index_name (str): Name of the index to recreate
            
        Returns:
            Dict[str, Any]: Result containing status and details
        """
        try:
            # Get index settings
            settings_response = self._make_request('GET', f'/{index_name}/_settings')
            if settings_response.status_code != 200:
                return {
                    "status": "error",
                    "message": f"Failed to get index settings. Status code: {settings_response.status_code}"
                }
            
            settings = settings_response.json()
            if index_name not in settings:
                return {
                    "status": "error",
                    "message": f"Index {index_name} not found in settings response"
                }
            
            # Get index mappings
            mappings_response = self._make_request('GET', f'/{index_name}/_mappings')
            if mappings_response.status_code != 200:
                return {
                    "status": "error",
                    "message": f"Failed to get index mappings. Status code: {mappings_response.status_code}"
                }
            
            mappings = mappings_response.json()
            if index_name not in mappings:
                return {
                    "status": "error",
                    "message": f"Index {index_name} not found in mappings response"
                }
            
            # Drop existing index
            drop_response = self._make_request('DELETE', f'/{index_name}')
            if drop_response.status_code != 200:
                return {
                    "status": "error",
                    "message": f"Failed to drop index. Status code: {drop_response.status_code}"
                }
            
            # Filter out internal settings that can't be set manually
            index_settings = settings[index_name]["settings"]["index"]
            filtered_settings = {
                k: v for k, v in index_settings.items()
                if k not in ["creation_date", "uuid", "version", "provided_name"]
            }
            
            # Create new index with preserved settings and mappings
            create_payload = {
                "settings": {
                    "index": filtered_settings
                },
                "mappings": mappings[index_name]["mappings"]
            }
            
            logger.info(f"Creating index {index_name} with preserved settings and mappings")
            create_response = self._make_request('PUT', f'/{index_name}', data=create_payload)
            
            if create_response.status_code == 200:
                return {
                    "status": "success",
                    "message": f"Successfully recreated index {index_name}"
                }
            else:
                return {
                    "status": "error",
                    "message": f"Failed to recreate index. Status code: {create_response.status_code}"
                }
            
        except Exception as e:
            error_msg = f"Error recreating index: {str(e)}"
            logger.error(error_msg)
            return {
                "status": "error",
                "message": error_msg
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