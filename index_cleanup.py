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

# Load environment variables
load_dotenv()

# Create log directory if it doesn't exist
logger = logging.getLogger(__name__)

# Disable SSL verification warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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

    def validate_and_cleanup_index(self, index_name: str) -> Dict[str, Any]:
        """
        Validate and clean up an index.
        
        Args:
            index_name (str): Name of the index to validate and clean up
            
        Returns:
            Dict[str, Any]: Result containing status and details
        """
        try:
            # Check if index exists
            if not self._verify_index_exists(index_name):
                return {
                    "status": "error",
                    "message": f"Index {index_name} does not exist"
                }
            
            # Check if index is part of any aliases
            alias_check = self._check_index_aliases(index_name)
            logger.info(f"Alias check result: {alias_check}")
            if alias_check["status"] == "error":
                logger.error(f"Index {index_name} is part of alias and can't be cleaned-up")
                return alias_check
            
            # Get document count
            doc_count = self._get_index_count(index_name)
            if doc_count == 0:
                return {
                    "status": "success",
                    "message": f"Index {index_name} is already empty"
                }
            
            # Delete all documents
            delete_result = self._delete_all_documents(index_name)
            if delete_result["status"] == "success":
                return {
                    "status": "success",
                    "message": f"Successfully cleaned up index {index_name}",
                    "documents_deleted": delete_result["documents_deleted"]
                }
            
            return delete_result
            
        except Exception as e:
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