"""
OpenSearch Reindex Manager

This module provides functionality for reindexing data in OpenSearch, including
validation, error handling, and logging.
"""

import requests
import logging
import base64
import os
from dotenv import load_dotenv
import argparse
import time
from index_cleanup import OpenSearchIndexManager
from datetime import datetime
from opensearch_base_manager import OpenSearchBaseManager, OpenSearchException
from typing import Dict, Any, Optional, List

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

class OpenSearchReindexManager(OpenSearchBaseManager):
    """
    Manages reindexing operations in OpenSearch.
    
    This class provides functionality to:
    - Validate source and target indices
    - Check document counts
    - Perform reindexing operations
    - Handle errors and logging
    """
    
    def __init__(self, opensearch_endpoint: Optional[str] = None):
        """
        Initialize the OpenSearch reindex manager.
        
        Args:
            opensearch_endpoint (str, optional): The OpenSearch cluster endpoint URL
        """
        super().__init__(opensearch_endpoint=opensearch_endpoint)
        self.index_manager = OpenSearchIndexManager()
        logger.info(f"Initialized OpenSearchReindexManager with endpoint: {self.opensearch_endpoint}")
    
    def reindex(self, source_index: str, target_index: str) -> Dict[str, Any]:
        """
        Reindex data from source index to target index using the OpenSearch _reindex API.
        
        Args:
            source_index (str): Name of the source index
            target_index (str): Name of the target index
            
        Returns:
            Dict[str, Any]: Result containing status and details
        """
        try:
            # Verify source index exists
            if not self._verify_index_exists(source_index):
                return {
                    "status": "error",
                    "message": f"Source index {source_index} does not exist"
                }
            
            # Get source index count
            doc_count = self._get_index_count(source_index)
            if doc_count == 0:
                return {
                    "status": "error",
                    "message": f"Source index {source_index} is empty"
                }
            
            # Validate and clean up target index
            cleanup_result = self.index_manager.validate_and_cleanup_index(target_index)
            if cleanup_result['status'] == 'error':
                return cleanup_result
            
            try:
                # Use the _reindex API to copy documents from source to target
                reindex_body = {
                    "source": {
                        "index": source_index
                    },
                    "dest": {
                        "index": target_index
                    }
                }
                
                # Make the reindex request
                reindex_result = self._make_request(
                    'POST',
                    '/_reindex',
                    data=reindex_body
                )
                
                if reindex_result['status'] == 'error':
                    return {
                        "status": "error",
                        "message": f"Failed to reindex documents: {reindex_result['message']}"
                    }
                
                # Get the reindex response
                response_data = reindex_result['response'].json()
                reindexed_count = response_data.get('total', 0)
                
                return {
                    "status": "success",
                    "message": f"Successfully reindexed {reindexed_count} documents from {source_index} to {target_index}"
                }
                
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to reindex documents: {str(e)}"
                }
                
        except Exception as e:
            error_msg = f"Error during reindex operation: {str(e)}"
            logger.error(error_msg)
            return {
                "status": "error",
                "message": error_msg
            }

def main():
    """
    Main entry point for the reindex script.
    
    Handles command line arguments and orchestrates the reindexing process.
    """
    # Set up argument parser
    parser = argparse.ArgumentParser(description='OpenSearch Reindex Operation')
    parser.add_argument('--source', required=True, help='Source index name')
    parser.add_argument('--target', required=True, help='Target index name')
    args = parser.parse_args()
    
    logger.info(f"Starting reindex script with source: {args.source}, target: {args.target}")
    
    try:
        # Initialize reindex manager - credentials will be handled by OpenSearchBaseManager
        reindex_manager = OpenSearchReindexManager()
        
        # Perform reindex operation
        result = reindex_manager.reindex(args.source, args.target)
        
        # Print results
        if result["status"] == "success":
            logger.info(result["message"])
        else:
            logger.error(f"Failed to reindex: {result['message']}")
            
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
# python reindex.py --source my_index_primary --target my_index_secondary