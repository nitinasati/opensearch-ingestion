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
from opensearch_base_manager import OpenSearchBaseManager
from typing import Dict, Any, Optional

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
    
    def __init__(self, opensearch_endpoint: Optional[str] = None, 
                 username: Optional[str] = None, 
                 password: Optional[str] = None,
                 verify_ssl: bool = False):
        """
        Initialize the reindex manager.
        
        Args:
            opensearch_endpoint (str, optional): The OpenSearch cluster endpoint URL
            username (str, optional): OpenSearch username
            password (str, optional): OpenSearch password
            verify_ssl (bool): Whether to verify SSL certificates
        """
        super().__init__(opensearch_endpoint, username, password, verify_ssl)
        self.index_manager = OpenSearchIndexManager()
        logger.info(f"Initialized OpenSearchReindexManager with endpoint: {self.opensearch_endpoint}")
    
    def reindex_data(self, source_index: str, target_index: str) -> Dict[str, Any]:
        """
        Reindex data from source index to target index.
        
        Args:
            source_index (str): Name of the source index
            target_index (str): Name of the target index
            
        Returns:
            Dict[str, Any]: Result containing status and details
        """
        try:
            # Validate source index exists
            if not self._verify_index_exists(source_index):
                return {
                    "status": "error",
                    "message": f"Source index {source_index} does not exist"
                }
            
            # Get source index count
            source_count = self._get_index_count(source_index)
            if source_count == 0:
                return {
                    "status": "error",
                    "message": f"Source index {source_index} has no documents"
                }
            
            # Clean up target index if it exists
       
            logger.info(f"Cleaning up target index {target_index}")
            cleanup_result = self.index_manager.validate_and_cleanup_index(target_index)
            if cleanup_result["status"] == "error":
                return {
                    "status": "error",
                    "message": f"Failed to clean up target index: {cleanup_result['message']}"
                }
            logger.info("Successfully cleaned up target index")
            
            # Perform reindex operation
            reindex_url = f"{self.opensearch_endpoint}/_reindex"
            reindex_body = {
                "source": {
                    "index": source_index
                },
                "dest": {
                    "index": target_index
                }
            }
            
            response = requests.post(
                reindex_url,
                headers={
                    'Authorization': self.auth_header,
                    'Content-Type': self.CONTENT_TYPE_JSON
                },
                json=reindex_body,
                verify=self.verify_ssl
            )
            
            if response.status_code == 200:
                result = response.json()
                total_docs = result.get('total', 0)
                created_docs = result.get('created', 0)
                updated_docs = result.get('updated', 0)
                failed_docs = result.get('failed', 0)
                
                logger.info("Reindex completed successfully:")
                logger.info(f"- Total documents processed: {total_docs}")
                logger.info(f"- Documents created: {created_docs}")
                logger.info(f"- Documents updated: {updated_docs}")
                logger.info(f"- Documents failed: {failed_docs}")
                
                return {
                    "status": "success",
                    "message": "Reindex completed successfully",
                    "total_documents": total_docs,
                    "created_documents": created_docs,
                    "updated_documents": updated_docs,
                    "failed_documents": failed_docs
                }
            
            error_msg = f"Failed to reindex data. Status code: {response.status_code}"
            logger.error(error_msg)
            return {
                "status": "error",
                "message": error_msg
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
        result = reindex_manager.reindex_data(args.source, args.target)
        
        # Print results
        if result["status"] == "success":
            logger.info(result["message"])
            logger.info(f"Total documents processed: {result['total_documents']}")
            logger.info(f"Documents created: {result['created_documents']}")
            logger.info(f"Documents updated: {result['updated_documents']}")
            logger.info(f"Documents failed: {result['failed_documents']}")
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