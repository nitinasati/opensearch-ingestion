"""
OpenSearch Alias Manager

This module provides functionality to manage OpenSearch index aliases.
It handles operations related to switching aliases between indices,
including validation and error handling.

Key features:
- Verifies index existence
- Validates document counts
- Manages alias operations
- Provides detailed logging
- Includes error recovery
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
from typing import Optional

# Load environment variables
load_dotenv()

# Create log directory if it doesn't exist
logger = logging.getLogger(__name__)

# Disable SSL verification warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class OpenSearchAliasManager(OpenSearchBaseManager):
    """
    Manages OpenSearch index aliases.
    
    This class handles operations related to OpenSearch index aliases such as:
    - Verifying index existence
    - Validating document counts
    - Managing alias operations
    """
    
    def __init__(self, opensearch_endpoint: Optional[str] = None, 
                 username: Optional[str] = None, 
                 password: Optional[str] = None,
                 verify_ssl: bool = False):
        """
        Initialize the alias manager.
        
        Args:
            opensearch_endpoint (str, optional): The OpenSearch cluster endpoint URL
            username (str, optional): OpenSearch username
            password (str, optional): OpenSearch password
            verify_ssl (bool): Whether to verify SSL certificates
        """
        super().__init__(opensearch_endpoint, username, password, verify_ssl)
        logger.info(f"Initialized OpenSearchAliasManager with endpoint: {self.opensearch_endpoint}")

    def _get_alias_info(self, alias_name: str) -> dict:
        """
        Get information about an alias.
        
        Args:
            alias_name (str): Name of the alias to get information for
            
        Returns:
            dict: Alias information including index, filter, and routing settings
        """
        try:
            logger.debug(f"Getting information for alias: {alias_name}")
            response = requests.get(
                f"{self.opensearch_endpoint}/_cat/aliases/{alias_name}?format=json",
                headers={
                    'Authorization': self.auth_header,
                    'Accept': self.CONTENT_TYPE_JSON
                },
                verify=self.verify_ssl
            )
            
            if response.status_code == 200:
                aliases = response.json()
                if aliases:
                    alias_info = {
                        "alias": aliases[0].get("alias"),
                        "index": aliases[0].get("index"),
                        "filter": aliases[0].get("filter"),
                        "routing.index": aliases[0].get("routing.index"),
                        "routing.search": aliases[0].get("routing.search")
                    }
                    logger.debug(f"Alias info: {alias_info}")
                    return alias_info
                logger.warning(f"No information found for alias: {alias_name}")
                return {}
            else:
                logger.error(f"Failed to get alias info. Status code: {response.status_code}")
                return {}
        except Exception as e:
            logger.error(f"Error getting alias info: {str(e)}")
            return {}

    def _validate_document_count_difference(self, source_index: str, target_index: str) -> dict:
        """
        Validate that the document count difference between indices is not more than the configured threshold.
        
        Args:
            source_index (str): Name of the source index
            target_index (str): Name of the target index
            
        Returns:
            dict: Validation result containing status and details
        """
        try:
            source_count = self._get_index_count(source_index)
            target_count = self._get_index_count(target_index)
            percentage_diff = 0
            # Get threshold from environment variable, default to 10%
            threshold = float(os.getenv('DOCUMENT_COUNT_THRESHOLD', '10'))
            if target_count == 0 and source_count > 0:
                error_msg = "Target index is empty, can't switch alias"
                logger.error(error_msg)
                return {
                    "status": "error",
                    "message": error_msg
                }
            
            # Calculate percentage difference
            if target_count != source_count:
                max_diff = max(source_count, target_count)
                min_diff = min(source_count, target_count)
                percentage_diff = ((max_diff - min_diff) / max_diff) * 100
                
                logger.info(f"Document count threshold: {threshold}%")
                
                logger.info(f"Source index count: {source_count}")
                logger.info(f"Target index count: {target_count}")
                logger.info(f"Document count difference: {percentage_diff:.2f}%")
                logger.info(f"Threshold: {threshold}%")
            
                if percentage_diff > threshold:
                    error_msg = f"Document count difference ({percentage_diff:.2f}%) exceeds {threshold}% threshold"
                    logger.error(error_msg)
                    return {
                        "status": "error",
                        "message": error_msg,
                        "source_count": source_count,
                        "target_count": target_count,
                        "percentage_diff": percentage_diff,
                        "threshold": threshold
                    }
            
            success_msg = "Document count validation passed"
            logger.info(success_msg)
            return {
                "status": "success",
                "message": success_msg,
                "source_count": source_count,
                "target_count": target_count,
                "percentage_diff": percentage_diff,
                "threshold": threshold
            }
            
        except Exception as e:
            error_msg = f"Error validating document count difference: {str(e)}"
            logger.error(error_msg)
            return {
                "status": "error",
                "message": error_msg
            }

    def switch_alias(self, alias_name: str, source_index: str, target_index: str) -> dict:
        """
        Switch an alias from source index to target index.
        
        This method handles the complete alias switching process including:
        - Verifying index existence
        - Validating document counts
        - Executing the alias switch
        - Verifying the results
        
        Args:
            alias_name (str): Name of the alias to switch
            source_index (str): Current source index name
            target_index (str): New target index name
            
        Returns:
            dict: Operation result containing status and details
        """
        start_time = time.time()
        logger.info(f"Starting alias switch operation for {alias_name} from {source_index} to {target_index}")
        
        try:
            # Verify both indices exist
            if not self._verify_index_exists(source_index):
                error_msg = f"Source index {source_index} does not exist"
                logger.error(error_msg)
                return {
                    "status": "error",
                    "message": error_msg
                }
            
            if not self._verify_index_exists(target_index):
                error_msg = f"Target index {target_index} does not exist"
                logger.error(error_msg)
                return {
                    "status": "error",
                    "message": error_msg
                }
            
            # Validate document count difference
            count_validation = self._validate_document_count_difference(source_index, target_index)
            if count_validation["status"] == "error":
                logger.error(f"Document count validation failed: {count_validation['message']}")
                return count_validation
            
            # Get current alias information
            alias_info = self._get_alias_info(alias_name)
            if not alias_info:
                error_msg = f"Alias {alias_name} does not exist"
                logger.error(error_msg)
                return {
                    "status": "error",
                    "message": error_msg
                }
            
            # Prepare alias update request
            alias_body = {
                "actions": [
                    {
                        "remove": {
                            "index": source_index,
                            "alias": alias_name
                        }
                    },
                    {
                        "add": {
                            "index": target_index,
                            "alias": alias_name
                        }
                    }
                ]
            }
            
            # Execute alias update
            logger.info("Executing alias switch operation")
            response = requests.post(
                f"{self.opensearch_endpoint}/_aliases",
                headers={
                    'Authorization': self.auth_header,
                    'Content-Type': self.CONTENT_TYPE_JSON,
                    'Accept': self.CONTENT_TYPE_JSON
                },
                json=alias_body,
                verify=self.verify_ssl
            )
            
            if response.status_code != 200:
                error_msg = f"Failed to switch alias. Status code: {response.status_code}"
                logger.error(error_msg)
                return {
                    "status": "error",
                    "message": error_msg,
                    "response": response.text
                }
            
            end_time = time.time()
            total_time = end_time - start_time
            
            success_msg = f"Successfully switched alias {alias_name} from {source_index} to {target_index}"
            logger.info(success_msg)
            logger.info(f"Total time taken: {round(total_time, 2)} seconds")
            
            return {
                "status": "success",
                "message": success_msg,
                "source_count": count_validation["source_count"],
                "target_count": count_validation["target_count"],
                "percentage_diff": count_validation["percentage_diff"],
                "total_time_seconds": round(total_time, 2)
            }
            
        except Exception as e:
            error_msg = f"Error during alias switch: {str(e)}"
            logger.error(error_msg)
            return {
                "status": "error",
                "message": error_msg
            }

def main():
    """
    Main entry point for the alias switching script.
    
    Handles command line arguments and orchestrates the alias switching process.
    """
    # Set up argument parser
    parser = argparse.ArgumentParser(description='OpenSearch Alias Switching')
    parser.add_argument('--alias', required=True, help='Alias name to switch')
    parser.add_argument('--source', required=True, help='Source index name')
    parser.add_argument('--target', required=True, help='Target index name')
    args = parser.parse_args()
    
    logger.info(f"Starting alias switching script with alias: {args.alias}, source: {args.source}, target: {args.target}")
    
    try:
        # Initialize alias manager - credentials will be handled by OpenSearchBaseManager
        alias_manager = OpenSearchAliasManager()
        
        # Perform alias switching
        result = alias_manager.switch_alias(args.alias, args.source, args.target)
        
        # Print results
        if result["status"] == "success":
            logger.info(result["message"])
            logger.info(f"Source document count: {result['source_count']}")
            logger.info(f"Target document count: {result['target_count']}")
            logger.info(f"Document count difference: {result['percentage_diff']:.2f}%")
            logger.info(f"Total time taken: {result['total_time_seconds']} seconds")
        else:
            logger.error(f"Failed to switch alias: {result['message']}")
            
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
# python switch_alias.py --alias my_index_alias --source my_index_primary --target my_index_secondary