#!/usr/bin/env python3
"""
AWS SQS Queue Watcher

This script continuously monitors an AWS SQS queue and prints messages as they arrive.
It handles message processing, error handling, and provides detailed logging.
Configuration is managed through environment variables.
"""

import boto3
import logging
import time
import json
import os
from typing import Dict, Any
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SQSWatcher:
    """Class to handle SQS queue monitoring and message processing."""
    
    def __init__(self):
        """
        Initialize the SQS watcher with configuration from environment variables.
        """
        # Get configuration from environment variables
        self.queue_url = os.getenv('SQS_QUEUE_URL')
        self.region = os.getenv('AWS_REGION', 'us-east-1')
        self.wait_time = int(os.getenv('SQS_WAIT_TIME', '20'))
        self.schedule_interval = int(os.getenv('SQS_SCHEDULE_INTERVAL', '0'))  # in minutes, 0 means continuous
        
        if not self.queue_url:
            raise ValueError("Queue URL must be provided via environment variable SQS_QUEUE_URL")
        
        # Initialize SQS client
        self.sqs_client = boto3.client('sqs', region_name=self.region)
        logger.info(f"Initialized SQS watcher for queue: {self.queue_url}")
        logger.info(f"Region: {self.region}, Wait time: {self.wait_time}s, Schedule interval: {self.schedule_interval}min")
        
    def _process_message(self, message: Dict[str, Any]) -> None:
        """
        Process a single message from the queue.
        
        Args:
            message (Dict[str, Any]): The SQS message to process
        """
        try:
            # Extract message body
            body = message.get('Body', '')
            
            # Try to parse as JSON if possible
            try:
                body = json.loads(body)
                logger.info("Message body (JSON):")
                print(json.dumps(body, indent=2))
            except json.JSONDecodeError:
                logger.info("Message body (raw):")
                print(body)
                
            # Print message attributes if present
            if 'MessageAttributes' in message:
                logger.info("Message attributes:")
                for attr_name, attr_value in message['MessageAttributes'].items():
                    print(f"{attr_name}: {attr_value.get('StringValue', '')}")
                    
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            
    def _delete_message(self, receipt_handle: str) -> None:
        """
        Delete a processed message from the queue.
        
        Args:
            receipt_handle (str): The receipt handle of the message to delete
        """
        try:
            self.sqs_client.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=receipt_handle
            )
            logger.debug("Successfully deleted message from queue")
        except ClientError as e:
            logger.error(f"Error deleting message: {str(e)}")
            
    def _process_messages(self) -> None:
        """Process messages from the queue."""
        try:
            # Receive messages from the queue
            response = self.sqs_client.receive_message(
                QueueUrl=self.queue_url,
                AttributeNames=['All'],
                MessageAttributeNames=['All'],
                MaxNumberOfMessages=10,
                WaitTimeSeconds=self.wait_time
            )
            
            # Process messages if any were received
            messages = response.get('Messages', [])
            if messages:
                logger.info(f"Received {len(messages)} messages")
                
                for message in messages:
                    logger.info("=" * 50)
                    logger.info(f"Processing message ID: {message.get('MessageId', 'unknown')}")
                    
                    # Process the message
                    self._process_message(message)
                    
                    # Delete the message after processing
                    self._delete_message(message['ReceiptHandle'])
                    
                    logger.info("=" * 50)
                    
        except ClientError as e:
            logger.error(f"AWS API error: {str(e)}")
            time.sleep(5)  # Wait before retrying
            
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            time.sleep(5)  # Wait before retrying
            
    def watch_queue(self) -> None:
        """Continuously monitor the SQS queue for new messages."""
        logger.info(f"Starting to watch queue: {self.queue_url}")
        
        try:
            if self.schedule_interval > 0:
                # Scheduled mode
                while True:
                    next_run = datetime.now() + timedelta(minutes=self.schedule_interval)
                    logger.info(f"Processing messages. Next run at: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
                    
                    self._process_messages()
                    
                    # Calculate sleep time until next run
                    sleep_time = (next_run - datetime.now()).total_seconds()
                    if sleep_time > 0:
                        logger.info(f"Sleeping for {sleep_time:.2f} seconds until next run")
                        time.sleep(sleep_time)
            else:
                # Continuous mode
                while True:
                    self._process_messages()
                    
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt. Shutting down...")
            
def main():
    """Main entry point for the script."""
    try:
        # Create and start the SQS watcher
        watcher = SQSWatcher()
        watcher.watch_queue()
    except ValueError as e:
        logger.error(f"Configuration error: {str(e)}")
        logger.error("Please ensure the following environment variables are set:")
        logger.error("  - SQS_QUEUE_URL: URL of the SQS queue to monitor")
        logger.error("  - AWS_REGION: AWS region (optional, defaults to us-east-1)")
        logger.error("  - SQS_WAIT_TIME: Long polling wait time in seconds (optional, defaults to 20)")
        logger.error("  - SQS_SCHEDULE_INTERVAL: Schedule interval in minutes (optional, defaults to 0 for continuous)")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
    
if __name__ == "__main__":
    main() 