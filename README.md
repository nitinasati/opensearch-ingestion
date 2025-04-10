# OpenSearch Ingestion System

A Python-based system for ingesting data from multiple sources (S3, local files, and folders) into OpenSearch with support for parallel processing, batch operations, and zero-downtime data refreshes.

## Features

- Efficient CSV and JSON file processing using pandas
- Parallel processing with configurable number of threads
- Document count validation (loaded to opensearch vs source records to be loaded) 
- Comprehensive error handling and logging
- Failed records to go into DLQ for further troubleshooting and correction
- Robust response handling for OpenSearch operations
- Resume and fresh load modes for interrupted operations
- Flexible file source options (local folder, S3, individual files)
- Upsert capability for updating existing documents (if source file has id field/attribute which uniquely identifies the document - must match with _id in index)

## Prerequisites

- Python 3.8+
- AWS IAM role with access to S3 and opensearch
- Required Python packages (install using `pip install -r requirements.txt`)

## Configuration

### Authentication Options

###OpenSearch Backend Role
The system supports OpenSearch backend role authentication using AWS IAM roles. This is the recommended approach for production environments.

1. Configure your OpenSearch domain to use IAM authentication
2. Set up an IAM role with appropriate permissions
3. Configure the following in your `.env` file:
```
OPENSEARCH_ENDPOINT=your_opensearch_endpoint
AWS_REGION=your_aws_region
```

Required IAM permissions:
1. **OpenSearch Cluster** (for all opensearch API calls):
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
				"es:ESHttpPost",
				"es:ESHttpPut",
				"es:ESHttpGet"
            ],
            "Resource": "arn:aws:es:region:account:domain/domain-name"
        }
    ]
}
```
2. **S3 Permissions** (for bulk ingestion):
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::your-bucket-name",
                "arn:aws:s3:::your-bucket-name/*"
            ]
        }
    ]
}
```
3. **SQS Permissions** (for error reporting to DLQ):
```json
{
    "Version": "2012-10-17",
    "Statement": [
		{
			"Effect": "Allow",
			"Action": [
				"sqs:GetQueueUrl",
				"sqs:SendMessage"
			],
			"Resource": "arn:aws:sqs:region:account-id:queue-name"
		}
    ]
}
```

Note: For SQS permissions, replace:
- `region` with your AWS region (e.g., us-east-1)
- `account-id` with your AWS account ID
- `queue-name` with your actual queue name

If you want to grant permissions to all queues in your account (not recommended for production), you can use:
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "sqs:GetQueueUrl",
                "sqs:SendMessage"
            ],
            "Resource": "arn:aws:sqs:region:account-id:*"
        }
    ]
}
```

## Usage

### Bulk Ingestion

The `bulkupdate.py` script provides flexible options for ingesting data from various sources:

#### File Source Options

1. **S3 Bucket**:
   ```bash
   python bulkupdate.py --bucket my-bucket --prefix data/ --index my_index
   ```

2. **Local Folder** (processes all CSV and JSON files in the folder):
   ```bash
   python bulkupdate.py --local-folder ./data --index my_index
   ```

3. **Individual Files** (mix of local files and S3 files):
   ```bash
   python bulkupdate.py --local-files data1.csv data2.json --index my_index
   ```

4. **Combined Sources** (process files from both S3 and local sources):
   ```bash
   python bulkupdate.py --bucket my-bucket --prefix data/ --local-files local1.csv local2.json --index my_index
   ```

Alteast one source is mandatory from #1, 2 or 3. All can be provided at the same time.

#### Processing Modes

- **Resume Mode** - optional (continue from where it left off - fresh load if this argument is not added):
  ```bash
  python bulkupdate.py --bucket my-bucket --prefix data/ --index my_index --resume
  ```

- **Fresh Load Mode** - optional (clear tracking file and process all files - this is optional argument and default is fresh load only):
  ```bash
  python bulkupdate.py --bucket my-bucket --prefix data/ --index my_index --fresh-load
  ```

#### Performance Tuning

- **Batch Size** - optional (Number of documents to process in each batch (default: 10000)):
  ```bash
  python bulkupdate.py --bucket my-bucket --prefix data/ --index my_index --batch-size 1000
  ```

- **Worker Threads** -optional (number of parallel processing threads - default is 4):
  ```bash
  python bulkupdate.py --bucket my-bucket --prefix data/ --index my_index --max-workers 4
  ```

#### Complete Example

```bash
python bulkupdate.py --bucket openlpocbucket --prefix opensearch/ --index my_index_primary --batch-size 1000 --max-workers 8
```

### Resume and Fresh Load Modes

The bulk ingestion tool supports two modes for handling file processing:

1. **Resume Mode** (`--resume`):
   - Skips files that have already been successfully processed
   - Uses a tracking file (`processed_files.json`) to maintain a record of processed files
   - Useful when a previous ingestion run was interrupted
   - Allows you to continue processing from where it left off
   - Verifies document count based on processed records from bulk API responses

2. **Fresh Load Mode** (default behavior):
   - Clears the tracking file for the specified index
   - Processes all files regardless of previous processing history
   - Useful when you want to reprocess all files from scratch
   - This is the default behavior when no flags are specified
   - Verifies document count based on processed records from bulk API responses

3. **Upsert Mode** (default behavior when id field is part of ingested files):
   - Updates existing documents if they exist in the index
   - Inserts new documents if they don't exist
   - Uses document ID from source files to identify existing documents
   - Preserves existing documents that aren't in the source files
   - Useful for incremental updates without full reindexing
   - Requires source files to have a unique identifier field

Note: You cannot specify `--resume` or omit this option for fresh-load which is default behavior.

### Alias Management (for supporting zero downtime)

The system includes functionality for managing OpenSearch aliases, allowing you to switch aliases between indices with validation:

```bash
python switch_alias.py --alias <alias_name> --source <source_index> --target <target_index>
```

#### Required Arguments:
- `--alias`: Name of the alias to switch
- `--source`: Current source index name
- `--target`: New target index name

#### Example:
```bash
python switch_alias.py --alias my_index_alias --source my_index_primary --target my_index_secondary
```

### Parallel Processing

The system  supports parallel processing of CSV and JSON files with the following features:
- Configurable number of worker threads
- Thread-safe document counting
- Queue-based batch processing
- Graceful worker shutdown
- Error handling for worker threads
- Accurate record counting from bulk API responses

The number of threads can be adjusted based on your system's capabilities and requirements. A higher number of threads may improve performance but will also increase memory usage and network connections.

## Recent Updates

### Response Handling Improvements

The system has been updated with improved response handling for OpenSearch operations:

1. **Consistent Response Structure**:
   - All OpenSearch operations now return a standardized response dictionary
   - Response includes status, message, and response object
   - Proper error handling for all API calls

2. **Fixed Response Access**:
   - Correctly accessing response objects from the result dictionary
   - Proper handling of status codes and error messages
   - Improved error reporting for failed operations

3. **Enhanced Error Handling**:
   - Better error messages with specific details
   - Proper logging of error conditions
   - Graceful recovery from common error scenarios

4. **Index Operations**:
   - Improved index validation and cleanup
   - Better handling of index existence checks
   - Enhanced document count verification using bulk API response counts

5. **Alias Management**:
   - Fixed alias switching operations
   - Improved alias information retrieval
   - Better validation of alias operations
   - Early validation of alias existence

### Resume and Fresh Load Functionality

The system now supports resume and fresh load modes for bulk ingestion:

1. **Resume Mode** (`--resume`):
   - Tracks successfully processed files in a JSON tracking file
   - Allows resuming interrupted ingestion processes
   - Skips already processed files to save time and resources
   - Maintains processing history per index
   - Must be explicitly specified with the `--resume` flag
   - Verifies document count based on processed records from bulk API responses

2. **Fresh Load Mode** (default behavior):
   - Clears the tracking file for a specific index
   - Enables reprocessing of all files from scratch
   - Useful for data refreshes or when reprocessing is needed
   - This is the default behavior when no flags are specified
   - Can be explicitly specified with the `--fresh-load` flag for clarity
   - Verifies document count based on processed records from bulk API responses

3. **File Tracking**:
   - Maintains a record of processed files in `processed_files.json`
   - Organizes tracking data by index name
   - Provides clear logging of skipped and processed files

4. **Accurate Record Counting**:
   - Tracks processed records based on bulk API response counts
   - Provides more accurate document count verification
   - Ensures data consistency between source and target indices

### Data Ingestion Requirements

For detailed information about data ingestion requirements, including attribute matching and delta updates, please refer to the [Data Ingestion Requirements](data_ingestion_requirements.md) document. This document covers:

- Attribute matching between source data and index mappings
- Delta updates for existing records
- Data type compatibility
- Best practices for data ingestion

## Performance Configuration Guide

### Batch Size Selection

The batch size significantly impacts both performance and resource usage. Consider the following factors when selecting a batch size:

1. **Document Size**:
   - Larger documents require smaller batch sizes
   - Smaller documents can use larger batch sizes
   - Monitor the size of your bulk requests (should not exceed OpenSearch's limits)

2. **OpenSearch Cluster Capacity**:
   - Consider your cluster's memory and CPU resources
   - Monitor OpenSearch's bulk queue size
   - Watch for rejected execution exceptions

3. **Testing Recommendations**:
   - Start with a conservative batch size (e.g., 1000)
   - Gradually increase while monitoring:
     - Memory usage
     - CPU utilization
     - Network bandwidth
     - OpenSearch cluster health
   - Stop increasing when you see:
     - Rejected execution exceptions
     - High memory usage
     - Cluster health degradation

### Max Workers Configuration

The number of parallel workers affects both throughput and resource consumption. Consider these factors:

1. **Server Resources**:
   - CPU cores available
   - Available memory
   - Network bandwidth
   - Disk I/O capacity

2. **OpenSearch Cluster Capacity**:
   - Number of nodes
   - Available memory per node
   - Network capacity
   - Bulk queue size

3. **Testing Process**:
   - Start with 2-4 workers
   - Monitor:
     - Server CPU usage
     - Memory consumption
     - Network bandwidth
     - OpenSearch cluster health
   - Increase gradually until you find the optimal balance
   - Stop when you see:
     - Server resource constraints
     - Network bottlenecks
     - OpenSearch cluster stress

### Performance Testing Checklist

Before running in production, perform rigorous testing:

1. **Resource Monitoring**:
   - Server CPU usage
   - Memory consumption
   - Network bandwidth
   - Disk I/O
   - OpenSearch cluster health

2. **Error Monitoring**:
   - Rejected execution exceptions
   - Rate limiting
   - Network timeouts
   - Memory pressure

3. **Performance Metrics**:
   - Documents processed per second
   - Batch processing time
   - Total ingestion time
   - Resource utilization

4. **Load Testing**:
   - Test with different file sizes
   - Test with varying document sizes
   - Test with different worker counts
   - Test with different batch sizes

### Recommended Starting Points

1. **For Small Documents (< 1KB)**:
   - Batch size: 5000-10000
   - Max workers: 4-8
   - Monitor and adjust based on cluster capacity

2. **For Medium Documents (1KB-10KB)**:
   - Batch size: 1000-5000
   - Max workers: 2-4
   - Adjust based on document size and cluster capacity

3. **For Large Documents (> 10KB)**:
   - Batch size: 100-1000
   - Max workers: 1-2
   - Monitor bulk request size carefully

### Environment Variables

## Error Handling

The system includes comprehensive error handling:
- Validates environment variables
- Checks for required permissions
- Verifies index existence
- Validates document counts
- Handles network errors
- Provides detailed error messages

## Dead Letter Queue (DLQ) and Message Splitting

The system includes robust error reporting through SQS Dead Letter Queues (DLQ) with automatic message splitting for large payloads:

### DLQ Configuration
- Configure SQS DLQ ARN through environment variables
- Failed records are automatically sent to the configured DLQ
- Each error message includes:
  - Error details and messages
  - Source file information
  - Failed record data
  - Timestamp and metadata

### Message Splitting
When error payloads exceed SQS's 230 KB message size limit, the system automatically splits them:

1. **Size Calculation**:
   - Base payload size (metadata, headers)
   - Available space for records
   - Average record size estimation

2. **Splitting Process**:
   - Divides records into multiple messages
   - Each message includes:
     - Part number (message_part)
     - Total parts (total_parts)
     - Total records (total_records)
     - Timestamp
     - Original error information

3. **Message Structure**:
   ```json
   {
     "error_message": "Error details",
     "file_key": "source/file/path",
     "source": "file_source",
     "message_part": 1,
     "total_parts": 3,
     "total_records": 1000,
     "timestamp": "2024-03-21T10:00:00Z",
     "failed_records": [...]
   }
   ```

4. **Recovery Process**:
   - Messages can be reassembled using part numbers
   - Total records count ensures completeness
   - Timestamps help with message ordering
   - Original error context is preserved

## Logging

All operations are logged with:
- Timestamps
- Operation details
- Success/failure status
- Performance metrics
- Error details when applicable

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## System Characteristics

### Load Type
- This system is designed for **full/delta load** operations
- It does  support delta/incremental loads when dataset has id attribute to uniquely identity record
- Each ingestion operation completely refreshes the target index with new data
- The system follows a zero-downtime strategy to ensure data consistency during full refreshes

### Limitations
- No support for partial updates or incremental data ingestion if data set doesn't have id attribute (insert only behavior)
- Each ingestion will perform processing the entire dataset from sources
- Historical data changes are not tracked or preserved
- Previous versions of records are overwritten during ingestion

### Environment Variables
Required environment variables in `.env` file:
```
# OpenSearch Configuration
OPENSEARCH_ENDPOINT=<opensearch endpoint>
AWS_REGION=<aws-region>

# AWS IAM Role Configuration
AWS_PROFILE=default  # Optional: specify AWS profile if using multiple profiles
AWS_ROLE_ARN=<role-arn>  # Optional: specify role ARN if using role assumption

# Performance Configuration
BATCH_SIZE=10000
MAX_WORKERS=4
INDEX_RECREATE_THRESHOLD=1000000

# Logging Configuration
LOG_LEVEL=INFO
LOG_FORMAT=%(asctime)s - %(name)s - %(levelname)s - %(message)s

# SSL Configuration
VERIFY_SSL=false  # Set to true in production

# DLQ Configuration
DLQ=enabled  # Set to 'enabled' to enable SQS DLQ for error reporting, 'disabled' to skip
SQS-DLQ-ARN=arn:aws:sqs:region:account-id:queue-name  # Required if DLQ is enabled

# Optional environment variables
DOCUMENT_COUNT_THRESHOLD=100  # Percentage difference threshold for alias switch operation to avoid alias switch when document count is 0 in target index

```

## Job Execution Order

The system follows a zero-downtime data refresh strategy with the following steps:

### Step 1: Copy Primary Index to Secondary
```bash
python reindex.py --source my_index_primary --target my_index_secondary
```
This step creates a copy of the primary index in the secondary index, preparing it for the data refresh process.


### Step 2: Switch Alias to Secondary Index
```bash
python switch_alias.py --alias my_index_alias --source my_index_primary --target my_index_secondary
```
This step switches the alias from the primary index to the secondary index, allowing the primary index to be updated without affecting live traffic.

### Step 3: Bulk Update Primary Index
```bash
python bulkupdate.py --bucket your-bucket --prefix your/prefix/ --index my_index_primary --batch-size 1000
```
This step ingests fresh data from S3 CSV files into the primary index, including validation to ensure correct record counts.

### Step 4: Switch Alias Back to Primary
```bash
python switch_alias.py --alias my_index_alias --source my_index_secondary --target my_index_primary
```
This step switches the alias back to the primary index, making the fresh data available to users.

### Execution Timing
- Each step includes timing information
- 5-second delay between steps for stability
- Total execution time is tracked and reported
- Individual step durations are logged

## Features


### 1. Reindexing (`reindex.py`)
- Copies data between indices
- Validates source and target indices
- Verifies document counts
- Handles authentication and SSL
- Provides detailed operation results
- Safety Guardrails:
  - Validates existence of both source and target indices before operation
  - Aborts operation if index is serving live traffic (part of an alias)
  - Verifies document count consistency between source and target
  - Validates index health status before starting reindex
  - Provides rollback capability in case of failures
  - Logs all validation steps for audit trail

### 2. Alias Management (`switch_alias.py`)
- Switches aliases between indices
- Validates document count differences
- Configurable threshold for document count differences
- Safe alias switching with validation
- Safety Guardrails:
  - Prevents alias switch if target index has zero records to avoid empty search results
  - Validates record count difference using configurable threshold from environment variable
  - Calculates percentage difference between source and target indices
  - Aborts alias switch if record count difference exceeds threshold
  - Provides detailed logging of record count validation
  - Ensures data consistency before and after alias switch
  - Validates index health status before alias operations
  - Maintains audit trail of all validation checks

### 3. Bulk Data Ingestion (`bulkupdate.py`)
- Processes CSV files from S3 buckets
- Handles batch processing for efficient ingestion
- Supports custom batch sizes
- Validates document counts
- Provides detailed progress logging
- Handles various data types (numeric, boolean, text)
- Safety Guardrails:
  - Validates if target index exists before ingestion
  - Aborts operation if target index is serving live traffic (part of an alias, prevents accidental ingestion into production indices
  - Validates index health status before starting ingestion
  - Performs force merge after ingestion to remove deleted records permanently
  - Ensures efficient disk space utilization
  - Validates CSV file format and content before processing
  - Provides detailed validation logging for audit trail
  - Implements retry mechanism for failed batch operations

### 4. Index Management (`index_cleanup.py`)
- Provides common index cleanup functionality used by other jobs
- Validates index existence
- Checks for index aliases
- Safely deletes documents while preserving index structure
- Performs force merge to remove deleted documents
- Comprehensive logging of operations
- Safety Guardrails:
  - Validates if target index exists before cleanup
  - Aborts operation if index is serving live traffic (part of an alias)
  - Prevents accidental cleanup of production indices
  - Validates index health status before cleanup
  - Performs force merge to permanently remove deleted records
  - Optimizes disk space by removing soft-deleted documents
  - Validates index settings and mappings before operations
  - Provides detailed validation logging for audit trail
  - Implements rollback capability in case of failures
  - Ensures index structure integrity during cleanup
- Used by:
  - Bulk Data Ingestion for pre-ingestion cleanup
  - Reindexing for target index preparation
  - Alias Management for index validation
  - Provides consistent cleanup behavior across all operations

### 5. Base Manager (`opensearch_base_manager.py`)
- Provides common functionality for all OpenSearch operations
- Handles authentication and SSL configuration
- Implements shared methods:
  - `_create_auth_header`: Creates Basic Authentication header
  - `_verify_index_exists`: Checks if an index exists
  - `_get_index_count`: Retrieves document count from an index
  - `_check_index_aliases`: Validates index alias associations
- Manages common configurations:
  - SSL verification settings
  - Content type headers
  - Authentication headers
- Serves as the foundation for all OpenSearch operations
- Ensures consistent behavior across all manager classes

## Error Handling

The system includes comprehensive error handling:
- Validates environment variables
- Checks for required permissions
- Verifies index existence
- Validates document counts
- Handles network errors
- Provides detailed error messages

## Dead Letter Queue (DLQ) and Message Splitting

The system includes robust error reporting through SQS Dead Letter Queues (DLQ) with automatic message splitting for large payloads:

### DLQ Configuration
- Configure SQS DLQ ARN through environment variables
- Failed records are automatically sent to the configured DLQ
- Each error message includes:
  - Error details and messages
  - Source file information
  - Failed record data
  - Timestamp and metadata

### Message Splitting
When error payloads exceed SQS's 230 KB message size limit, the system automatically splits them:

1. **Size Calculation**:
   - Base payload size (metadata, headers)
   - Available space for records
   - Average record size estimation

2. **Splitting Process**:
   - Divides records into multiple messages
   - Each message includes:
     - Part number (message_part)
     - Total parts (total_parts)
     - Total records (total_records)
     - Timestamp
     - Original error information

3. **Message Structure**:
   ```json
   {
     "error_message": "Error details",
     "file_key": "source/file/path",
     "source": "file_source",
     "message_part": 1,
     "total_parts": 3,
     "total_records": 1000,
     "timestamp": "2024-03-21T10:00:00Z",
     "failed_records": [...]
   }
   ```

4. **Recovery Process**:
   - Messages can be reassembled using part numbers
   - Total records count ensures completeness
   - Timestamps help with message ordering
   - Original error context is preserved

## Logging

All operations are logged with:
- Timestamps
- Operation details
- Success/failure status
- Performance metrics
- Error details when applicable

## Performance Considerations

- Batch processing for efficient ingestion
- Configurable batch sizes
- Force merge operations for cleanup
- Retry mechanisms for failed operations
- Progress tracking and timing information

## Security

- SSL verification support
- Basic authentication
- Environment variable based credentials
- Safe index operations with validation

## Dependencies

- Python 3.x
- boto3 (for S3 operations)
- requests (for HTTP operations)
- python-dotenv (for environment variables)
- urllib3 (for SSL handling)

## Best Practices

1. Always validate indices before operations
2. Use appropriate batch sizes for your data
3. Monitor operation logs for issues
4. Keep environment variables secure
5. Regular index maintenance with force merge
6. Proper error handling and recovery

## Notes

- The system disables SSL verification by default (configurable)
- Document count threshold is configurable via environment variable
- All operations include validation steps
- Comprehensive logging for debugging and monitoring 