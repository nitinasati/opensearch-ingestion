# OpenSearch Ingestion System

A Python-based system for ingesting data from S3 CSV files into OpenSearch with support for parallel processing.

## Features

- Efficient CSV file processing using pandas
- Parallel processing with configurable number of threads
- Batch processing for optimal performance
- Document count validation
- Comprehensive error handling and logging
- AWS S3 integration
- OpenSearch bulk ingestion

## Prerequisites

- Python 3.8+
- AWS credentials configured
- OpenSearch cluster access
- Required Python packages (install using `pip install -r requirements.txt`)

## Configuration

### Authentication Options

#### 1. OpenSearch Backend Role (Recommended)
The system supports OpenSearch backend role authentication using AWS IAM roles. This is the recommended approach for production environments.

1. Configure your OpenSearch domain to use IAM authentication
2. Set up an IAM role with appropriate permissions
3. Configure the following in your `.env` file:
```
OPENSEARCH_ENDPOINT=your_opensearch_endpoint
AWS_REGION=your_aws_region
USE_BACKEND_ROLE=true
```

Required IAM permissions:
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "es:ESHttp*",
                "es:DescribeDomain",
                "es:DescribeDomains",
                "es:DescribeDomainConfig",
                "es:ESHttp*",
                "es:UpdateDomainConfig"
            ],
            "Resource": "arn:aws:es:region:account:domain/domain-name"
        }
    ]
}
```

#### 2. Basic Authentication (Legacy)
For development or testing environments, you can use basic authentication:

1. Create a `.env` file in the project root with the following variables:
```
OPENSEARCH_ENDPOINT=your_opensearch_endpoint
OPENSEARCH_USERNAME=your_username
OPENSEARCH_PASSWORD=your_password
AWS_REGION=your_aws_region
USE_BACKEND_ROLE=false
```

## Usage

### Bulk Update Script

The `bulkupdate.py` script processes CSV files from S3 and ingests them into OpenSearch with parallel processing support.

```bash
python bulkupdate.py --bucket <bucket_name> --prefix <prefix> --index <index_name> [options]
```

#### Required Arguments:
- `--bucket`: S3 bucket name containing CSV files
- `--prefix`: S3 prefix to filter files
- `--index`: OpenSearch index name for ingestion

#### Optional Arguments:
- `--batch-size`: Number of documents to process in each batch (default: 10000)
- `--max-workers`: Maximum number of parallel threads for processing (default: 4)

#### Example:
```bash
python bulkupdate.py --bucket openlpocbucket --prefix opensearch/ --index my_index_primary --batch-size 1000 --max-workers 8
```

### Parallel Processing

The system now supports parallel processing of CSV files with the following features:
- Configurable number of worker threads
- Thread-safe document counting
- Queue-based batch processing
- Graceful worker shutdown
- Error handling for worker threads

The number of threads can be adjusted based on your system's capabilities and requirements. A higher number of threads may improve performance but will also increase memory usage and network connections.

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

Configure these in your `.env` file:

```
OPENSEARCH_ENDPOINT=/opensearch/endpoint
OPENSEARCH_USERNAME=/opensearch/username
OPENSEARCH_PASSWORD=/opensearch/password
AWS_REGION=us-east-1
INDEX_RECREATE_THRESHOLD=1000000  # Threshold for recreating index
LOG_LEVEL=INFO
LOG_FORMAT='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
```

## Error Handling

The system includes comprehensive error handling for:
- S3 access issues
- OpenSearch connection problems
- CSV parsing errors
- Document validation failures
- Worker thread exceptions

## Logging

Detailed logging is provided for:
- Processing progress
- Error messages
- Performance metrics
- Document counts
- Worker thread status

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## System Characteristics

### Load Type
- This system is designed for **full load** operations only
- It does not support delta/incremental loads
- Each ingestion operation completely refreshes the target index with new data
- The system follows a zero-downtime strategy to ensure data consistency during full refreshes

### Limitations
- No support for partial updates or incremental data ingestion
- Each ingestion requires processing the entire dataset from S3
- Historical data changes are not tracked or preserved
- Previous versions of records are overwritten during ingestion

### AWS IAM Requirements

The tool requires an IAM role with the following permissions:

1. **OpenSearch Permissions**:
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "es:ESHttp*"
            ],
            "Resource": [
                "arn:aws:es:region:account:domain/domain-name/*"
            ]
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
  ```

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