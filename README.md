# OpenSearch Ingestion System

This system provides a comprehensive solution for managing OpenSearch indices, including data ingestion, reindexing, and cleanup operations. It supports bulk data ingestion from S3 CSV files with validation and error handling.

## AWS Parameter Store Integration

The system uses AWS Parameter Store to securely store and retrieve OpenSearch credentials. This provides better security and centralized credential management.

### Required AWS Parameter Store Parameters
```
/opensearch/endpoint    # OpenSearch cluster endpoint URL
/opensearch/username    # OpenSearch username
/opensearch/password    # OpenSearch password
```

### AWS IAM Requirements
- The system requires AWS IAM permissions to access Parameter Store:
  ```json
  {
      "Version": "2012-10-17",
      "Statement": [
          {
              "Effect": "Allow",
              "Action": [
                  "ssm:GetParameter",
                  "ssm:GetParameters"
              ],
              "Resource": [
                  "arn:aws:ssm:*:*:parameter/opensearch/*"
              ]
          }
      ]
  }
  ```

### Environment Variables
Required environment variables in `.env` file:
```
AWS_REGION=<your-aws-region>
DOCUMENT_COUNT_THRESHOLD=10  # Optional, defaults to 10%
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

### 2. Alias Management (`switch_alias.py`)
- Switches aliases between indices
- Validates document count differences
- Configurable threshold for document count differences
- Safe alias switching with validation

### 3. Bulk Data Ingestion (`bulkupdate.py`)
- Processes CSV files from S3 buckets
- Handles batch processing for efficient ingestion
- Supports custom batch sizes
- Validates document counts
- Provides detailed progress logging
- Handles various data types (numeric, boolean, text)

### 4. Index Management (`index_cleanup.py`)
- Validates index existence
- Checks for index aliases
- Safely deletes documents while preserving index structure
- Performs force merge to remove deleted documents
- Comprehensive logging of operations

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