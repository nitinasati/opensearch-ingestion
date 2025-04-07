# Data Ingestion Requirements

## Attribute Matching

When ingesting data into OpenSearch indices, the following requirements must be met:

1. **JSON Data Ingestion**:
   - The attributes in the ingested JSON must match the mapping defined in the OpenSearch index
   - Field names are case-sensitive and must match exactly
   - Data types must be compatible with the field types defined in the index mapping
   - Example:
     ```json
     // Index mapping
     {
       "mappings": {
         "properties": {
           "user_id": { "type": "keyword" },
           "name": { "type": "text" },
           "age": { "type": "integer" },
           "created_at": { "type": "date" }
         }
       }
     }
     
     // Valid JSON for ingestion
     {
       "user_id": "12345",
       "name": "John Doe",
       "age": 30,
       "created_at": "2023-01-15T12:00:00Z"
     }
     ```

2. **CSV Data Ingestion**:
   - Column names in the CSV file must match the field names in the OpenSearch index mapping
   - Column names are case-sensitive and must match exactly
   - Data types must be compatible with the field types defined in the index mapping
   - Example:
     ```
     // CSV header
     user_id,name,age,created_at
     
     // Index mapping
     {
       "mappings": {
         "properties": {
           "user_id": { "type": "keyword" },
           "name": { "type": "text" },
           "age": { "type": "integer" },
           "created_at": { "type": "date" }
         }
       }
     }
     ```

## Delta Updates

OpenSearch supports delta updates when the source data contains a unique identifier field:

1. **Unique ID Field**:
   - If the source data contains a field that can uniquely identify records (e.g., `id`, `user_id`, `document_id`), this field can be used for delta updates
   - The unique ID field must be mapped as a `keyword` type in the OpenSearch index
   - Example mapping with ID field:
     ```json
     {
       "mappings": {
         "properties": {
           "id": { "type": "keyword" },
           "user_id": { "type": "keyword" },
           "name": { "type": "text" },
           "age": { "type": "integer" },
           "created_at": { "type": "date" }
         }
       }
     }
     ```

2. **Update Existing Records**:
   - When ingesting data with a unique ID that already exists in the index, the record will be updated instead of creating a duplicate
   - This is handled automatically by the ingestion process when the ID field is properly configured
   - Example JSON for updating an existing record:
     ```json
     {
       "id": "12345",
       "user_id": "user123",
       "name": "John Doe Updated",
       "age": 31,
       "created_at": "2023-01-15T12:00:00Z"
     }
     ```

3. **Partial Updates**:
   - Delta updates can include only the fields that have changed, not the entire record
   - Fields not included in the update will retain their previous values
   - Example partial update:
     ```json
     {
       "id": "12345",
       "age": 32
     }
     ```
     This will update only the `age` field for the record with `id: "12345"`.

## Data Type Compatibility

Ensure that the data types in your source data are compatible with the field types in your OpenSearch index:

| OpenSearch Type | Compatible JSON/CSV Types |
|----------------|---------------------------|
| keyword        | String                    |
| text           | String                    |
| long           | Integer, Long             |
| integer        | Integer                   |
| short          | Integer                   |
| byte           | Integer                   |
| double         | Float, Double             |
| float          | Float                     |
| date           | ISO 8601 date string      |
| boolean        | Boolean                   |
| object         | JSON object               |
| nested         | JSON array of objects     |
| geo_point      | lat/lon object or string  |
| ip             | IP address string         |

## Best Practices

1. **Schema Validation**:
   - Validate your data against the index mapping before ingestion
   - Use tools like JSON Schema to ensure data compatibility

2. **Bulk Ingestion**:
   - Use bulk ingestion for better performance when ingesting large datasets
   - Recommended batch size: 1000-5000 documents per bulk request

3. **Error Handling**:
   - Implement proper error handling for failed ingestions
   - Log failed documents for later reprocessing

4. **Index Aliases**:
   - Use index aliases to support zero-downtime reindexing
   - Switch aliases after successful data ingestion

5. **Monitoring**:
   - Monitor ingestion performance and error rates
   - Set up alerts for failed ingestions 