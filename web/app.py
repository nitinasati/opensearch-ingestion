from flask import Flask, render_template, request, jsonify
import sys
import os
import atexit
import signal
import logging
import json
import time
import requests
from opensearchpy.exceptions import OpenSearchException
import boto3
from botocore.exceptions import BotoCoreError, ClientError

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from opensearch_base_manager import OpenSearchBaseManager
import json


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Log startup message
logger.info("Application starting up...")

app = Flask(__name__)
opensearch_manager = OpenSearchBaseManager()

# Register cleanup function
def cleanup():
    logger.info("Cleaning up resources...")
    # Add any cleanup code here if needed

atexit.register(cleanup)

# Handle signals for graceful shutdown
def signal_handler(signum, frame):
    logger.info(f"Received signal {signum}")
    cleanup()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def verify_index():
    try:
        # Check if index exists
        response = opensearch_manager._make_request(
            method='GET',
            path='/smart_search_alias'
        )
        logger.info(f"Index verification response: {response}")
        return response.get('status') == 'success'
    except Exception as e:
        logger.error(f"Index verification error: {str(e)}", exc_info=True)
        return False

@app.route('/debug/mappings')
def get_mappings():
    try:
        response = opensearch_manager._make_request(
            method='GET',
            path='/smart_search_alias/_mapping'
        )
        logger.info(f"Mapping response: {response}")
        return jsonify(response.get('response').json())
    except Exception as e:
        logger.error(f"Error getting mappings: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/')
def index():
    logger.info("Rendering index page")
    if not verify_index():
        logger.error("Index verification failed")
        return "Error: smart_search_alias does not exist or is not accessible", 500
    return render_template('index.html')

@app.route('/api/search', methods=['POST'])
def search():
    logger.info("Received search request")
    if not verify_index():
        logger.error("Index verification failed during search")
        return jsonify({"error": "Index smart_search_alias does not exist or is not accessible"}), 500

    data = request.json
    logger.debug(f"Search parameters: {data}")
    
    search_params = {
        'memberId': data.get('memberId', ''),
        'firstName': data.get('firstName', ''),
        'lastName': data.get('lastName', ''),
        'memberStatus': data.get('memberStatus', ''),
        'state': data.get('state', ''),
        'fatherName': data.get('fatherName', ''),
        'email1': data.get('email1', ''),
        'dateOfBirth': data.get('dateOfBirth', '')
    }
    
    # Build the search query
    query = {
        "query": {
            "bool": {
                "must": []
            }
        },
        "size": 100
    }
    
    # Add search conditions for non-empty fields
    for field, value in search_params.items():
        if value:
            if field == 'dateOfBirth' and value.strip():
                # Handle date field with range query
                logger.info(f"Adding date of birth range filter: {value}")
                query["query"]["bool"]["must"].append({
                    "range": {
                        "dateOfBirth": {
                            "gte": value,
                            "lte": value
                        }
                    }
                })
            elif field != 'dateOfBirth':  # Skip empty dateOfBirth
                # Handle text fields with wildcard
                query["query"]["bool"]["must"].append({
                    "wildcard": {
                        field: {
                            "value": f"{value.lower()}*",
                            "case_insensitive": True
                        }
                    }
                })
    
    logger.info(f"Search query: {json.dumps(query, indent=2)}")
    
    try:
        response = opensearch_manager._make_request(
            method='POST',
            path=f'/member/_search',
            data=query
        )
        
        if response.get('status') == 'error':
            logger.error(f"Search error: {response.get('message')}")
            return jsonify({"error": response.get('message')}), 500
            
        response_data = response.get('response').json()
        logger.info(f"Search completed with {len(response_data.get('hits', {}).get('hits', []))} results")
        return jsonify(response_data)
    except Exception as e:
        logger.error(f"Search exception: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/api/autocomplete')
def autocomplete():
    query = request.args.get('query', '')
    logger.info(f"Received autocomplete request for query: {query}")
    
    if not query or len(query) < 3:
        logger.debug("Query too short for autocomplete")
        return jsonify([])
    
    try:
        # Check OpenSearch connection first
        try:
            health_check = opensearch_manager._make_request('GET', '/_cluster/health')
            logger.info(f"Health check response: {health_check}")
            if health_check['status'] == 'error':
                logger.error(f"OpenSearch health check failed: {health_check['message']}")
                return jsonify({"error": "OpenSearch service is not responding. Please try again later."}), 503
            logger.debug(f"OpenSearch health check successful: {health_check['response'].json()}")
        except Exception as e:
            logger.error(f"Failed to connect to OpenSearch: {str(e)}", exc_info=True)
            return jsonify({"error": "Cannot connect to OpenSearch service. Please check the connection."}), 503

        # Verify index exists
        try:
            if not opensearch_manager._verify_index_exists('smart_search_alias'):
                logger.error("Index 'smart_search_alias' does not exist or is not accessible")
                return jsonify({"error": "Search index is not available. Please contact support."}), 404
            logger.debug("Index verification successful")
        except Exception as e:
            logger.error(f"Index verification failed: {str(e)}", exc_info=True)
            return jsonify({"error": "Error verifying search index. Please contact support."}), 500
        
        # Load boosting values
        boosting_values = load_boosting_values()
        logger.info(f"Using boosting values: {boosting_values}")
        
        # Create the autocomplete query with boosting
        autocomplete_query = {
            "query": {
                "multi_match": {
                    "query": query,
                    "type": "bool_prefix",
                    "fields": [
                        f"firstName.suggest^{boosting_values.get('firstName', 9)}",
                        f"lastName.suggest^{boosting_values.get('lastName', 8)}",
                        f"memberId^{boosting_values.get('memberId', 10)}",
                        f"fatherName.suggest^{boosting_values.get('fatherName', 2)}",
                        f"email1.suggest^{boosting_values.get('email1', 1)}",
                        f"email2.suggest^{boosting_values.get('email2', 2)}",
                        f"phoneNumber1^{boosting_values.get('phoneNumber1', 1)}",
                        f"phoneNumber2^{boosting_values.get('phoneNumber2', 1)}",
                        f"addressLine1.suggest^{boosting_values.get('addressLine1', 1)}",
                        f"addressLine2.suggest^{boosting_values.get('addressLine2', 1)}",
                        f"city.suggest^{boosting_values.get('city', 1)}",
                        f"state.suggest^{boosting_values.get('state', 1)}",
                        f"zipcode.suggest^{boosting_values.get('zipcode', 1)}",
                        f"country.suggest^{boosting_values.get('country', 1)}",
                        f"policyNumber^{boosting_values.get('policyNumber', 2)}",
                        f"memberStatus^{boosting_values.get('memberStatus', 1)}",
                        f"employer_name.suggest^{boosting_values.get('employer_name', 8)}",
                        f"group_policy_number^{boosting_values.get('group_policy_number', 10)}",
                        f"policy_status^{boosting_values.get('policy_status', 2)}",
                        f"industry^{boosting_values.get('industry', 1)}"
                    ],
                    "operator": "or"
                }
            },
            "size": 10
        }
        
        logger.info(f"Autocomplete query: {json.dumps(autocomplete_query, indent=2)}")
        
        # Execute the query
        response = opensearch_manager._make_request(
            'POST',
            '/smart_search_alias/_search',
            data=autocomplete_query,
            headers={
                'Content-Type': 'application/json',
                'Cache-Control': 'no-cache, no-store, must-revalidate',
                'Pragma': 'no-cache',
                'Expires': '0'
            }
        )
        
        if response['status'] == 'error':
            logger.error(f"Error in autocomplete query: {response['message']}")
            return jsonify({"error": f"Search error: {response['message']}"}), 500
        
        response_data = response['response'].json()
        hits = response_data.get('hits', {}).get('hits', [])
        total_hits = response_data.get('hits', {}).get('total', {}).get('value', 0)
        logger.info(f"Final results: {total_hits} hits")
        
        suggestions = []
        for hit in hits:
            source = hit.get('_source', {})
            suggestion = {
                'value': source.get('memberId', ''),
                'label': f"{source.get('firstName', '')} {source.get('lastName', '')} ({source.get('memberId', '')})",
                'memberId': source.get('memberId', ''),
                'firstName': source.get('firstName', ''),
                'lastName': source.get('lastName', ''),
                'memberStatus': source.get('memberStatus', ''),
                'state': source.get('state', '')
            }
            suggestions.append(suggestion)
        
        # Add response headers to prevent caching
        response = jsonify(suggestions)
        response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '0'
        return response
        
    except Exception as e:
        logger.error(f"Unexpected error in autocomplete: {str(e)}", exc_info=True)
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500

@app.route('/api/default-search')
def default_search():
    """Default search endpoint to load initial records."""
    try:
        # Check if the index exists
        if not opensearch_manager._verify_index_exists('smart_search_alias'):
            logger.error("Index 'smart_search_alias' does not exist or is not accessible")
            return jsonify({"error": "Search index is not available. Please contact support."})
        
        # Get the index mapping
        try:
            mapping_response = opensearch_manager._make_request('GET', '/smart_search_alias/_mapping')
            if mapping_response['status'] == 'error':
                logger.error(f"Error getting mapping: {mapping_response['message']}")
                return jsonify({"error": "Error getting index mapping. Please contact support."})
            
            mapping = mapping_response['response'].json()
            logger.info(f"Index mapping: {json.dumps(mapping, indent=2)}")
            
            # Create a simple query with index filter
            default_query = {
                "query": {
                    "bool": {
                        "must": {
                            "match_all": {}
                        },
                        "filter": {
                            "term": {
                                "_index": "member"
                            }
                        }
                    }
                },
                "size": 10
            }
            
            logger.info(f"Default search query: {json.dumps(default_query, indent=2)}")
            
            # Execute the query
            response = opensearch_manager._make_request('POST', '/smart_search_alias/_search', default_query)
            
            # Log the response status and message
            logger.info(f"OpenSearch response status: {response.get('status')}")
            logger.info(f"OpenSearch response message: {response.get('message')}")
            
            # Check for errors
            if response['status'] == 'error':
                logger.error(f"Error in default search: {response['message']}")
                # Log the raw response if available
                if 'response' in response:
                    try:
                        logger.error(f"Raw error response: {response['response'].text}")
                    except:
                        pass
                return jsonify({"error": "Search service is temporarily unavailable. Please try again later."})
            
            # Get the response data
            response_data = response['response'].json()
            
            # Log the number of hits for debugging
            hits = response_data.get('hits', {}).get('hits', [])
            logger.debug(f"Default search returned {len(hits)} hits")
            
            return jsonify(response_data)
            
        except Exception as e:
            logger.error(f"Error getting mapping: {str(e)}", exc_info=True)
            return jsonify({"error": "Error getting index mapping. Please contact support."})
        
    except OpenSearchException as e:
        logger.error(f"OpenSearch exception in default search: {str(e)}", exc_info=True)
        return jsonify({"error": "Search service is temporarily unavailable. Please try again later."})
    except Exception as e:
        logger.error(f"Unexpected error in default search: {str(e)}", exc_info=True)
        return jsonify({"error": "An unexpected error occurred. Please try again later."})

# Constants
ALIASES_ENDPOINT = '/_aliases'
INDEX_NOT_EXIST_MESSAGE = 'Index does not exist'
BOOSTING_VALUES_FILE = 'boosting_values.json'

@app.route('/api/save-boosting', methods=['POST'])
def save_boosting():
    logger.info("Received request to save boosting values")
    try:
        boosting_values = request.json
        logger.debug(f"Received boosting values: {boosting_values}")
        
        # Validate boosting values
        for field, value in boosting_values.items():
            if not isinstance(value, (int, float)) or value < 1 or value > 10:
                logger.error(f"Invalid boosting value for {field}: {value}")
                return jsonify({"error": f"Invalid boosting value for {field}. Must be between 1 and 10."}), 400
        
        # Save to OpenSearch index
        doc_id = "boosting_values"  # Using a fixed document ID
        response = opensearch_manager._make_request(
            method='PUT',
            path=f'/smart_search_boosting/_doc/{doc_id}',
            data=boosting_values
        )
        
        if response.get('status') == 'error':
            logger.error(f"Error saving boosting values to OpenSearch: {response.get('message')}")
            return jsonify({"error": "Failed to save boosting values"}), 500
        
        logger.info("Boosting values saved successfully to OpenSearch")
        return jsonify({"message": "Boosting values saved successfully"})
        
    except Exception as e:
        logger.error(f"Error saving boosting values: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/api/load-boosting', methods=['GET'])
def load_boosting():
    logger.info("Received request to load boosting values")
    try:
        # Try to get boosting values from OpenSearch
        doc_id = "boosting_values"
        response = opensearch_manager._make_request(
            method='GET',
            path=f'/smart_search_boosting/_doc/{doc_id}'
        )
        
        if response.get('status') == 'success':
            boosting_values = response.get('response').json().get('_source', {})
            logger.debug(f"Loaded boosting values from OpenSearch: {boosting_values}")
            return jsonify(boosting_values)
        else:
            logger.info("No boosting values found in OpenSearch, returning defaults")
            default_values = {
                'memberId': 1,
                'firstName': 1,
                'lastName': 1,
                'memberStatus': 1,
                'state': 1,
                'fatherName': 1,
                'email1': 1,
                'fullName': 1,
                'employer_name': 1,
                'group_policy_number': 1,
                'policy_status': 1,
                'industry': 1
            }
            return jsonify(default_values)
            
    except Exception as e:
        logger.error(f"Error loading boosting values: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500

def load_boosting_values():
    try:
        # Try to get boosting values from OpenSearch
        doc_id = "boosting_values"
        response = opensearch_manager._make_request(
            method='GET',
            path=f'/smart_search_boosting/_doc/{doc_id}'
        )
        
        if response.get('status') == 'success':
            values = response.get('response').json().get('_source', {})
            logger.debug(f"Loaded boosting values from OpenSearch: {values}")
            return values
        else:
            logger.info("No boosting values found in OpenSearch, returning defaults")
            return {
                'memberId': 1,
                'firstName': 1,
                'lastName': 1,
                'memberStatus': 1,
                'state': 1,
                'fatherName': 1,
                'email1': 1,
                'fullName': 1,
                'employer_name': 1,
                'group_policy_number': 1,
                'policy_status': 1,
                'industry': 1
            }
    except Exception as e:
        logger.error(f"Error loading boosting values: {str(e)}")
        return {
            'memberId': 1,
            'firstName': 1,
            'lastName': 1,
            'memberStatus': 1,
            'state': 1,
            'fatherName': 1,
            'email1': 1,
            'fullName': 1,
            'employer_name': 1,
            'group_policy_number': 1,
            'policy_status': 1,
            'industry': 1
        }

@app.route('/api/combined-autocomplete')
def combined_autocomplete():
    query = request.args.get('query', '')
    logger.info(f"Received combined autocomplete request for query: {query}")
    
    if not query or len(query) < 3:
        logger.debug(f"Query too short for autocomplete: {len(query) if query else 0} characters")
        return jsonify([])
    
    try:
        # Load boosting values
        boosting_values = load_boosting_values()
        logger.info(f"Using boosting values: {boosting_values}")
        
        # Create the autocomplete query with boosting
        autocomplete_query = {
            "query": {
                "multi_match": {
                    "query": query,
                    "type": "bool_prefix",
                    "fields": [
                        f"firstName.suggest^{boosting_values.get('firstName', 9)}",
                        f"lastName.suggest^{boosting_values.get('lastName', 8)}",
                        f"memberId^{boosting_values.get('memberId', 10)}",
                        f"fatherName.suggest^{boosting_values.get('fatherName', 2)}",
                        f"email1.suggest^{boosting_values.get('email1', 1)}",
                        f"email2.suggest^{boosting_values.get('email2', 2)}",
                        f"phoneNumber1^{boosting_values.get('phoneNumber1', 1)}",
                        f"phoneNumber2^{boosting_values.get('phoneNumber2', 1)}",
                        f"addressLine1.suggest^{boosting_values.get('addressLine1', 1)}",
                        f"addressLine2.suggest^{boosting_values.get('addressLine2', 1)}",
                        f"city.suggest^{boosting_values.get('city', 1)}",
                        f"state.suggest^{boosting_values.get('state', 1)}",
                        f"zipcode.suggest^{boosting_values.get('zipcode', 1)}",
                        f"country.suggest^{boosting_values.get('country', 1)}",
                        f"policyNumber^{boosting_values.get('policyNumber', 2)}",
                        f"memberStatus^{boosting_values.get('memberStatus', 1)}",
                        f"employer_name.suggest^{boosting_values.get('employer_name', 8)}",
                        f"group_policy_number^{boosting_values.get('group_policy_number', 10)}",
                        f"policy_status^{boosting_values.get('policy_status', 2)}",
                        f"industry^{boosting_values.get('industry', 1)}"
                    ],
                    "operator": "or"
                }
            },
            "size": 10
        }

        
        # Execute the query
        response = opensearch_manager._make_request(
            'POST',
            '/smart_search_alias/_search',
            data=autocomplete_query
        )
        logger.info(f"Combined autocomplete query: {autocomplete_query}")
        if response['status'] == 'error':
            logger.error(f"Error in combined search: {response['message']}")
            return jsonify({"error": f"Search error: {response['message']}"}), 500
        
        response_data = response['response'].json()
        hits = response_data.get('hits', {}).get('hits', [])
        total_hits = response_data.get('hits', {}).get('total', {}).get('value', 0)
        logger.info(f"Combined search results: {total_hits} hits")
        
        suggestions = []
        for hit in hits:
            source = hit.get('_source', {})
            # Determine if it's a member or policy record
            is_member = 'memberId' in source
            is_policy = 'employer_id' in source
            
            if is_member:
                suggestion = {
                    'type': 'member',
                    'value': source.get('memberId', ''),
                    'label': f"{source.get('firstName', '')} {source.get('lastName', '')} ({source.get('memberId', '')})",
                    'id': source.get('memberId', ''),
                    'name': f"{source.get('firstName', '')} {source.get('lastName', '')}",
                    'status': source.get('memberStatus', ''),
                    'state': source.get('state', ''),
                    'email': source.get('email1', ''),
                    'memberId': source.get('memberId', ''),
                    'firstName': source.get('firstName', ''),
                    'lastName': source.get('lastName', ''),
                    'memberStatus': source.get('memberStatus', ''),
                    'state': source.get('state', ''),
                    'fatherName': source.get('fatherName', ''),
                    'email1': source.get('email1', ''),
                    'email2': source.get('email2', ''),
                    'phoneNumber1': source.get('phoneNumber1', ''),
                    'phoneNumber2': source.get('phoneNumber2', ''),
                    'addressLine1': source.get('addressLine1', ''),
                    'addressLine2': source.get('addressLine2', ''),
                    'city': source.get('city', ''),
                    'zipcode': source.get('zipcode', ''),
                    'country': source.get('country', ''),
                    'dateOfBirth': source.get('dateOfBirth', ''),
                    'gender': source.get('gender', ''),
                    'maritalStatus': source.get('maritalStatus', ''),
                    'groupId': source.get('groupId', ''),
                    'policyNumber': source.get('policyNumber', ''),
                    'created_at': source.get('created_at', ''),
                    'updated_at': source.get('updated_at', '')
                }
                logger.info(f"Created member suggestion: {json.dumps(suggestion, indent=2)}")
            elif is_policy:
                suggestion = {
                    'type': 'policy',
                    'value': source.get('employer_id', ''),
                    'label': f"{source.get('employer_name', '')} ({source.get('group_policy_number', '')})",
                    'id': source.get('employer_id', ''),
                    'name': source.get('employer_name', ''),
                    'status': source.get('policy_status', ''),
                    'policy_number': source.get('group_policy_number', ''),
                    'product_type': source.get('product_type', ''),
                    'broker': source.get('broker_name', ''),
                    'employer_id': source.get('employer_id', ''),
                    'employer_name': source.get('employer_name', ''),
                    'group_policy_number': source.get('group_policy_number', ''),
                    'policy_status': source.get('policy_status', ''),
                    'product_type': source.get('product_type', ''),
                    'broker_name': source.get('broker_name', ''),
                    'industry': source.get('industry', ''),
                    'policy_effective_date': source.get('policy_effective_date', ''),
                    'policy_end_date': source.get('policy_end_date', ''),
                    'created_at': source.get('created_at', ''),
                    'updated_at': source.get('updated_at', ''),
                    'coverage_classes': source.get('coverage_classes', [])
                }
                logger.info(f"Created policy suggestion: {json.dumps(suggestion, indent=2)}")
            else:
                logger.warning(f"Unknown record type: {json.dumps(source, indent=2)}")
                continue
                
            suggestions.append(suggestion)
        
        logger.info(f"Returning {len(suggestions)} suggestions")
        return jsonify(suggestions)
        
    except Exception as e:
        logger.error(f"Unexpected error in combined search: {str(e)}", exc_info=True)
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500

@app.route('/plan')
def plan():
    """Render the policy details page."""
    return render_template('plan.html')

@app.route('/member-details')
def member_details():
    """Render the member details page."""
    return render_template('member-details.html')

def fetch_member_by_id(member_id):
    query = {
        "query": {
            "term": {
                "memberId": member_id
            }
        },
        "size": 1
    }
    response = opensearch_manager._make_request(
        method='POST',
        path='/member/_search',
        data=query
    )
    if response.get('status') == 'error':
        raise Exception(response.get('message'))
    hits = response.get('response').json().get('hits', {}).get('hits', [])
    if not hits:
        return None
    return hits[0]['_source']

@app.route('/api/member/<member_id>', methods=['GET'])
def get_member_by_id(member_id):
    try:
        member_data = fetch_member_by_id(member_id)
        if not member_data:
            return jsonify({"error": "Member not found"}), 404
        return jsonify(member_data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/member-summary/<member_id>', methods=['GET'])
def get_member_summary(member_id):
    try:
        logger.info(f"Fetching member summary for member_id: {member_id}")
        member_data = fetch_member_by_id(member_id)
        logger.info(f"Member data: {member_data}")
        if not member_data:
            return jsonify({"error": "Member not found"}), 404

        # Only include the most relevant fields for summarization
        fields_to_include = [
            "memberId", "firstName", "lastName", "dateOfBirth", "gender", "maritalStatus",
            "employmentStatus", "memberStatus", "policyNumber", "coverageStartDate", "coverageEndDate",
            "groupId", "addressLine1", "city", "state", "country", "email1", "phoneNumber1"
        ]
        filtered_data = {k: v for k, v in member_data.items() if k in fields_to_include}

        # Format the prompt according to Claude's requirements
        system_prompt = (
            """
                    You are a helpful assistant that summarizes member profiles in a professional tone.

                    Instructions:

                    If the member's Date of Birth is within the next 30 days or occurred in the past 7 days from current date which is 18th May 2025, start the summary with a bolded alert in this format:
                    **Upcoming Birthday Alert: [Member Name]'s birthday is on [Month Day], [Birthday falls in X days / was X days ago].**

                    Follow the alert with a brief, formal message summarizing the member's profile, covering:

                    Full Name, Residence (City, State, Country)
                    Gender, Marital Status, Employment Status
                    Date of Birth
                    Contact Info (Phone, Email)
                    Policy Information: Policy Number, Coverage Start & End Dates, Status

                    Maintain a professional and business-friendly tone.

                    Use clear headings such as Basic Information, Personal Information, Contact Information, Address, Policy Details.

                    If any field is missing, state "N/A" instead of omitting it.
                    """
        )
        prompt = (
            f"Member Data in JSON:\n{json.dumps(filtered_data, indent=2)}\n\n"
            "Please provide a concise summary of this member's profile."
        )

        logger.info("Starting summary generation with prompt length: %d", len(prompt))

        # Prepare the request body for Claude 3 Messages API
        request_body = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 512,
            "temperature": 0.7,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": system_prompt + "\n\n" + prompt
                        }
                    ]
                }
            ]
        }

        bedrock = boto3.client("bedrock-runtime")
        response = bedrock.invoke_model_with_response_stream(
            body=json.dumps(request_body).encode('utf-8'),
            modelId="anthropic.claude-3-sonnet-20240229-v1:0"
        )

        # Process the streaming response and concatenate all text
        full_response = ""
        for event in response['body']:
            if 'chunk' in event:
                chunk_data = event['chunk']['bytes']
                chunk_json = json.loads(chunk_data.decode('utf-8'))
                if 'delta' in chunk_json and 'text' in chunk_json['delta']:
                    full_response += chunk_json['delta']['text']

        if not full_response:
            return jsonify({'error': 'No response from model'}), 500

        # Return the summary as plain text
        return jsonify({'summary': full_response})

    except (BotoCoreError, ClientError) as aws_err:
        logger.error(f"AWS error in get_member_summary: {str(aws_err)}", exc_info=True)
        return jsonify({"error": str(aws_err)}), 500
    except Exception as e:
        logger.error(f"Error in get_member_summary: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    # Use threaded=False to avoid the threading issue
    app.run(debug=True, threaded=False) 