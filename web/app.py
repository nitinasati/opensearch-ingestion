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
            path='/member_search_alias'
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
            path='/member_search_alias/_mapping'
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
        return "Error: member_search_alias does not exist or is not accessible", 500
    return render_template('index.html')

@app.route('/api/search', methods=['POST'])
def search():
    logger.info("Received search request")
    if not verify_index():
        logger.error("Index verification failed during search")
        return jsonify({"error": "Index member_search_alias does not exist or is not accessible"}), 500

    data = request.json
    logger.debug(f"Search parameters: {data}")
    
    search_params = {
        'memberId': data.get('memberId', ''),
        'firstName': data.get('firstName', ''),
        'lastName': data.get('lastName', ''),
        'memberStatus': data.get('memberStatus', ''),
        'state': data.get('state', ''),
        'fatherName': data.get('fatherName', ''),
        'email1': data.get('email1', '')
    }
    
    # Load boosting values
    boosting_values = load_boosting_values()
    logger.debug(f"Using boosting values: {boosting_values}")
    
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
            boost_value = boosting_values.get(field, 1.0)
            query["query"]["bool"]["must"].append({
                "term": {
                    field: {
                        "value": value,
                        "case_insensitive": True,
                        "boost": boost_value
                    }
                }
            })
    
    logger.info(f"Search query: {json.dumps(query, indent=2)}")
    
    try:
        response = opensearch_manager._make_request(
            method='POST',
            path=f'/member_index_primary/_search',
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
            if not opensearch_manager._verify_index_exists('member_search_alias'):
                logger.error("Index 'member_search_alias' does not exist or is not accessible")
                return jsonify({"error": "Search index is not available. Please contact support."}), 404
            logger.debug("Index verification successful")
        except Exception as e:
            logger.error(f"Index verification failed: {str(e)}", exc_info=True)
            return jsonify({"error": "Error verifying search index. Please contact support."}), 500
        
        # Load boosting values
        boosting_values = load_boosting_values()
        logger.info(f"Using boosting values: {boosting_values}")
        
        # Create the autocomplete query with wildcard search
        autocomplete_query = {
            "query": {
                "bool": {
                    "should": [
                        {
                            "wildcard": {
                                "fullName": {
                                    "value": f"*{query}*",
                                    "case_insensitive": True,
                                    "boost": boosting_values.get('fullName', 10)
                                }
                            }
                        },
                        {
                            "wildcard": {
                                "firstName": {
                                    "value": f"*{query}*",
                                    "case_insensitive": True,
                                    "boost": boosting_values.get('firstName', 9)
                                }
                            }
                        },
                        {
                            "wildcard": {
                                "lastName": {
                                    "value": f"*{query}*",
                                    "case_insensitive": True,
                                    "boost": boosting_values.get('lastName', 8)
                                }
                            }
                        },
                        {
                            "wildcard": {
                                "memberId": {
                                    "value": f"*{query}*",
                                    "case_insensitive": True,
                                    "boost": boosting_values.get('memberId', 10)
                                }
                            }
                        },
                        {
                            "wildcard": {
                                "fatherName": {
                                    "value": f"*{query}*",
                                    "case_insensitive": True,
                                    "boost": boosting_values.get('fatherName', 2)
                                }
                            }
                        },
                        {
                            "wildcard": {
                                "email1": {
                                    "value": f"*{query}*",
                                    "case_insensitive": True,
                                    "boost": boosting_values.get('email1', 1)
                                }
                            }
                        },
                        {
                            "wildcard": {
                                "email2": {
                                    "value": f"*{query}*",
                                    "case_insensitive": True,
                                    "boost": boosting_values.get('email2', 2)
                                }
                            }
                        },
                        {
                            "wildcard": {
                                "phoneNumber1": {
                                    "value": f"*{query}*",
                                    "case_insensitive": True,
                                    "boost": boosting_values.get('phoneNumber1', 1)
                                }
                            }
                        },
                        {
                            "wildcard": {
                                "phoneNumber2": {
                                    "value": f"*{query}*",
                                    "case_insensitive": True,
                                    "boost": boosting_values.get('phoneNumber2', 1)
                                }
                            }
                        },
                        {
                            "wildcard": {
                                "addressLine1": {
                                    "value": f"*{query}*",
                                    "case_insensitive": True,
                                    "boost": boosting_values.get('addressLine1', 1)
                                }
                            }
                        },
                        {
                            "wildcard": {
                                "addressLine2": {
                                    "value": f"*{query}*",
                                    "case_insensitive": True,
                                    "boost": boosting_values.get('addressLine2', 1)
                                }
                            }
                        },
                        {
                            "wildcard": {
                                "city": {
                                    "value": f"*{query}*",
                                    "case_insensitive": True,
                                    "boost": boosting_values.get('city', 1)
                                }
                            }
                        },
                        {
                            "wildcard": {
                                "state": {
                                    "value": f"*{query}*",
                                    "case_insensitive": True,
                                    "boost": boosting_values.get('state', 1)
                                }
                            }
                        },
                        {
                            "wildcard": {
                                "zipcode": {
                                    "value": f"*{query}*",
                                    "case_insensitive": True,
                                    "boost": boosting_values.get('zipcode', 1)
                                }
                            }
                        },
                        {
                            "wildcard": {
                                "country": {
                                    "value": f"*{query}*",
                                    "case_insensitive": True,
                                    "boost": boosting_values.get('country', 1)
                                }
                            }
                        },
                        {
                            "wildcard": {
                                "policyNumber": {
                                    "value": f"*{query}*",
                                    "case_insensitive": True,
                                    "boost": boosting_values.get('policyNumber', 2)
                                }
                            }
                        },
                        {
                            "wildcard": {
                                "memberStatus": {
                                    "value": f"*{query}*",
                                    "case_insensitive": True,
                                    "boost": boosting_values.get('memberStatus', 1)
                                }
                            }
                        }
                    ],
                    "minimum_should_match": 1,
                    "filter": {
                        "term": {
                            "_index": "member_index_primary"
                        }
                    }
                }
            },
            "size": 10
        }
        
        logger.info(f"Autocomplete query: {json.dumps(autocomplete_query, indent=2)}")
        
        # Execute the query
        response = opensearch_manager._make_request(
            'POST',
            '/member_search_alias/_search',
            data=autocomplete_query,
            headers={'Content-Type': 'application/json'}
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
        
        return jsonify(suggestions)
        
    except Exception as e:
        logger.error(f"Unexpected error in autocomplete: {str(e)}", exc_info=True)
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500

@app.route('/api/default-search')
def default_search():
    """Default search endpoint to load initial records."""
    try:
        # Check if the index exists
        if not opensearch_manager._verify_index_exists('member_search_alias'):
            logger.error("Index 'member_search_alias' does not exist or is not accessible")
            return jsonify({"error": "Search index is not available. Please contact support."})
        
        # Get the index mapping
        try:
            mapping_response = opensearch_manager._make_request('GET', '/member_search_alias/_mapping')
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
                                "_index": "member_index_primary"
                            }
                        }
                    }
                },
                "size": 10
            }
            
            logger.info(f"Default search query: {json.dumps(default_query, indent=2)}")
            
            # Execute the query
            response = opensearch_manager._make_request('POST', '/member_search_alias/_search', default_query)
            
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
        
        with open(BOOSTING_VALUES_FILE, 'w') as f:
            json.dump(boosting_values, f)
        
        logger.info("Boosting values saved successfully")
        return jsonify({"message": "Boosting values saved successfully"})
        
    except Exception as e:
        logger.error(f"Error saving boosting values: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/api/load-boosting', methods=['GET'])
def load_boosting():
    logger.info("Received request to load boosting values")
    try:
        if os.path.exists(BOOSTING_VALUES_FILE):
            with open(BOOSTING_VALUES_FILE, 'r') as f:
                boosting_values = json.load(f)
            logger.debug(f"Loaded boosting values: {boosting_values}")
            return jsonify(boosting_values)
        else:
            logger.info("No boosting values file found, returning defaults")
            default_values = {
                'memberId': 1.0,
                'firstName': 1.0,
                'lastName': 1.0,
                'memberStatus': 1.0,
                'state': 1.0,
                'fatherName': 1.0,
                'email1': 1.0
            }
            return jsonify(default_values)
            
    except Exception as e:
        logger.error(f"Error loading boosting values: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500

def load_boosting_values():
    try:
        if os.path.exists(BOOSTING_VALUES_FILE):
            with open(BOOSTING_VALUES_FILE, 'r') as f:
                values = json.load(f)
            logger.debug(f"Loaded boosting values: {values}")
            return values
        else:
            logger.info("No boosting values file found, returning defaults")
            return {
                'memberId': 1.0,
                'firstName': 1.0,
                'lastName': 1.0,
                'memberStatus': 1.0,
                'state': 1.0,
                'fatherName': 1.0,
                'email1': 1.0,
                'fullName': 1.0
            }
    except Exception as e:
        logger.error(f"Error loading boosting values: {str(e)}", exc_info=True)
        return {
            'memberId': 1.0,
            'firstName': 1.0,
            'lastName': 1.0,
            'memberStatus': 1.0,
            'state': 1.0,
            'fatherName': 1.0,
            'email1': 1.0,
            'fullName': 1.0
        }

@app.route('/api/combined-autocomplete')
def combined_autocomplete():
    query = request.args.get('query', '')
    logger.info(f"Received combined autocomplete request for query: {query}")
    
    if not query or len(query) < 3:
        logger.debug(f"Query too short for autocomplete: {len(query) if query else 0} characters")
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
            if not opensearch_manager._verify_index_exists('member_search_alias'):
                logger.error("Index 'member_search_alias' does not exist or is not accessible")
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
                    "query": f"*{query}*",
                    "type": "best_fields",
                    "fields": [
                        # Member fields
                        f"fullName^{boosting_values.get('fullName', 10)}",
                        f"firstName^{boosting_values.get('firstName', 9)}",
                        f"lastName^{boosting_values.get('lastName', 8)}",
                        f"memberId^{boosting_values.get('memberId', 10)}",
                        f"fatherName^{boosting_values.get('fatherName', 2)}",
                        f"email1^{boosting_values.get('email1', 2)}",
                        f"email2^{boosting_values.get('email2', 2)}",
                        f"phoneNumber1^{boosting_values.get('phoneNumber1', 1)}",
                        f"phoneNumber2^{boosting_values.get('phoneNumber2', 1)}",
                        f"addressLine1^{boosting_values.get('addressLine1', 1)}",
                        f"addressLine2^{boosting_values.get('addressLine2', 1)}",
                        f"city^{boosting_values.get('city', 1)}",
                        f"state^{boosting_values.get('state', 1)}",
                        f"zipcode^{boosting_values.get('zipcode', 1)}",
                        f"country^{boosting_values.get('country', 1)}",
                        f"memberStatus^{boosting_values.get('memberStatus', 1)}",
                        # Policy fields
                        f"employer_id^{boosting_values.get('employer_id', 10)}",
                        f"employer_name^{boosting_values.get('employer_name', 8)}",
                        f"group_policy_number^{boosting_values.get('group_policy_number', 10)}",
                        f"policy_status^{boosting_values.get('policy_status', 2)}",
                        f"product_type^{boosting_values.get('product_type', 2)}",
                        f"broker_name^{boosting_values.get('broker_name', 2)}",
                        f"industry^{boosting_values.get('industry', 1)}"
                    ]
                }
            },
            "size": 10
        }
        
        # Execute the query
        response = opensearch_manager._make_request(
            'POST',
            '/member_search_alias/_search',
            data=autocomplete_query
        )
        
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

if __name__ == '__main__':
    # Use threaded=False to avoid the threading issue
    app.run(debug=True, threaded=False) 