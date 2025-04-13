from flask import Flask, render_template, request, jsonify
import sys
import os
import atexit
import signal
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from opensearch_base_manager import OpenSearchBaseManager
import json

app = Flask(__name__)
opensearch_manager = OpenSearchBaseManager()

# Register cleanup function
def cleanup():
    print("Cleaning up resources...")
    # Add any cleanup code here if needed

atexit.register(cleanup)

# Handle signals for graceful shutdown
def signal_handler(sig, frame):
    print("Shutting down gracefully...")
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
        return response.get('status') == 'success'
    except Exception as e:
        logger.error(f"Index verification error: {str(e)}")
        return False

@app.route('/debug/mappings')
def get_mappings():
    try:
        response = opensearch_manager._make_request(
            method='GET',
            path='/member_search_alias/_mapping'
        )
        return jsonify(response.get('response').json())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/')
def index():
    if not verify_index():
        return "Error: member_search_alias does not exist or is not accessible", 500
    return render_template('index.html')

@app.route('/api/search', methods=['POST'])
def search():
    if not verify_index():
        return jsonify({"error": "Index member_search_alias does not exist or is not accessible"}), 500

    data = request.json
    search_params = {
        'memberId': data.get('memberId', ''),
        'firstName': data.get('firstName', ''),
        'lastName': data.get('lastName', ''),
        'memberStatus': data.get('memberStatus', ''),
        'state': data.get('state', ''),
        'fatherName': data.get('fatherName', ''),
        'email1': data.get('email1', '')
    }
    
    # Build the search query
    query = {
        "query": {
            "bool": {
                "must": []
            }
        },
        "size": 100  # Add a size limit
    }
    
    # Add search conditions for non-empty fields
    for field, value in search_params.items():
        if value:
            # Use term query with case-insensitive matching
            query["query"]["bool"]["must"].append({
                "term": {
                    field: {
                        "value": value,
                        "case_insensitive": True
                    }
                }
            })
    
    # Execute search
    try:
        logger.debug(f"Search query: {json.dumps(query)}")
        response = opensearch_manager._make_request(
            method='POST',
            path=f'/member_search_alias/_search',
            data=query
        )
        
        if response.get('status') == 'error':
            logger.error(f"Search error: {response.get('message')}")
            return jsonify({"error": response.get('message')}), 500
            
        # Extract the JSON from the response object
        response_data = response.get('response').json()
        logger.debug(f"Search response: {json.dumps(response_data)}")
        return jsonify(response_data)
    except Exception as e:
        logger.error(f"Search exception: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/autocomplete')
def autocomplete():
    """Autocomplete endpoint for member search."""
    query = request.args.get('query', '')
    if not query or len(query) < 3:
        return jsonify([])
    
    try:
        # Check if the index exists
        if not opensearch_manager._verify_index_exists('member_search_alias'):
            logger.error("Index 'member_search_alias' does not exist or is not accessible")
            return jsonify({"error": "Search index is not available. Please contact support."})
        
        # Log the autocomplete query for debugging
        logger.debug(f"Autocomplete query: {query}")
        
        # Create a query_string query with wildcard
        autocomplete_query = {
            "query": {
                "query_string": {
                    "query": f"{query}*",
                    "fields": ["*"],
                    "default_operator": "and"
                }
            },
            "size": 10
        }
        
        # Log the query for debugging
        logger.debug(f"Autocomplete query: {json.dumps(autocomplete_query)}")
        
        # Execute the query
        response = opensearch_manager._make_request('POST', '/member_search_alias/_search', autocomplete_query)
        
        # Check for errors
        if response['status'] == 'error':
            logger.error(f"Error in autocomplete query: {response['message']}")
            return jsonify({"error": "Search service is temporarily unavailable. Please try again later."})
        
        # Get the response data
        response_data = response['response'].json()
        
        # Log the number of hits for debugging
        hits = response_data.get('hits', {}).get('hits', [])
        logger.debug(f"Autocomplete returned {len(hits)} hits")
        
        # Format the suggestions
        suggestions = []
        for hit in hits:
            source = hit.get('_source', {})
            suggestions.append({
                'value': source.get('memberId', ''),
                'label': f"{source.get('firstName', '')} {source.get('lastName', '')} ({source.get('memberId', '')})",
                'memberId': source.get('memberId', ''),
                'firstName': source.get('firstName', ''),
                'lastName': source.get('lastName', ''),
                'memberStatus': source.get('memberStatus', ''),
                'state': source.get('state', '')
            })
        
        # Log the output for debugging
        logger.debug(f"Autocomplete output: {json.dumps(suggestions)}")
        
        return jsonify(suggestions)
        
    except OpenSearchException as e:
        logger.error(f"OpenSearch exception in autocomplete: {str(e)}")
        return jsonify({"error": "Search service is temporarily unavailable. Please try again later."})
    except Exception as e:
        logger.error(f"Unexpected error in autocomplete: {str(e)}")
        return jsonify({"error": "An unexpected error occurred. Please try again later."})

@app.route('/api/default-search')
def default_search():
    """Default search endpoint to load initial records."""
    try:
        # Check if the index exists
        if not opensearch_manager._verify_index_exists('member_search_alias'):
            logger.error("Index 'member_search_alias' does not exist or is not accessible")
            return jsonify({"error": "Search index is not available. Please contact support."})
        
        # Create a simple query to get the first 10 records
        default_query = {
            "query": {
                "match_all": {}
            },
            "size": 10,
            "sort": [
                { "firstName": { "order": "asc" } }
            ]
        }
        
        # Execute the query
        response = opensearch_manager._make_request('POST', '/member_search_alias/_search', default_query)
        
        # Check for errors
        if response['status'] == 'error':
            logger.error(f"Error in default search: {response['message']}")
            return jsonify({"error": "Search service is temporarily unavailable. Please try again later."})
        
        # Get the response data
        response_data = response['response'].json()
        
        # Log the number of hits for debugging
        hits = response_data.get('hits', {}).get('hits', [])
        logger.debug(f"Default search returned {len(hits)} hits")
        
        return jsonify(response_data)
        
    except OpenSearchException as e:
        logger.error(f"OpenSearch exception in default search: {str(e)}")
        return jsonify({"error": "Search service is temporarily unavailable. Please try again later."})
    except Exception as e:
        logger.error(f"Unexpected error in default search: {str(e)}")
        return jsonify({"error": "An unexpected error occurred. Please try again later."})

if __name__ == '__main__':
    # Use threaded=False to avoid the threading issue
    app.run(debug=True, threaded=False) 