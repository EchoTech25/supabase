# app.py
# Import necessary libraries
from flask import Flask, jsonify
from supabase import create_client, Client
import os # To access environment variables

# Initialize Flask app
app = Flask(__name__)

# Define Supabase URL and Key from environment variables
# IMPORTANT: Never hardcode sensitive keys directly in your code, especially for deployment.
# Render allows you to set these as environment variables securely.
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# Ensure environment variables are set
if not SUPABASE_URL or not SUPABASE_KEY:
    print("Error: SUPABASE_URL or SUPABASE_KEY environment variables are not set.")
    print("Please configure them in your Render dashboard.")

@app.route('/')
def home():
    """
    Root endpoint to display a welcome message.
    """
    return "Welcome to the Supabase Connection Test App! Go to /test-connection to run the test."

@app.route('/test-connection')
def test_connection():
    """
    Endpoint to test the Supabase connection.
    It now attempts to query the 'auth.users' table, which exists in every Supabase project.
    """
    if not SUPABASE_URL or not SUPABASE_KEY:
        return jsonify({
            "status": "error",
            "message": "Supabase URL or Key not configured. Please set environment variables."
        }), 500

    print("Attempting to connect to Supabase...")
    supabase: Client = None
    result_message = ""
    status_code = 200 # Default to success

    try:
        # Create a Supabase client instance
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("Supabase client initialized successfully.")
        result_message += "Supabase client initialized successfully.\n"

        # Attempt a query to a table that exists in every Supabase project: 'auth.users'.
        # This will verify connectivity and basic authentication.
        # We limit to 1 record to avoid fetching sensitive data or large amounts of data.
        print("Attempting to query 'auth.users' table to verify connectivity...")
        response = supabase.table('users').select('*').limit(1).execute() # 'users' is the public name for 'auth.users'

        # Check the response. If 'data' is present, the query was successful,
        # even if no users exist (in which case data will be an empty list).
        if response.data is not None:
            result_message += f"Query to 'auth.users' executed successfully. Response data (first record): {response.data}\n"
            result_message += "Successfully connected to Supabase and performed a valid query!"
        else:
            # This case is less likely if the connection is truly successful,
            # but included for robustness.
            result_message += "Query to 'auth.users' executed, but no data returned or unexpected response structure.\n"
            result_message += "This indicates the connection to Supabase is likely successful, but check the response structure."

    except Exception as e:
        result_message = f"Failed to connect to Supabase or execute query. Error: {e}\n"
        result_message += "Please ensure your SUPABASE_URL and SUPABASE_KEY are correct, and check network connectivity."
        status_code = 500 # Indicate an internal server error

    return jsonify({
        "status": "success" if status_code == 200 else "error",
        "message": result_message
    }), status_code

if __name__ == '__main__':
    # Run the Flask app
    # In a production Render environment, Gunicorn or an equivalent WSGI server
    # will be used, so this block is primarily for local testing.
    app.run(debug=True, host='0.0.0.0', port=os.environ.get('PORT', 5000))
