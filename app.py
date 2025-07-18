# app.py
# Import necessary libraries
from flask import Flask, jsonify
from supabase import create_client, Client
import os
from datetime import datetime, timedelta
import time
import re
import yfinance as yf # New dependency for financial data
import pandas as pd   # New dependency for data manipulation

# Initialize Flask app
app = Flask(__name__)

# --- Supabase Configuration (USING PROVIDED KEYS DIRECTLY) ---
# It's best practice to use environment variables for sensitive keys in production.
# Render will provide these as environment variables.
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://irsnfypvdtrihpcveeus.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "sb_secret_uAdt1zgtnKIcHdkJc0QCyg_b2SPqeOd")

# Initialize Supabase client globally (will be set in __main__ or on first use)
supabase: Client = None

# --- Configuration for ticker file and dates ---
# Assuming tickers_to_use.txt is in the root of your deployed repository
TICKER_FILE_PATH = "tickers_to_use.txt"
END_DATE_PRICES = "2025-07-18" # Consistent with current time context
START_DATE_PRICES = (datetime.strptime(END_DATE_PRICES, '%Y-%m-%d') - timedelta(days=365*5)).strftime('%Y-%m-%d')

# --- Rate Limiting Settings ---
REQUEST_DELAY_SECONDS = 2
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 5

# --- Helper function to clean column names (from your original script) ---
def clean_column_name(name):
    name = str(name).lower()
    name = re.sub(r'[^a-z0-9_]+', '_', name)
    name = re.sub(r'_+', '_', name)
    name = name.strip('_')
    return name

# --- Ticker Fetching Function (adapted for Render file path) ---
def get_asx_tickers_from_file(file_path):
    """
    Loads ASX tickers from a file. Assumes the file is in the same directory
    as the script on Render.
    """
    print(f"Attempting to load ASX tickers from file: {file_path}")
    tickers = []
    try:
        # Use a more robust path for Render environment
        current_dir = os.path.dirname(os.path.abspath(__file__))
        full_file_path = os.path.join(current_dir, file_path)
        
        with open(full_file_path, 'r') as f:
            for line in f:
                ticker = line.strip()
                if ticker:
                    if not ticker.upper().endswith('.AX'):
                        ticker = f"{ticker.upper()}.AX"
                    else:
                        ticker = ticker.upper()
                    tickers.append(ticker)
        print(f"Loaded {len(tickers)} tickers from the file.")
        return tickers
    except FileNotFoundError:
        print(f"Error: Ticker file not found at {full_file_path}")
        print("Returning an empty list. Please ensure 'tickers_to_use.txt' is in your GitHub repo root.")
        return []
    except Exception as e:
        print(f"An unexpected error occurred while reading the ticker file: {e}")
        return []

# --- Dynamic Data Preparation for Financial Statements (from your original script) ---
def prepare_dynamic_financial_data(df, security_id, fiscal_quarter=None):
    """
    Dynamically prepares a financial DataFrame for insertion into Supabase,
    using yfinance's own line item names as column names.
    """
    prepared_records = []
    if df.empty:
        return prepared_records

    df_transposed = df.transpose() # Transpose to make periods rows, line items columns

    for index, row in df_transposed.iterrows():
        record = {}
        record['security_id'] = str(security_id)
        record['report_date'] = index.date() # Get just the date part
        record['fiscal_year'] = index.year
        record['fiscal_quarter'] = fiscal_quarter # None for annual reports

        for col_name, value in row.items():
            cleaned_col_name = clean_column_name(col_name)
            if pd.isna(value):
                record[cleaned_col_name] = None
            elif isinstance(value, (pd.Timestamp, datetime)):
                record[cleaned_col_name] = value.date()
            elif isinstance(value, (int, float, bool)):
                record[cleaned_col_name] = value
            else:
                record[cleaned_col_name] = str(value)

        prepared_records.append(record)
    return prepared_records

# --- Data Upload Functions (from your original script) ---
def upload_data_to_supabase(table_name, data_records, on_conflict_cols):
    """
    Generic function to upload data records to a specified Supabase table.
    Handles upserting based on conflict columns.
    """
    if not data_records:
        print(f"No data to upload for {table_name}.")
        return {"status": "skipped", "message": f"No data to upload for {table_name}."}

    try:
        response = supabase.table(table_name).upsert(
            data_records,
            on_conflict=','.join(on_conflict_cols)
        ).execute()

        if response.data:
            print(f"Successfully uploaded {len(response.data)} records to {table_name}.")
            return {"status": "success", "message": f"Successfully uploaded {len(response.data)} records to {table_name}."}
        elif response.error:
            print(f"Failed to upload data to {table_name}: {response.error}")
            return {"status": "error", "message": f"Failed to upload data to {table_name}: {response.error}"}
        else:
            print(f"Upload to {table_name} completed with no data returned (might be no changes).")
            return {"status": "success", "message": f"Upload to {table_name} completed with no data returned (might be no changes)."}

    except Exception as e:
        print(f"An error occurred during upload to {table_name}: {e}")
        return {"status": "error", "message": f"An error occurred during upload to {table_name}: {e}"}

# --- Flask Routes ---
@app.route('/')
def home():
    """
    Root endpoint to display a welcome message.
    """
    return "Welcome to the Supabase Data Ingestion Test App! Go to /run-ingestion to start."

@app.route('/run-ingestion')
def run_ingestion():
    """
    Endpoint to trigger the data ingestion process.
    This will fetch data for all tickers in tickers_to_use.txt,
    skipping those it cannot find, and upload it to Supabase.
    """
    global supabase # Access the global supabase client

    # Initialize Supabase client if not already initialized
    if supabase is None:
        try:
            supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
            print("Supabase client initialized.")
        except Exception as e:
            error_msg = f"Error initializing Supabase client: {e}"
            print(error_msg)
            return jsonify({"status": "error", "message": error_msg}), 500

    ingestion_results = []
    skipped_tickers = []
    successful_tickers = []
    partially_failed_tickers = [] # Track tickers where core data worked, but financials failed

    all_asx_tickers = get_asx_tickers_from_file(TICKER_FILE_PATH)

    if not all_asx_tickers:
        msg = "No tickers loaded from file. Please ensure 'tickers_to_use.txt' is in your repo and contains tickers."
        print(msg)
        return jsonify({"status": "error", "message": msg}), 400

    # --- Iterate through all tickers ---
    for ticker_symbol in all_asx_tickers:
        ingestion_results.append(f"\n--- Processing Ticker: {ticker_symbol} ---")
        print(f"\n--- Processing Ticker: {ticker_symbol} ---")

        # --- Step 0: Ensure companies and securities exist in core tables ---
        retries = 0
        company_id = None
        security_id = None
        ticker_obj = None
        core_data_success = False # Flag to track if core data was successfully fetched/upserted

        while retries < MAX_RETRIES:
            try:
                ticker_obj = yf.Ticker(ticker_symbol)
                info = ticker_obj.info

                # Check if info is empty, which can happen for invalid tickers
                if not info or not info.get('longName'): # Check for longName as a basic indicator of valid info
                    raise ValueError(f"Could not fetch info for ticker {ticker_symbol}. It might be invalid or delisted.")

                company_data = {
                    'ticker': ticker_symbol,
                    'company_name': info.get('longName') or info.get('shortName'),
                    'exchange': info.get('exchange'),
                    'sector': info.get('sector'),
                    'industry': info.get('industry'),
                    'country': info.get('country'),
                    'website': info.get('website'),
                    'description': info.get('longBusinessSummary')
                }
                company_resp = supabase.table('core.companies').upsert(company_data, on_conflict='ticker').execute()
                company_id = company_resp.data[0]['id'] if company_resp.data else None

                if not company_id:
                    raise Exception(f"Failed to get company_id for {ticker_symbol} after upsert.")

                security_data = {
                    'company_id': str(company_id),
                    'symbol': ticker_symbol,
                    'security_type': 'COMMON_STOCK',
                    'currency': info.get('currency'),
                }
                security_resp = supabase.table('core.securities').upsert(security_data, on_conflict='symbol').execute()
                security_id = security_resp.data[0]['id'] if security_resp.data else None

                if not security_id:
                    raise Exception(f"Failed to get security_id for {ticker_symbol} after upsert.")

                msg = f"Ensured core data for {ticker_symbol} (Company ID: {company_id}, Security ID: {security_id})."
                ingestion_results.append(msg)
                print(msg)
                core_data_success = True
                break # Exit retry loop on success
            except Exception as e:
                msg = f"Error ensuring core data for {ticker_symbol}: {e}"
                ingestion_results.append(msg)
                print(msg)
                retries += 1
                time.sleep(RETRY_DELAY_SECONDS)
        
        if not core_data_success: # If core data failed after all retries
            msg = f"Failed to ensure core data for {ticker_symbol} after {MAX_RETRIES} retries. Skipping this ticker."
            ingestion_results.append(msg)
            print(msg)
            skipped_tickers.append(ticker_symbol)
            continue # Move to the next ticker in the list

        # --- Fetch and Upload Financial Statements (only if core data was successful) ---
        ingestion_results.append(f"\nFetching financial statements for {ticker_symbol}...")
        print(f"\nFetching financial statements for {ticker_symbol}...")
        
        ticker_financials_failed = False # Flag to track if any financial statement failed for this ticker

        # --- Income Statement (Annual) ---
        ingestion_results.append("\n--- Processing Income Statement (Annual) ---")
        print("\n--- Processing Income Statement (Annual) ---")
        retries = 0
        while retries < MAX_RETRIES:
            try:
                income_stmt_annual = ticker_obj.financials
                income_stmt_records = prepare_dynamic_financial_data(income_stmt_annual, security_id, fiscal_quarter=None)
                upload_res = upload_data_to_supabase('financials.income_statements_annual', income_stmt_records, ['security_id', 'fiscal_year', 'fiscal_quarter'])
                ingestion_results.append(upload_res['message'])
                if upload_res['status'] == 'error':
                    ticker_financials_failed = True
                break
            except Exception as e:
                msg = f"Error fetching/uploading Income Statement for {ticker_symbol}: {e}"
                ingestion_results.append(msg)
                print(msg)
                retries += 1
                time.sleep(RETRY_DELAY_SECONDS)
        if retries == MAX_RETRIES:
            msg = f"Failed to fetch/upload Income Statement for {ticker_symbol} after {MAX_RETRIES} retries."
            ingestion_results.append(msg)
            print(msg)
            ticker_financials_failed = True
        time.sleep(REQUEST_DELAY_SECONDS) # Delay for rate limiting

        # --- Balance Sheet (Annual) ---
        ingestion_results.append("\n--- Processing Balance Sheet (Annual) ---")
        print("\n--- Processing Balance Sheet (Annual) ---")
        retries = 0
        while retries < MAX_RETRIES:
            try:
                balance_sheet_annual = ticker_obj.balance_sheet
                balance_sheet_records = prepare_dynamic_financial_data(balance_sheet_annual, security_id, fiscal_quarter=None)
                upload_res = upload_data_to_supabase('financials.balance_sheets_annual', balance_sheet_records, ['security_id', 'fiscal_year', 'fiscal_quarter'])
                ingestion_results.append(upload_res['message'])
                if upload_res['status'] == 'error':
                    ticker_financials_failed = True
                break
            except Exception as e:
                msg = f"Error fetching/uploading Balance Sheet for {ticker_symbol}: {e}"
                ingestion_results.append(msg)
                print(msg)
                retries += 1
                time.sleep(RETRY_DELAY_SECONDS)
        if retries == MAX_RETRIES:
            msg = f"Failed to fetch/upload Balance Sheet for {ticker_symbol} after {MAX_RETRIES} retries."
            ingestion_results.append(msg)
            print(msg)
            ticker_financials_failed = True
        time.sleep(REQUEST_DELAY_SECONDS)

        # --- Cash Flow (Annual) ---
        ingestion_results.append("\n--- Processing Cash Flow (Annual) ---")
        print("\n--- Processing Cash Flow (Annual) ---")
        retries = 0
        while retries < MAX_RETRIES:
            try:
                cash_flow_annual = ticker_obj.cashflow
                cash_flow_records = prepare_dynamic_financial_data(cash_flow_annual, security_id, fiscal_quarter=None)
                upload_res = upload_data_to_supabase('financials.cash_flows_annual', cash_flow_records, ['security_id', 'fiscal_year', 'fiscal_quarter'])
                ingestion_results.append(upload_res['message'])
                if upload_res['status'] == 'error':
                    ticker_financials_failed = True
                break
            except Exception as e:
                msg = f"Error fetching/uploading Cash Flow for {ticker_symbol}: {e}"
                ingestion_results.append(msg)
                print(msg)
                retries += 1
                time.sleep(RETRY_DELAY_SECONDS)
        if retries == MAX_RETRIES:
            msg = f"Failed to fetch/upload Cash Flow for {ticker_symbol} after {MAX_RETRIES} retries." # Fixed typo here
            ingestion_results.append(msg)
            print(msg)
            ticker_financials_failed = True
        time.sleep(REQUEST_DELAY_SECONDS)

        if not ticker_financials_failed:
            successful_tickers.append(ticker_symbol)
        else:
            partially_failed_tickers.append(ticker_symbol) # Core data succeeded, but some financials failed

    final_summary = "\n--- Ingestion Summary ---"
    final_summary += f"\nTotal tickers attempted: {len(all_asx_tickers)}"
    final_summary += f"\nSuccessfully processed (all financials): {len(successful_tickers)} - {', '.join(successful_tickers) or 'None'}"
    final_summary += f"\nPartially failed (core data OK, some financials failed): {len(partially_failed_tickers)} - {', '.join(partially_failed_tickers) or 'None'}"
    final_summary += f"\nSkipped (yfinance data not found): {len(skipped_tickers)} - {', '.join(skipped_tickers) or 'None'}"
    ingestion_results.append(final_summary)
    print(final_summary)

    # Determine overall status for the API response
    overall_status = "success"
    if skipped_tickers or partially_failed_tickers:
        overall_status = "partial_success" if successful_tickers else "error" # If no successful tickers, and some skipped/failed, it's an overall error.

    return jsonify({"status": overall_status, "message": "\n".join(ingestion_results)}), 200

if __name__ == '__main__':
    # Run the Flask app for local testing
    app.run(debug=True, host='0.0.0.0', port=os.environ.get('PORT', 5000))
