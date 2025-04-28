import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import json

# Set page configuration
st.set_page_config(
    page_title="Customer Complaint Dashboard",
    page_icon="⚠️",
    layout="wide"
)

# Title and description
st.title("Customer Complaint Dashboard")
st.markdown("Real-time dashboard for monitoring customer complaints in FMCG production")

# Function to connect to Google Sheets
def connect_to_sheets():
    try:
        # Check if we're running in Streamlit Cloud (in which case we need to use st.secrets)
        if 'GOOGLE_CLIENT_SECRET' in st.secrets:
            # Create a credentials dictionary from the secret
            creds_dict = json.loads(st.secrets["GOOGLE_CLIENT_SECRET"])
            
            # Define the scope
            scope = ['https://spreadsheets.google.com/feeds',
                    'https://www.googleapis.com/auth/drive']
            
            # Create credentials from the dictionary
            creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
            client = gspread.authorize(creds)
            
            # Open the Google Sheet by URL or key
            sheet_url = "https://docs.google.com/spreadsheets/d/1d6uGPbJV6BsOB6XSB1IS3NhfeaMyMBcaQPvOnNg2yA4/edit?gid=1495122288#gid=1495122288"
            # Extract sheet key from URL
            sheet_key = sheet_url.split('/d/')[1].split('/')[0]
            
            # Open the spreadsheet and the first worksheet
            spreadsheet = client.open_by_key(sheet_key)
            worksheet = spreadsheet.worksheet('Integrated_Data')  # Use the same worksheet name as in sheets_integration.py
            
            return worksheet
        # Fallback to local file (for local development)
        else:
            # Method 2: If you have a client_secret.json file in your repository
            creds_path = "client_secret.json"  # Path to your credentials file
            
            # Define the scope
            scope = ['https://spreadsheets.google.com/feeds',
                    'https://www.googleapis.com/auth/drive']
            
            # Authenticate
            creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
            client = gspread.authorize(creds)
            
            # Open the Google Sheet by URL or key
            sheet_url = "https://docs.google.com/spreadsheets/d/1d6uGPbJV6BsOB6XSB1IS3NhfeaMyMBcaQPvOnNg2yA4/edit?gid=1495122288#gid=1495122288"
            # Extract sheet key from URL
            sheet_key = sheet_url.split('/d/')[1].split('/')[0]
            
            # Open the spreadsheet and the first worksheet
            spreadsheet = client.open_by_key(sheet_key)
            worksheet = spreadsheet.worksheet('Integrated_Data')  # Use the same worksheet name as in sheets_integration.py
            
            return worksheet
    except Exception as e:
        st.error(f"Error connecting to Google Sheets: {e}")
        return None

# Rest of your app.py code remains the same...
