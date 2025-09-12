# generate_onedrive_tokens.py - Generate tokens specifically for shared OneDrive access

import msal
import json
import os
import requests
import base64
from datetime import datetime, timedelta
from config_onedrive_shared import GRAPH_API_CONFIG, SHARED_FILE_CONFIG

def generate_onedrive_tokens():
    """Generate tokens with enhanced scopes for shared OneDrive access"""
    
    print("="*70)
    print("🔑 ONEDRIVE SHARED FILE TOKEN GENERATOR")
    print("="*70)
    print(f"Target file: {SHARED_FILE_CONFIG['filename']}")
    print(f"File ID: {SHARED_FILE_CONFIG['file_id']}")
    print(f"Owner: {SHARED_FILE_CONFIG['owner_email']}")
    print(f"Required scopes: {', '.join(GRAPH_API_CONFIG['scopes'])}")
    print()
    
    try:
        # Create MSAL app
        app = msal.PublicClientApplication(
            GRAPH_API_CONFIG['client_id'],
            authority=GRAPH_API_CONFIG['authority']
        )
        
        print("🌐 Starting authentication...")
        print("⚠️  IMPORTANT: Login with an account that has been shared the OneDrive file")
        print()
        
        # Device code flow
        flow = app.initiate_device_flow(scopes=GRAPH_API_CONFIG['scopes'])
        
        if "user_code" not in flow:
            print("❌ Failed to create device flow")
            return False
        
        print("=" * 50)
        print("🔑 AUTHENTICATION REQUIRED")
        print("=" * 50)
        print(f"1. Open browser: {flow['verification_uri']}")
        print(f"2. Enter code: {flow['user_code']}")
        print("3. Login with account that can access the shared file")
        print("4. Accept all permissions")
        print("=" * 50)
        
        input("Press Enter when you've completed authentication...")
        
        # Get token
        result = app.acquire_token_by_device_flow(flow)
        
        if "access_token" in result:
            print("✅ Authentication successful!")
            
            # Test the token immediately
            if test_shared_file_access(result['access_token']):
                save_onedrive_tokens(result)
                return True
            else:
                print("❌ Token generated but file access test failed")
                return False
        else:
            print(f"❌ Authentication failed: {result.get('error_description', 'Unknown error')}")
            return False
            
    except Exception as e:
        print(f"❌ Error during token generation: {str(e)}")
        return False

def test_shared_file_access(access_token):
    """Test if token can access the shared OneDrive file"""
    
    print("\n🧪 TESTING SHARED FILE ACCESS...")
    
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    
    base_url = "https://graph.microsoft.com/v1.0"
    file_id = SHARED_FILE_CONFIG['file_id']
    owner_email = SHARED_FILE_CONFIG['owner_email']
    
    # Test 1: Basic authentication
    print("1️⃣ Testing authentication...")
    response = requests.get(f"{base_url}/me", headers=headers, timeout=30)
    if response.status_code != 200:
        print(f"❌ Authentication failed: {response.status_code}")
        return False
    
    user_info = response.json()
    current_user = user_info.get('userPrincipalName', 'Unknown')
    print(f"✅ Authenticated as: {user_info.get('displayName')} ({current_user})")
    
    # Test 2: Try different approaches to access shared file
    access_methods = [
        {
            'name': 'Direct file access',
            'url': f"{base_url}/me/drive/items/{file_id}",
            'description': 'Access file directly if it\'s in shared items'
        },
        {
            'name': 'Owner drive access',
            'url': f"{base_url}/users/{owner_email}/drive/items/{file_id}",
            'description': 'Access via owner\'s drive if permissions allow'
        },
        {
            'name': 'Shared items listing',
            'url': f"{base_url}/me/drive/sharedWithMe",
            'description': 'List all shared items to find the file'
        }
    ]
    
    file_found = False
    download_url = None
    
    for i, m
