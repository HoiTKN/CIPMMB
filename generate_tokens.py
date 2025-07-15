"""
Token Generator for Delegation Flow
Chạy script này locally để tạo access token và refresh token
Sau đó add tokens vào GitHub Secrets
"""

import msal
import json
import os
from datetime import datetime, timedelta
from config_delegation import GRAPH_API_CONFIG

def generate_tokens():
    """Generate access and refresh tokens using delegation flow"""
    try:
        print("="*60)
        print("🔑 SHAREPOINT TOKEN GENERATOR - DELEGATION FLOW")
        print("="*60)
        print()
        
        # Check required environment variables
        client_id = GRAPH_API_CONFIG['client_id']
        tenant_id = GRAPH_API_CONFIG['tenant_id']
        
        if not client_id or not tenant_id:
            print("❌ Missing TENANT_ID or CLIENT_ID environment variables")
            print("Please set these environment variables:")
            print("export TENANT_ID=your_tenant_id")
            print("export CLIENT_ID=your_client_id")
            return False
        
        print(f"📋 Configuration:")
        print(f"  Tenant ID: {tenant_id[:8]}...")
        print(f"  Client ID: {client_id[:8]}...")
        print(f"  Scopes: {', '.join(GRAPH_API_CONFIG['scopes'])}")
        print()
        
        # Create MSAL Public Client Application
        app = msal.PublicClientApplication(
            client_id,
            authority=GRAPH_API_CONFIG['authority']
        )
        
        scopes = GRAPH_API_CONFIG['scopes']
        
        # Try silent authentication first (cache-based)
        print("🔍 Checking for cached tokens...")
        accounts = app.get_accounts()
        if accounts:
            print(f"📱 Found {len(accounts)} cached account(s)")
            result = app.acquire_token_silent(scopes, account=accounts[0])
            if result and "access_token" in result:
                print("✅ Found valid cached token!")
                save_tokens(result)
                return True
            else:
                print("⚠️ Cached token expired or invalid")
        
        # Device code flow for new authentication
        print("\n🌐 Starting device code authentication...")
        print("This will open a browser window for authentication")
        print()
        
        flow = app.initiate_device_flow(scopes=scopes)
        
        if "user_code" not in flow:
            print("❌ Failed to create device flow")
            return False
        
        print("=" * 60)
        print("🔑 AUTHENTICATION REQUIRED")
        print("=" * 60)
        print(f"1. Open your browser and go to: {flow['verification_uri']}")
        print(f"2. Enter the code: {flow['user_code']}")
        print("3. Sign in with your Masan Group account")
        print("4. Grant permissions for SharePoint access")
        print("5. Return here and wait for completion...")
        print("=" * 60)
        print()
        
        # Wait for user to complete authentication
        print("⏳ Waiting for authentication completion...")
        result = app.acquire_token_by_device_flow(flow)
        
        if "access_token" in result:
            print("✅ Authentication successful!")
            save_tokens(result)
            return True
        else:
            print(f"❌ Authentication failed: {result.get('error_description', 'Unknown error')}")
            return False
            
    except Exception as e:
        print(f"❌ Error during token generation: {str(e)}")
        return False

def save_tokens(token_result):
    """Save tokens to file and display GitHub Secrets info"""
    try:
        print("\n📝 Saving tokens...")
        
        # Save tokens to file
        token_data = {
            'access_token': token_result['access_token'],
            'refresh_token': token_result.get('refresh_token'),
            'expires_in': token_result.get('expires_in'),
            'token_type': token_result.get('token_type', 'Bearer'),
            'scope': token_result.get('scope'),
            'generated_at': datetime.now().isoformat(),
            'expires_at': (datetime.now() + timedelta(seconds=token_result.get('expires_in', 3600))).isoformat()
        }
        
        # Save to local file
        with open('sharepoint_tokens.json', 'w') as f:
            json.dump(token_data, f, indent=2)
        
        print("✅ Tokens saved to: sharepoint_tokens.json")
        
        # Display GitHub Secrets information
        print("\n" + "="*60)
        print("🔧 GITHUB SECRETS SETUP")
        print("="*60)
        print("Add these secrets to your GitHub repository:")
        print()
        print("Secret Name: SHAREPOINT_ACCESS_TOKEN")
        print(f"Secret Value: {token_result['access_token']}")
        print()
        
        if token_result.get('refresh_token'):
            print("Secret Name: SHAREPOINT_REFRESH_TOKEN")
            print(f"Secret Value: {token_result['refresh_token']}")
            print()
        
        # Token expiry info
        expires_at = datetime.now() + timedelta(seconds=token_result.get('expires_in', 3600))
        print(f"📅 Token expires at: {expires_at.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"⏰ Token valid for: {token_result.get('expires_in', 3600)} seconds")
        print()
        
        # Instructions
        print("🔧 SETUP INSTRUCTIONS:")
        print("1. Go to your GitHub repository")
        print("2. Settings > Secrets and variables > Actions")
        print("3. Add the above secrets")
        print("4. Run the GitHub Actions workflow")
        print()
        
        if token_result.get('refresh_token'):
            print("💡 REFRESH TOKEN AVAILABLE:")
            print("- Your workflow can automatically refresh the access token")
            print("- No need to regenerate tokens manually unless refresh token expires")
        else:
            print("⚠️ NO REFRESH TOKEN:")
            print("- You may need to regenerate tokens when they expire")
            print("- Consider running this script periodically")
        
        print("\n" + "="*60)
        print("🎉 TOKEN GENERATION COMPLETE!")
        print("="*60)
        
        return True
        
    except Exception as e:
        print(f"❌ Error saving tokens: {str(e)}")
        return False

def test_tokens():
    """Test generated tokens by making a simple Graph API call"""
    try:
        print("\n🧪 Testing generated tokens...")
        
        # Read tokens from file
        with open('sharepoint_tokens.json', 'r') as f:
            token_data = json.load(f)
        
        access_token = token_data['access_token']
        
        # Test API call
        import requests
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        # Test site access
        site_url = f"https://graph.microsoft.com/v1.0/sites/masangroup.sharepoint.com:/sites/MCH.MMB.QA"
        response = requests.get(site_url, headers=headers)
        
        if response.status_code == 200:
            site_data = response.json()
            print(f"✅ Site access successful: {site_data.get('displayName', 'Unknown')}")
            
            # Test drives access
            drives_url = f"https://graph.microsoft.com/v1.0/sites/{site_data['id']}/drives"
            drives_response = requests.get(drives_url, headers=headers)
            
            if drives_response.status_code == 200:
                drives = drives_response.json().get('value', [])
                print(f"✅ Found {len(drives)} drives in SharePoint site")
                print("✅ Token test successful!")
                return True
            else:
                print(f"❌ Cannot access drives: {drives_response.status_code}")
                return False
        else:
            print(f"❌ Site access failed: {response.status_code}")
            if response.status_code == 401:
                print("💡 Token may be expired or invalid")
            return False
            
    except FileNotFoundError:
        print("❌ Token file not found. Please run token generation first.")
        return False
    except Exception as e:
        print(f"❌ Error testing tokens: {str(e)}")
        return False

if __name__ == "__main__":
    print("🚀 Starting token generation process...\n")
    
    success = generate_tokens()
    
    if success:
        print("\n🧪 Running token test...")
        test_success = test_tokens()
        
        if test_success:
            print("\n🎉 SUCCESS! Tokens generated and tested successfully!")
            print("📋 Next steps:")
            print("1. Add tokens to GitHub Secrets")
            print("2. Run GitHub Actions workflow")
            print("3. Monitor workflow execution")
        else:
            print("\n⚠️ Tokens generated but test failed")
            print("Please check SharePoint permissions and try again")
    else:
        print("\n❌ Token generation failed")
        print("Please check your credentials and try again")
