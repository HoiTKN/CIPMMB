"""
GitHub Actions runner for delegation flow
Handles authentication challenges in automated environment
"""

import os
import sys
import json
import requests
import msal
import pandas as pd
from datetime import datetime

class GitHubDelegationRunner:
    def __init__(self):
        self.tenant_id = os.environ.get('TENANT_ID')
        self.client_id = os.environ.get('CLIENT_ID')
        self.site_url = os.environ.get('SHAREPOINT_SITE_URL')
        self.debug_mode = os.environ.get('DEBUG_MODE', 'false').lower() == 'true'
        self.test_mode = os.environ.get('TEST_MODE', 'true').lower() == 'true'
        self.access_token = None
        
    def log(self, message):
        """Log with timestamp"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{timestamp}] {message}")
    
    def check_environment(self):
        """Check if running in GitHub Actions and validate config"""
        self.log("🔧 Checking environment...")
        
        if not self.tenant_id or not self.client_id:
            self.log("❌ Missing TENANT_ID or CLIENT_ID")
            return False
        
        is_github_actions = os.environ.get('GITHUB_ACTIONS', 'false').lower() == 'true'
        
        if is_github_actions:
            self.log("✅ Running in GitHub Actions")
            self.log(f"Repository: {os.environ.get('GITHUB_REPOSITORY', 'Unknown')}")
            self.log(f"Run Number: {os.environ.get('GITHUB_RUN_NUMBER', 'Unknown')}")
        else:
            self.log("🏠 Running locally")
        
        self.log(f"Tenant ID: {self.tenant_id[:8]}...")
        self.log(f"Client ID: {self.client_id[:8]}...")
        self.log(f"Site URL: {self.site_url}")
        self.log(f"Debug Mode: {self.debug_mode}")
        self.log(f"Test Mode: {self.test_mode}")
        
        return True
    
    def attempt_delegation_auth(self):
        """Attempt delegation authentication with GitHub Actions compatibility"""
        self.log("🔑 Attempting delegation authentication...")
        
        try:
            # Create MSAL Public Client Application
            app = msal.PublicClientApplication(
                self.client_id,
                authority=f"https://login.microsoftonline.com/{self.tenant_id}"
            )
            
            scopes = [
                "https://graph.microsoft.com/Sites.Read.All",
                "https://graph.microsoft.com/Files.Read.All"
            ]
            
            # Try silent authentication first (cache-based)
            accounts = app.get_accounts()
            if accounts:
                self.log("📱 Found cached account, attempting silent authentication...")
                result = app.acquire_token_silent(scopes, account=accounts[0])
                if result and "access_token" in result:
                    self.access_token = result['access_token']
                    self.log("✅ Silent authentication successful!")
                    return True
            
            # Check if running in GitHub Actions
            if os.environ.get('GITHUB_ACTIONS'):
                self.log("❌ Interactive authentication not possible in GitHub Actions")
                self.log("💡 GitHub Actions requires non-interactive authentication")
                self.provide_github_solutions()
                return False
            
            # For local testing - device code flow
            self.log("🌐 Starting device code flow...")
            flow = app.initiate_device_flow(scopes=scopes)
            
            if "user_code" not in flow:
                self.log("❌ Failed to create device flow")
                return False
            
            self.log("=" * 60)
            self.log("🔑 DEVICE CODE AUTHENTICATION")
            self.log("=" * 60)
            self.log(f"1. Open browser: {flow['verification_uri']}")
            self.log(f"2. Enter code: {flow['user_code']}")
            self.log("3. Sign in with your Masan account")
            self.log("4. Return here and wait...")
            self.log("=" * 60)
            
            result = app.acquire_token_by_device_flow(flow)
            
            if "access_token" in result:
                self.access_token = result['access_token']
                self.log("✅ Device flow authentication successful!")
                return True
            else:
                self.log(f"❌ Device flow failed: {result.get('error_description', 'Unknown error')}")
                return False
                
        except Exception as e:
            self.log(f"❌ Authentication error: {str(e)}")
            return False
    
    def provide_github_solutions(self):
        """Provide solutions for GitHub Actions automation"""
        self.log("\n" + "=" * 60)
        self.log("💡 GITHUB ACTIONS AUTOMATION SOLUTIONS")
        self.log("=" * 60)
        
        self.log("🔧 Option 1: Ask IT team for Service Principal")
        self.log("Send this message to IT team:")
        self.log("---")
        self.log("Anh/chị ơi, em cần setup automation cho SharePoint API trong GitHub Actions.")
        self.log("Em có thể có Service Principal với delegated permissions không?")
        self.log("Hoặc approach nào khác cho non-interactive authentication?")
        self.log("Current delegation flow cần browser interaction, không work với automation.")
        self.log("---")
        
        self.log("\n🔧 Option 2: Application Flow with CLIENT_SECRET")
        self.log("- Ask IT team for CLIENT_SECRET")
        self.log("- Use application permissions instead of delegated")
        self.log("- Fully automated, no user interaction needed")
        
        self.log("\n🔧 Option 3: Hybrid Approach")
        self.log("- Manual local processing with delegation")
        self.log("- Upload results to GitHub manually or via script")
        self.log("- GitHub Actions for processing uploaded data")
        
        self.log("\n🔧 Option 4: Pre-authenticated Tokens")
        self.log("- Generate tokens locally via delegation")
        self.log("- Add tokens to GitHub Secrets (refresh periodically)")
        self.log("- Use tokens directly in GitHub Actions")
    
    def test_sharepoint_connectivity(self):
        """Test basic SharePoint connectivity"""
        if not self.access_token:
            self.log("❌ No access token available for SharePoint testing")
            return False
        
        try:
            self.log("🌐 Testing SharePoint connectivity...")
            
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json'
            }
            
            # Test site access
            site_url = f"https://graph.microsoft.com/v1.0/sites/masangroup.sharepoint.com:/sites/MCH.MMB.QA"
            response = requests.get(site_url, headers=headers)
            
            if response.status_code == 200:
                site_data = response.json()
                self.log(f"✅ SharePoint site accessible: {site_data.get('displayName', 'Unknown')}")
                
                # Test drives
                drives_url = f"https://graph.microsoft.com/v1.0/sites/{site_data['id']}/drives"
                drives_response = requests.get(drives_url, headers=headers)
                
                if drives_response.status_code == 200:
                    drives = drives_response.json().get('value', [])
                    self.log(f"✅ Found {len(drives)} drives in SharePoint site")
                    
                    for drive in drives[:3]:  # Show first 3 drives
                        self.log(f"  📁 {drive.get('name', 'Unknown')} (ID: {drive.get('id', 'Unknown')[:8]}...)")
                    
                    return True
                else:
                    self.log(f"❌ Cannot access drives: {drives_response.status_code}")
                    return False
            else:
                self.log(f"❌ Cannot access SharePoint site: {response.status_code}")
                if response.status_code == 403:
                    self.log("💡 This might be a permissions issue")
                return False
                
        except Exception as e:
            self.log(f"❌ SharePoint connectivity test failed: {str(e)}")
            return False
    
    def create_test_output(self):
        """Create test output files for verification"""
        try:
            self.log("📝 Creating test output...")
            
            # Create output directory
            os.makedirs('output', exist_ok=True)
            
            # Create test summary
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            test_results = {
                'test_timestamp': timestamp,
                'environment': 'GitHub Actions' if os.environ.get('GITHUB_ACTIONS') else 'Local',
                'tenant_id': self.tenant_id[:8] + '...',
                'client_id': self.client_id[:8] + '...',
                'site_url': self.site_url,
                'authentication_successful': self.access_token is not None,
                'sharepoint_accessible': False,  # Will be updated if test passes
                'debug_mode': self.debug_mode,
                'test_mode': self.test_mode
            }
            
            # Test SharePoint if authenticated
            if self.access_token:
                test_results['sharepoint_accessible'] = self.test_sharepoint_connectivity()
            
            # Save results as JSON
            results_file = f'output/test_results_{timestamp}.json'
            with open(results_file, 'w') as f:
                json.dump(test_results, f, indent=2)
            
            self.log(f"✅ Test results saved: {results_file}")
            
            # Create summary CSV
            summary_data = pd.DataFrame([{
                'Test': 'Delegation Flow Authentication',
                'Status': 'Success' if self.access_token else 'Failed',
                'Timestamp': timestamp,
                'Environment': test_results['environment'],
                'Notes': 'Authentication successful' if self.access_token else 'Need alternative authentication method'
            }])
            
            summary_file = f'output/test_summary_{timestamp}.csv'
            summary_data.to_csv(summary_file, index=False)
            self.log(f"✅ Test summary saved: {summary_file}")
            
            return True
            
        except Exception as e:
            self.log(f"❌ Error creating test output: {str(e)}")
            return False
    
    def run(self):
        """Main run method"""
        self.log("=" * 60)
        self.log("🚀 GITHUB DELEGATION FLOW RUNNER")
        self.log("=" * 60)
        
        # Check environment
        if not self.check_environment():
            sys.exit(1)
        
        # Attempt authentication
        auth_success = self.attempt_delegation_auth()
        
        # Create test output regardless of auth success
        output_success = self.create_test_output()
        
        # Summary
        self.log("\n" + "=" * 60)
        self.log("📊 TEST SUMMARY")
        self.log("=" * 60)
        
        if auth_success:
            self.log("✅ Delegation authentication: SUCCESS")
            self.log("✅ SharePoint access: Available")
            self.log("🎉 Ready for data processing!")
        else:
            self.log("❌ Delegation authentication: FAILED")
            self.log("💡 Check solutions above for GitHub Actions automation")
        
        if output_success:
            self.log("✅ Test output files: Generated")
        else:
            self.log("❌ Test output files: Failed")
        
        self.log(f"📁 Check 'output/' folder for test results")
        
        # Return appropriate exit code
        return 0 if auth_success else 1

def main():
    runner = GitHubDelegationRunner()
    exit_code = runner.run()
    sys.exit(exit_code)

if __name__ == "__main__":
    main()
