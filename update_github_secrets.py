"""
Update GitHub Secrets with new tokens
This script updates SHAREPOINT_ACCESS_TOKEN and SHAREPOINT_REFRESH_TOKEN in GitHub Secrets
"""

import os
import sys
import requests
import base64
from nacl import encoding, public
import json

class GitHubSecretsUpdater:
    def __init__(self):
        self.github_token = os.environ.get('GITHUB_TOKEN')
        self.repo = os.environ.get('GITHUB_REPOSITORY', '')
        
        if not self.github_token:
            raise Exception("GITHUB_TOKEN not found in environment")
        
        if '/' not in self.repo:
            raise Exception("Invalid GITHUB_REPOSITORY format")
        
        self.repo_owner, self.repo_name = self.repo.split('/')
        self.api_base = "https://api.github.com"
        
        print(f"✅ Initialized for repo: {self.repo_owner}/{self.repo_name}")
    
    def get_public_key(self):
        """Get repository public key for encrypting secrets"""
        url = f"{self.api_base}/repos/{self.repo_owner}/{self.repo_name}/actions/secrets/public-key"
        headers = {
            "Authorization": f"token {self.github_token}",
            "Accept": "application/vnd.github.v3+json"
        }
        
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to get public key: {response.status_code} - {response.text}")
    
    def encrypt_secret(self, public_key, secret_value):
        """Encrypt secret using repository public key"""
        public_key_obj = public.PublicKey(public_key.encode("utf-8"), encoding.Base64Encoder())
        sealed_box = public.SealedBox(public_key_obj)
        encrypted = sealed_box.encrypt(secret_value.encode("utf-8"))
        
        return base64.b64encode(encrypted).decode("utf-8")
    
    def update_secret(self, secret_name, secret_value):
        """Update a GitHub secret"""
        try:
            print(f"🔄 Updating {secret_name}...")
            
            # Get public key
            key_data = self.get_public_key()
            
            # Encrypt secret
            encrypted_value = self.encrypt_secret(key_data["key"], secret_value)
            
            # Update secret
            url = f"{self.api_base}/repos/{self.repo_owner}/{self.repo_name}/actions/secrets/{secret_name}"
            headers = {
                "Authorization": f"token {self.github_token}",
                "Accept": "application/vnd.github.v3+json"
            }
            data = {
                "encrypted_value": encrypted_value,
                "key_id": key_data["key_id"]
            }
            
            response = requests.put(url, headers=headers, json=data)
            if response.status_code in [201, 204]:
                print(f"✅ Successfully updated {secret_name}")
                return True
            else:
                print(f"❌ Failed to update {secret_name}: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            print(f"❌ Error updating secret {secret_name}: {str(e)}")
            return False

def main():
    """Main function to update tokens from environment variables or file"""
    print("="*60)
    print("🔑 GitHub Secrets Updater")
    print("="*60)
    
    try:
        updater = GitHubSecretsUpdater()
        
        # Option 1: Get tokens from environment variables
        access_token = os.environ.get('NEW_ACCESS_TOKEN')
        refresh_token = os.environ.get('NEW_REFRESH_TOKEN')
        
        # Option 2: Get tokens from file (if exists)
        if not access_token and os.path.exists('new_tokens.json'):
            print("📄 Reading tokens from new_tokens.json...")
            with open('new_tokens.json', 'r') as f:
                token_data = json.load(f)
                access_token = token_data.get('access_token')
                refresh_token = token_data.get('refresh_token')
        
        if not access_token:
            print("❌ No access token found to update")
            print("Please set NEW_ACCESS_TOKEN environment variable or create new_tokens.json file")
            sys.exit(1)
        
        # Update secrets
        success = True
        
        if access_token:
            if not updater.update_secret('SHAREPOINT_ACCESS_TOKEN', access_token):
                success = False
        
        if refresh_token:
            if not updater.update_secret('SHAREPOINT_REFRESH_TOKEN', refresh_token):
                success = False
        
        if success:
            print("\n✅ All secrets updated successfully!")
        else:
            print("\n⚠️ Some secrets failed to update")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
