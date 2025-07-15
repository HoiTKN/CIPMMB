"""
Quick Setup Script for SharePoint Delegation Flow
Hướng dẫn và tự động setup environment
"""

import os
import sys
import subprocess

def check_environment():
    """Check current environment setup"""
    print("🔍 Checking current environment...")
    
    tenant_id = os.environ.get('TENANT_ID')
    client_id = os.environ.get('CLIENT_ID')
    
    print(f"TENANT_ID: {'✅ Set' if tenant_id else '❌ Missing'}")
    print(f"CLIENT_ID: {'✅ Set' if client_id else '❌ Missing'}")
    
    if tenant_id:
        print(f"  Value: {tenant_id[:8]}...")
    if client_id:
        print(f"  Value: {client_id[:8]}...")
    
    return tenant_id and client_id

def setup_environment():
    """Guide user through environment setup"""
    print("\n🔧 Environment Setup Required")
    print("=" * 50)
    
    print("You need to set these environment variables:")
    print("TENANT_ID=81060475...")
    print("CLIENT_ID=076541aa...")
    print()
    
    # Detect OS and provide appropriate commands
    if os.name == 'nt':  # Windows
        print("For Windows Command Prompt:")
        print("set TENANT_ID=81060475...")
        print("set CLIENT_ID=076541aa...")
        print()
        print("For Windows PowerShell:")
        print("$env:TENANT_ID=\"81060475...\"")
        print("$env:CLIENT_ID=\"076541aa...\"")
    else:  # Mac/Linux
        print("For Mac/Linux:")
        print("export TENANT_ID=81060475...")
        print("export CLIENT_ID=076541aa...")
    
    print()
    print("📋 Get the actual values from:")
    print("- Your existing GitHub Secrets")
    print("- IT team")
    print("- Previous working setup")
    
    input("\nPress Enter after setting environment variables...")
    return check_environment()

def check_dependencies():
    """Check if required Python packages are installed"""
    print("\n📦 Checking Python dependencies...")
    
    required_packages = ['msal', 'requests', 'pandas', 'openpyxl']
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
            print(f"✅ {package}")
        except ImportError:
            print(f"❌ {package} (missing)")
            missing_packages.append(package)
    
    if missing_packages:
        print(f"\n🔧 Installing missing packages: {', '.join(missing_packages)}")
        try:
            subprocess.check_call([sys.executable, '-m', 'pip', 'install'] + missing_packages)
            print("✅ Dependencies installed successfully")
        except subprocess.CalledProcessError:
            print("❌ Failed to install dependencies")
            print("Please run manually: pip install msal requests pandas openpyxl")
            return False
    
    return True

def run_token_generation():
    """Run the token generation script"""
    print("\n🔑 Running token generation...")
    print("=" * 50)
    
    if not os.path.exists('generate_tokens.py'):
        print("❌ generate_tokens.py not found in current directory")
        print("Please make sure you're in the correct directory")
        return False
    
    try:
        # Import and run token generation
        from generate_tokens import generate_tokens, test_tokens
        
        success = generate_tokens()
        
        if success:
            print("\n🧪 Testing generated tokens...")
            test_success = test_tokens()
            
            if test_success:
                print("\n🎉 TOKEN GENERATION SUCCESSFUL!")
                return True
            else:
                print("\n⚠️ Tokens generated but test failed")
                print("Check SharePoint permissions and try again")
                return False
        else:
            print("\n❌ Token generation failed")
            return False
            
    except Exception as e:
        print(f"❌ Error running token generation: {str(e)}")
        return False

def show_github_instructions():
    """Show instructions for adding tokens to GitHub"""
    print("\n📋 GITHUB SECRETS SETUP")
    print("=" * 50)
    print()
    print("1. Go to your GitHub repository")
    print("2. Settings > Secrets and variables > Actions")
    print("3. Click 'New repository secret'")
    print("4. Add these secrets (values from generate_tokens.py output):")
    print("   - SHAREPOINT_ACCESS_TOKEN")
    print("   - SHAREPOINT_REFRESH_TOKEN")
    print()
    print("5. Test the workflow:")
    print("   - Go to Actions tab")
    print("   - Select 'QA Data Processing - SharePoint Delegation Flow'")
    print("   - Click 'Run workflow'")
    print()
    print("📁 Generated files:")
    if os.path.exists('sharepoint_tokens.json'):
        print("✅ sharepoint_tokens.json (keep as backup)")
    else:
        print("❌ sharepoint_tokens.json (not found)")

def main():
    """Main setup function"""
    print("🚀 SharePoint Delegation Flow - Quick Setup")
    print("=" * 60)
    print()
    print("This script will help you setup delegation flow for QA data processing")
    print("No CLIENT_SECRET required!")
    print()
    
    # Step 1: Check environment
    if not check_environment():
        if not setup_environment():
            print("❌ Environment setup failed. Please set environment variables and try again.")
            return
    
    # Step 2: Check dependencies
    if not check_dependencies():
        print("❌ Dependency check failed. Please install required packages and try again.")
        return
    
    # Step 3: Generate tokens
    print("\n🎯 Ready to generate SharePoint access tokens!")
    print("This will:")
    print("1. Open your browser")
    print("2. Ask you to sign in with Masan account")
    print("3. Generate access and refresh tokens")
    print("4. Show you exactly what to add to GitHub Secrets")
    print()
    
    proceed = input("Continue with token generation? (y/n): ").lower().strip()
    if proceed != 'y':
        print("Setup cancelled by user")
        return
    
    if run_token_generation():
        show_github_instructions()
        
        print("\n🎉 SETUP COMPLETE!")
        print("=" * 50)
        print("✅ Environment configured")
        print("✅ Dependencies installed")
        print("✅ Tokens generated")
        print("✅ GitHub instructions provided")
        print()
        print("📋 Next steps:")
        print("1. Add tokens to GitHub Secrets")
        print("2. Run GitHub Actions workflow")
        print("3. Monitor workflow execution")
        print()
        print("💡 Keep sharepoint_tokens.json file as backup!")
        
    else:
        print("\n❌ SETUP FAILED")
        print("Please check error messages above and try again")

if __name__ == "__main__":
    main()
