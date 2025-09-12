# TEST SCRIPT FOR SHARED ONEDRIVE FILE ACCESS
# Run this to find the best way to access the shared file

import requests
import os
import json
import base64

def test_shared_file_access():
    """Test different approaches to access shared OneDrive file"""
    
    access_token = os.environ.get('SHAREPOINT_ACCESS_TOKEN')
    if not access_token:
        print("❌ No access token found")
        return False
    
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    
    base_url = "https://graph.microsoft.com/v1.0"
    file_id = "69AE13C5-76D7-4061-90E2-CE48F965C33A"
    filename = "BÁO CÁO KNKH.xlsx"
    user_email = "hanpt@mml.masangroup.com"
    
    print("🔍 Testing different approaches to access shared OneDrive file...")
    print(f"Target file: {filename}")
    print(f"File ID: {file_id}")
    print(f"Owner: {user_email}")
    
    # Test 1: Check authentication
    print("\n1️⃣ Testing authentication...")
    response = requests.get(f"{base_url}/me", headers=headers, timeout=30)
    if response.status_code == 200:
        user_info = response.json()
        print(f"✅ Authenticated as: {user_info.get('displayName')} ({user_info.get('userPrincipalName')})")
        current_user = user_info.get('userPrincipalName', 'unknown')
    else:
        print(f"❌ Authentication failed: {response.status_code}")
        return False
    
    # Test 2: Direct file access (original approach)
    print("\n2️⃣ Testing direct file access...")
    url = f"{base_url}/me/drive/items/{file_id}"
    response = requests.get(url, headers=headers, timeout=30)
    print(f"Direct access result: {response.status_code}")
    if response.status_code == 200:
        file_info = response.json()
        print(f"✅ Direct access works: {file_info.get('name')}")
        return True
    else:
        print(f"❌ Direct access failed: {response.text[:200]}")

    # Test 3: Search in shared items
    print("\n3️⃣ Testing shared items search...")
    url = f"{base_url}/me/drive/sharedWithMe"
    response = requests.get(url, headers=headers, timeout=30)
    if response.status_code == 200:
        shared_items = response.json().get('value', [])
        print(f"Found {len(shared_items)} shared items")
        
        for i, item in enumerate(shared_items):
            item_name = item.get('name', 'Unknown')
            item_id = item.get('id', 'No ID')
            print(f"  {i+1}. {item_name} (ID: {item_id[:20]}...)")
            
            # Check if this is our target file
            if (item_id == file_id or 
                filename.lower() in item_name.lower() or
                'KNKH' in item_name.upper()):
                print(f"✅ Found target file in shared items: {item_name}")
                
                # Try to get download URL
                download_url = item.get('@microsoft.graph.downloadUrl')
                if download_url:
                    print(f"✅ Download URL available")
                    return True
                else:
                    # Try to get file details
                    detail_url = f"{base_url}/me/drive/items/{item_id}"
                    detail_response = requests.get(detail_url, headers=headers, timeout=30)
                    if detail_response.status_code == 200:
                        detail_info = detail_response.json()
                        download_url = detail_info.get('@microsoft.graph.downloadUrl')
                        if download_url:
                            print(f"✅ Download URL found via details: {download_url[:50]}...")
                            return True
                    print(f"⚠️ Could not get download URL")
    else:
        print(f"❌ Shared items access failed: {response.status_code}")

    # Test 4: Search by filename
    print("\n4️⃣ Testing filename search...")
    search_query = filename.replace('.xlsx', '').replace(' ', '%20')
    url = f"{base_url}/me/drive/search(q='{search_query}')"
    response = requests.get(url, headers=headers, timeout=30)
    if response.status_code == 200:
        search_results = response.json().get('value', [])
        print(f"Found {len(search_results)} search results for '{search_query}'")
        
        for i, item in enumerate(search_results):
            item_name = item.get('name', 'Unknown')
            item_id = item.get('id', 'No ID')
            print(f"  {i+1}. {item_name} (ID: {item_id[:20]}...)")
            
            if filename.lower() in item_name.lower() or 'KNKH' in item_name.upper():
                print(f"✅ Found target file via search: {item_name}")
                download_url = item.get('@microsoft.graph.downloadUrl')
                if download_url:
                    print(f"✅ Download URL available")
                    return True
    else:
        print(f"❌ Search failed: {response.status_code}")

    # Test 5: User-specific access
    print("\n5️⃣ Testing user-specific access...")
    url = f"{base_url}/users/{user_email}/drive/items/{file_id}"
    response = requests.get(url, headers=headers, timeout=30)
    print(f"User-specific access result: {response.status_code}")
    if response.status_code == 200:
        file_info = response.json()
        print(f"✅ User-specific access works: {file_info.get('name')}")
        download_url = file_info.get('@microsoft.graph.downloadUrl')
        if download_url:
            print(f"✅ Download URL available")
            return True
    else:
        print(f"❌ User-specific access failed: {response.text[:200]}")

    # Test 6: Try shares endpoint with various URL formats
    print("\n6️⃣ Testing shares endpoint...")
    
    # Original share URL from your message
    share_urls = [
        f"https://masangroup-my.sharepoint.com/personal/hanpt_mml_masangroup_com/Documents/{filename}",
        f"https://masangroup-my.sharepoint.com/personal/hanpt_mml_masangroup_com/_layouts/15/Doc.aspx?sourcedoc={{{file_id.upper()}}}",
    ]
    
    for share_url in share_urls:
        try:
            # Encode the URL for shares endpoint
            encoded_url = base64.b64encode(share_url.encode()).decode().rstrip('=')
            url = f"{base_url}/shares/u!{encoded_url}/driveItem"
            response = requests.get(url, headers=headers, timeout=30)
            print(f"Shares endpoint ({share_url[:50]}...): {response.status_code}")
            
            if response.status_code == 200:
                file_info = response.json()
                print(f"✅ Shares endpoint works: {file_info.get('name')}")
                download_url = file_info.get('@microsoft.graph.downloadUrl')
                if download_url:
                    print(f"✅ Download URL available")
                    return True
            else:
                print(f"⚠️ Response: {response.text[:100]}")
        except Exception as e:
            print(f"⚠️ Shares endpoint error: {str(e)}")

    # Test 7: Try to list all drives accessible to current user
    print("\n7️⃣ Testing drive enumeration...")
    url = f"{base_url}/me/drives"
    response = requests.get(url, headers=headers, timeout=30)
    if response.status_code == 200:
        drives = response.json().get('value', [])
        print(f"Found {len(drives)} drives accessible to current user:")
        for drive in drives:
            drive_name = drive.get('name', 'Unknown')
            drive_id = drive.get('id', 'No ID')
            drive_type = drive.get('driveType', 'Unknown')
            print(f"  - {drive_name} ({drive_type}): {drive_id}")
            
            # Try to access file in each drive
            try:
                url = f"{base_url}/drives/{drive_id}/items/{file_id}"
                response = requests.get(url, headers=headers, timeout=10)
                if response.status_code == 200:
                    file_info = response.json()
                    print(f"  ✅ File found in drive {drive_name}: {file_info.get('name')}")
                    return True
            except:
                pass
    else:
        print(f"❌ Drive enumeration failed: {response.status_code}")

    print("\n❌ All approaches failed. Possible solutions:")
    print("1. File owner needs to explicitly share the file with proper permissions")
    print("2. You may need to access the file using the owner's token")
    print("3. File might need to be moved to a SharePoint site instead of personal OneDrive")
    print("4. Check if the file ID is still correct")
    
    return False

if __name__ == "__main__":
    success = test_shared_file_access()
    if success:
        print("\n✅ File access test PASSED - found a working method!")
    else:
        print("\n❌ File access test FAILED - no working method found")
        print("\n💡 Next steps:")
        print("1. Ask the file owner (hanpt@mml.masangroup.com) to:")
        print("   - Re-share the file with explicit edit permissions")
        print("   - Or move the file to the SharePoint site instead")
        print("2. Or generate tokens using the file owner's account")
