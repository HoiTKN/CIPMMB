# config_onedrive_shared.py - Configuration for accessing shared OneDrive files

GRAPH_API_CONFIG = {
    'client_id': '076541aa-c734-405e-8518-ed52b67f8cbd',
    'tenant_id': '81060475-7e7f-4ede-8d8d-bf61f53ca528', 
    'authority': 'https://login.microsoftonline.com/81060475-7e7f-4ede-8d8d-bf61f53ca528',
    'scopes': [
        # CRITICAL: These scopes are required for shared OneDrive access
        'https://graph.microsoft.com/Files.ReadWrite.All',    # Access to all files including shared
        'https://graph.microsoft.com/Sites.ReadWrite.All',   # SharePoint sites access  
        'https://graph.microsoft.com/User.Read',             # Basic user info
        'https://graph.microsoft.com/Files.Read.All',        # Read access to all files
        'https://graph.microsoft.com/Directory.Read.All'     # Directory access (for user lookup)
    ]
}

# Shared file information
SHARED_FILE_CONFIG = {
    'file_id': '69AE13C5-76D7-4061-90E2-CE48F965C33A',
    'filename': 'BÁO CÁO KNKH.xlsx',
    'owner_email': 'hanpt@mml.masangroup.com',
    'share_url': 'https://masangroup-my.sharepoint.com/:x:/r/personal/hanpt_mml_masangroup_com/_layouts/15/Doc.aspx?sourcedoc=%7B69AE13C5-76D7-4061-90E2-CE48F965C33A%7D&file=B%C3%81O%20C%C3%81O%20KNKH.xlsx&action=default&mobileredirect=true&wdOrigin=OUTLOOK-METAOS.FILEBROWSER'
}
