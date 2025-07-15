"""
SharePoint Configuration for QA Data Processing - Delegation Flow
Cấu hình cho delegation flow (không cần CLIENT_SECRET)
"""

import os

# Microsoft Graph API Configuration - Delegation Flow
GRAPH_API_CONFIG = {
    'client_id': os.environ.get('CLIENT_ID'),
    'tenant_id': os.environ.get('TENANT_ID'),
    'authority': f"https://login.microsoftonline.com/{os.environ.get('TENANT_ID')}",
    'scopes': [
        "https://graph.microsoft.com/Sites.Read.All",
        "https://graph.microsoft.com/Files.ReadWrite.All",
        "https://graph.microsoft.com/Sites.ReadWrite.All"
    ],
    # Delegation flow - không cần client_secret
    'flow_type': 'delegation',
    
    # Pre-authenticated tokens (sẽ được tạo locally và add vào GitHub Secrets)
    'access_token': os.environ.get('SHAREPOINT_ACCESS_TOKEN'),
    'refresh_token': os.environ.get('SHAREPOINT_REFRESH_TOKEN'),
}

# SharePoint Site Configuration
SHAREPOINT_CONFIG = {
    'site_url': os.environ.get('SHAREPOINT_SITE_URL'),
    'site_name': 'MCH.MMB.QA',
    'base_url': 'masangroup.sharepoint.com'
}

# File Paths Configuration - Updated for SharePoint
FILE_PATHS = {
    # Sample ID file - chứa VHM và % Hao hụt OPP data
    'sample_id': {
        'filename': 'Sample ID.xlsx',
        'folder': 'Shared Documents',
        'description': 'Sample ID - VHM and % Hao hụt OPP data',
        'sheets': ['Sheet1']
    },
    
    # Data SX file - chứa ID AQL, AQL gói, AQL Tô ly data
    'data_sx': {
        'filename': 'Data SX.xlsx',
        'folder': 'Shared Documents',
        'description': 'Data SX - Production and Quality data',
        'sheets': ['ID AQL', 'AQL gói', 'AQL Tô ly']
    },
    
    # CF data file - output file
    'cf_data_output': {
        'filename': 'CF data.xlsx',
        'folder': 'Shared Documents',
        'description': 'CF data - Processed output data',
        'sheets': ['Processed_Data']
    }
}

# SharePoint File IDs (extracted from URLs)
SHAREPOINT_FILE_IDS = {
    'sample_id': '8220CAEA-0CD9-585B-D483-DE0A82A98564',
    'data_sx': '6CB4A738-1EDD-4BC4-9996-43A815D3F5CF',
    'cf_data_output': 'E1B65B6F-6A53-52E0-1BB3-3BCA75A32F63'
}

# Output Configuration
OUTPUT_CONFIG = {
    'local_output_dir': 'output',
    'processed_filename': 'QA_Processed_Data_{timestamp}.xlsx',
    'summary_filename': 'QA_Summary_{timestamp}.xlsx',
    'backup_local': True,
    'upload_to_sharepoint': True
}

# QA Configuration
QA_CONFIG = {
    'target_tv': {
        'line_1_6': 0.29,  # Lines 1-6 (gói)
        'line_7_8': 2.19   # Lines 7-8 (tô ly)
    },
    'shift_mapping': {
        1: {'start': 6, 'end': 14},    # Ca 1: 6h-14h
        2: {'start': 14, 'end': 22},   # Ca 2: 14h-22h
        3: {'start': 22, 'end': 6},    # Ca 3: 22h-6h (next day)
        14: {'start': 6, 'end': 18},   # Ca 14: 6h-18h (extended)
        34: {'start': 18, 'end': 6}    # Ca 34: 18h-6h (extended)
    },
    'mdg_grouping': {
        1: [1, 2],  # MĐG 1 covers MĐG 1 and 2
        3: [3, 4]   # MĐG 3 covers MĐG 3 and 4
    },
    'required_columns': {
        'id_aql': ['Line', 'Defect code', 'Ngày SX', 'Giờ', 'MĐG'],
        'aql_goi': ['Defect code', 'Defect name'],
        'aql_to_ly': ['Defect code', 'Defect name'],
        'sample_id': ['Ngày SX', 'Ca', 'Line', 'MĐG', 'VHM', '% Hao hụt OPP']
    }
}

# Token Management Configuration
TOKEN_CONFIG = {
    'token_expiry_buffer': 300,  # 5 minutes buffer before token expires
    'max_retry_attempts': 3,
    'retry_delay': 2  # seconds
}

# Debug Configuration
DEBUG_CONFIG = {
    'enable_debug': os.environ.get('DEBUG_MODE', 'false').lower() == 'true',
    'verbose_logging': True,
    'save_intermediate_files': True
}
