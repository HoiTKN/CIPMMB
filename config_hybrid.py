# config_hybrid.py
# Configuration file for Hybrid Google Sheets + SharePoint Integration

# SharePoint Graph API Configuration
GRAPH_API_CONFIG = {
    'client_id': '076541aa-c734-405e-8518-ed52b67f8cbd',
    'tenant_id': '81060475-7e7f-4ede-8d8d-bf61f53ca528',
    'authority': 'https://login.microsoftonline.com/81060475-7e7f-4ede-8d8d-bf61f53ca528',
    'scopes': ['https://graph.microsoft.com/Sites.ReadWrite.All']
}

# SharePoint Site Configuration
SHAREPOINT_CONFIG = {
    'base_url': 'masangroup.sharepoint.com',
    'site_name': 'MCH.MMB.QA'
}

# SharePoint File IDs (extracted from provided URLs)
SHAREPOINT_FILE_IDS = {
    # Input: Sample ID.xlsx (contains AQL data)
    'sample_id': '8220CAEA-0CD9-585B-D483-DE0A82A98564',
    
    # Output: Data KNKH.xlsx (integrated results)
    'data_knkh_output': '3E86CA4D-3F41-5C10-666B-5A51F8D9C911'
}

# Google Sheets Configuration
GOOGLE_SHEETS_CONFIG = {
    'scopes': [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ],
    'knkh_sheet_url': 'https://docs.google.com/spreadsheets/d/1Z5mtkH-Yb4jg-2N_Fqr3i44Ta_YTFYHBoxw1YhB4RrQ/edit',
    'knkh_worksheet_name': 'MMB'
}

# Token Management Configuration
TOKEN_CONFIG = {
    'max_retry_attempts': 3,
    'retry_delay': 5,  # seconds
    'token_refresh_threshold': 300  # seconds before expiry
}

# Data Processing Configuration
PROCESSING_CONFIG = {
    'date_filter_start': '2024-01-01',  # Only process data from this date onwards
    'target_department': 'Nhà máy',     # Filter for this department only
    'output_sheet_name': 'Data_KNKH',  # Name of output sheet in SharePoint
    'debug_sheet_name': 'Debug_Info',  # Name of debug sheet
    'max_debug_records': 500           # Maximum records in debug sheet
}

# Column Mapping Configuration
COLUMN_MAPPING = {
    'output_columns': [
        'Mã ticket', 'Ngày tiếp nhận', 'Tỉnh', 'Ngày SX', 'Sản phẩm/Dịch vụ',
        'Số lượng (ly/hộp/chai/gói/hủ)', 'Nội dung phản hồi', 'Item', 'Tên sản phẩm', 'Tên sản phẩm ngắn',
        'SL pack/ cây lỗi', 'Tên lỗi', 'Line', 'Máy', 'Giờ',
        'QA', 'Tên Trưởng ca', 'Shift', 
        'Tháng sản xuất', 'Năm sản xuất', 'Tuần nhận khiếu nại', 'Tháng nhận khiếu nại', 'Năm nhận khiếu nại',
        'Bộ phận chịu trách nhiệm'
    ],
    'debug_columns': [
        'Mã ticket', 'Ngày SX', 'Item', 'Line', 'Giờ', 'QA', 'Tên Trưởng ca', 'debug_info'
    ],
    'rename_mapping': {
        'Line_extracted': 'Line',
        'Máy_extracted': 'Máy',
        'Giờ_extracted': 'Giờ',
        'QA_matched': 'QA',
        'Tên Trưởng ca_matched': 'Tên Trưởng ca',
        'Ngày tiếp nhận_formatted': 'Ngày tiếp nhận',
        'Ngày SX_formatted': 'Ngày SX'
    }
}

# Error Messages
ERROR_MESSAGES = {
    'sharepoint_auth_failed': 'SharePoint authentication failed. Please check tokens.',
    'google_auth_failed': 'Google Sheets authentication failed. Please check credentials.',
    'file_download_failed': 'Failed to download file from SharePoint.',
    'file_upload_failed': 'Failed to upload file to SharePoint.',
    'data_processing_failed': 'Data processing failed during integration.',
    'no_data_found': 'No data found to process.',
    'missing_columns': 'Required columns missing from data source.'
}

# Success Messages
SUCCESS_MESSAGES = {
    'sharepoint_auth_success': 'SharePoint authentication successful.',
    'google_auth_success': 'Google Sheets authentication successful.',
    'file_download_success': 'File downloaded successfully from SharePoint.',
    'file_upload_success': 'File uploaded successfully to SharePoint.',
    'data_processing_success': 'Data processing completed successfully.',
    'integration_complete': 'Hybrid integration completed successfully.'
}

# Logging Configuration
LOGGING_CONFIG = {
    'enable_debug': True,
    'log_format': '[{timestamp}] {level}: {message}',
    'progress_interval': 50  # Log progress every N rows
}
