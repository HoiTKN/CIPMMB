import os

# Microsoft Graph API Configuration từ IT Team
GRAPH_API_CONFIG = {
    'tenant_id': os.environ.get('TENANT_ID', '81060475-7e7f-4ede-8d8d-bf61f53ca528'),
    'client_id': os.environ.get('CLIENT_ID', '076541aa-c734-405e-8518-ed52b67f8cbd'),
    'client_secret': os.environ.get('CLIENT_SECRET'),  # Cần hỏi IT team
    'authority': 'https://login.microsoftonline.com/81060475-7e7f-4ede-8d8d-bf61f53ca528',
    'scopes': ['https://graph.microsoft.com/.default']
}

# SharePoint Site Configuration
SHAREPOINT_CONFIG = {
    'site_url': os.environ.get('SHAREPOINT_SITE_URL', 'https://masangroup.sharepoint.com/sites/MCH.MMB.QA'),
    'site_id': 'masangroup.sharepoint.com,sites,MCH.MMB.QA',  # Format for Graph API
}

# File Paths trong SharePoint (dựa trên cấu trúc bạn đã show)
FILE_PATHS = {
    'sample_id': {
        'folder': 'QATP MI/Báo cáo năm 2025',
        'filename': 'ID TP MI 2025.xlsx',
        'description': 'Sample ID Data'
    },
    'quality_daily': {
        'folder': 'QATP MI/Báo cáo năm 2025', 
        'filename': 'BC KPH chất lượng hàng ngày.xlsx',
        'description': 'Daily Quality Report'
    },
    'quality_weight': {
        'folder': 'QATP MI/Báo cáo năm 2025',
        'filename': 'BC trong lượng mi 2025.xlsx', 
        'description': 'Weight Quality Report'
    },
    'calibration': {
        'folder': 'QATP MI/Báo cáo năm 2025',
        'filename': 'Hiệu chuẩn cân hàng tháng xưởng mi 2025.xlsx',
        'description': 'Monthly Calibration Report'
    }
}

# Output Configuration
OUTPUT_CONFIG = {
    'local_output_dir': 'output',
    'processed_filename': 'Processed_QA_Data_{timestamp}.xlsx',
    'summary_filename': 'QA_Summary_{timestamp}.xlsx'
}

# QA Processing Configuration
QA_CONFIG = {
    'filter_date_from': '2024-01-01',  # Lọc dữ liệu từ ngày này
    'target_lines': [1, 2, 3, 4, 5, 6, 7, 8],  # Production lines cần phân tích
    'defect_threshold': 5,  # Ngưỡng cảnh báo defect
    'quality_metrics': ['defect_rate', 'production_efficiency', 'compliance_rate']
}
