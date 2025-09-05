# -*- coding: utf-8 -*-
import os
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.credentials import Credentials as OAuthCredentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import config

def get_google_credentials():
    """Xác thực Google bằng OAuth 2.0."""
    creds = None
    if os.path.exists(config.TOKEN_FILE):
        creds = OAuthCredentials.from_authorized_user_file(config.TOKEN_FILE, config.SCOPES)
    
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(config.CLIENT_SECRET_FILE, config.SCOPES)
            creds = flow.run_local_server(port=0)
        
        with open(config.TOKEN_FILE, 'w') as token:
            token.write(creds.to_json())
    return creds

def get_or_create_gdrive_folder(drive_service, folder_name, parent_id):
    """Tìm hoặc tạo một thư mục trên Google Drive và trả về ID."""
    query = f"name='{folder_name}' and '{parent_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    results = drive_service.files().list(q=query, fields="files(id)").execute()
    items = results.get('files', [])
    if items:
        return items[0]['id']
    else:
        print(f"   -> Thư mục '{folder_name}' chưa tồn tại, đang tạo mới...")
        file_metadata = {'name': folder_name, 'mimeType': 'application/vnd.google-apps.folder', 'parents': [parent_id]}
        folder = drive_service.files().create(body=file_metadata, fields='id').execute()
        return folder.get('id')

def get_or_create_gsheet(gspread_client, drive_service, file_name, folder_id):
    """Tìm hoặc tạo một file Google Sheet và trả về đối tượng Spreadsheet."""
    query = f"name='{file_name}' and '{folder_id}' in parents and mimeType='application/vnd.google-apps.spreadsheet' and trashed=false"
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    items = results.get('files', [])
    if items:
        spreadsheet_id = items[0]['id']
        print(f"   -> Đã tìm thấy file Google Sheet: '{file_name}'")
        return gspread_client.open_by_key(spreadsheet_id)
    else:
        print(f"   -> File '{file_name}' chưa tồn tại, đang tạo mới...")
        file_metadata = {'name': file_name, 'parents': [folder_id], 'mimeType': 'application/vnd.google-apps.spreadsheet'}
        file = drive_service.files().create(body=file_metadata, fields='id').execute()
        spreadsheet_id = file.get('id')
        return gspread_client.open_by_key(spreadsheet_id)

def upload_df_to_gsheet(spreadsheet, sheet_name, df):
    """Tải một DataFrame lên một sheet cụ thể trong Google Sheet."""
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
        worksheet.clear()
    except gspread.exceptions.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=100, cols=20)
    set_with_dataframe(worksheet, df)
    print(f"     ... Đã tải dữ liệu lên sheet '{sheet_name}' thành công.")
