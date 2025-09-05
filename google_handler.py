# -*- coding: utf-8 -*-
"""
google_handler.py
- GIỮ NGUYÊN các hàm cũ đang dùng.
- Bổ sung helper mở/đọc DSKH và sửa escape Drive query để tránh SyntaxError.
"""
from __future__ import annotations

import os
import gspread
import pandas as pd
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
        with open(config.TOKEN_FILE, "w", encoding="utf-8") as f:
            f.write(creds.to_json())
    return creds


# ---- Drive helpers ----
def _search_file_in_folder(drive_service, name: str, parent_id: str, mime: str | None = None):
    """Tìm file theo tên trong 1 thư mục. Trả về file id nếu thấy, None nếu không."""
    safe_name = name.replace("'", "\\'")
    query_parts = [f"name = '{safe_name}'", f"'{parent_id}' in parents", "trashed = false"]
    if mime:
        query_parts.append(f"mimeType = '{mime}'")
    query = " and ".join(query_parts)
    resp = drive_service.files().list(q=query, fields="files(id, name)", pageSize=1).execute()
    files = resp.get("files", [])
    return files[0]["id"] if files else None


def get_or_create_gdrive_folder(drive_service, folder_name: str, parent_id: str | None = None) -> str:
    parent = parent_id or "root"
    folder_id = _search_file_in_folder(drive_service, folder_name, parent, mime="application/vnd.google-apps.folder")
    if folder_id:
        return folder_id
    metadata = {"name": folder_name, "mimeType": "application/vnd.google-apps.folder", "parents": [parent]}
    folder = drive_service.files().create(body=metadata, fields="id").execute()
    return folder["id"]


def get_or_create_gsheet(gspread_client, drive_service, file_name: str, folder_id: str):
    ss_id = _search_file_in_folder(drive_service, file_name, folder_id, mime="application/vnd.google-apps.spreadsheet")
    if ss_id:
        return gspread_client.open_by_key(ss_id)
    metadata = {"name": file_name, "parents": [folder_id], "mimeType": "application/vnd.google-apps.spreadsheet"}
    file = drive_service.files().create(body=metadata, fields="id").execute()
    return gspread_client.open_by_key(file["id"])


def open_gsheet_in_folder(gspread_client, drive_service, file_name: str, parent_folder_id: str):
    ss_id = _search_file_in_folder(drive_service, file_name, parent_folder_id, mime="application/vnd.google-apps.spreadsheet")
    return gspread_client.open_by_key(ss_id) if ss_id else None


# ---- Sheet helpers ----
def upload_df_to_gsheet(spreadsheet, sheet_name: str, df: pd.DataFrame):
    if spreadsheet is None:
        raise ValueError("spreadsheet is None")
    try:
        ws = spreadsheet.worksheet(sheet_name)
        ws.clear()
    except gspread.exceptions.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=sheet_name, rows=max(100, len(df) + 10), cols=max(20, len(df.columns) + 5))
    set_with_dataframe(ws, df)
    print(f"     ... Đã tải dữ liệu lên sheet '{sheet_name}' thành công.")


def read_worksheet_as_df(spreadsheet, sheet_name: str) -> pd.DataFrame:
    ws = spreadsheet.worksheet(sheet_name)
    records = ws.get_all_records()
    return pd.DataFrame.from_records(records) if records else pd.DataFrame()


# ---- DSKH convenience ----
def load_dskh_dataframe(gspread_client, drive_service, root_folder_id: str, filename: str = "DSKH", sheet_name: str = "DSKH") -> pd.DataFrame:
    ss = open_gsheet_in_folder(gspread_client, drive_service, filename, root_folder_id)
    if ss is None:
        raise FileNotFoundError(f"Không tìm thấy file Google Sheet '{filename}' trong thư mục gốc.")
    return read_worksheet_as_df(ss, sheet_name)
