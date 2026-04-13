# -*- coding: utf-8 -*-
from google_auth_oauthlib.flow import InstalledAppFlow
import config

# Lấy trực tiếp danh sách SCOPES từ config.py (Đã bao gồm BigQuery)
flow = InstalledAppFlow.from_client_secrets_file(config.CLIENT_SECRET_FILE, config.SCOPES)
creds = flow.run_local_server(port=0, access_type="offline", include_granted_scopes="true", prompt="consent")

# Sử dụng hàm to_json() chính chủ của Google để không bao giờ bị lỗi định dạng múi giờ
with open(config.TOKEN_FILE, "w", encoding="utf-8") as f:
    f.write(creds.to_json())

print("OK: Đã tạo file token.json chuẩn xác 100%!")