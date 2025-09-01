# -*- coding: utf-8 -*-
import json

# === TẢI CẤU HÌNH TỪ FILE JSON ===
def load_app_config():
    """Hàm đọc file app_config.json và trả về dữ liệu."""
    try:
        with open('app_config.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print("LỖI: Không tìm thấy file 'app_config.json'. Vui lòng tạo file này.")
        return {}
    except json.JSONDecodeError:
        print("LỖI: File 'app_config.json' có định dạng không hợp lệ.")
        return {}

# Tải dữ liệu một lần khi chương trình khởi động
_app_config = load_app_config()

# Gán dữ liệu vào các biến để các module khác sử dụng
STORE_INFO = _app_config.get("STORE_INFO", {})
TARGET_PRODUCTS_BH03 = _app_config.get("TARGET_PRODUCTS_BH03", [])
STORE_MAPPING_SSE_TO_POS = _app_config.get("STORE_MAPPING_SSE_TO_POS", {})
# THÊM DÒNG MỚI: Nạp cấu hình mapping cho đối soát tiền mặt
STORE_MAPPING_CASH_SSE_TO_POS = _app_config.get("STORE_MAPPING_CASH_SSE_TO_POS", {})


# === CẤU HÌNH CỐ ĐỊNH ===
# Các cấu hình này ít thay đổi nên vẫn giữ lại trong file .py

# --- Cấu hình PVOIL API ---
PVOIL_USERNAME = "taibaocao"
PVOIL_PASSWORD = "585173"
PVOIL_TENANT_CODE = "namdinh"

# --- Cấu hình Google ---
GOOGLE_DRIVE_ROOT_FOLDER_ID = '1HNq_IQA9f-_fSQbmqRgpTkpAyjoP0aZM'
CLIENT_SECRET_FILE = 'client_secret.json'
TOKEN_FILE = 'token.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# --- Cấu hình Logic ---
MAX_ATTEMPTS = 3
RETRY_DELAY_SECONDS = 5
