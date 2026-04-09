# -*- coding: utf-8 -*-
import requests
import json
import io
import time
import pandas as pd
from datetime import datetime, timedelta
import config

# Cấu hình kỹ thuật riêng cho API HD01
BASE_URL = "https://pos.pvoil.vn/api"
REPORT_API_URL_PARAM = "https://pos.pvoil.vn/api/report/YW_D10E01afVaEde3FObd38dWYK3FLjo6Y7g23-OBPibWU-ZokCmyrLkxrVsbIFs"
COMMON_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Content-Type': 'application/json',
    'Referer': f'https://pos.pvoil.vn/{config.PVOIL_TENANT_CODE}/report/report-categories',
    'TenantCode': config.PVOIL_TENANT_CODE
}

def download_hd01_report(session, access_token, store_code, report_year, report_month):
    """Tải báo cáo HD01 cho một cửa hàng trong 1 tháng. Trả về DataFrame hoặc Exception."""
    headers = COMMON_HEADERS.copy()
    headers['Authorization'] = f'Bearer {access_token}'
    
    json_headers = headers.copy()
    json_headers['Content-Type'] = 'application/json; charset=UTF-8'
    
    try:
        # Bước 1: Khởi tạo Client
        client_response = session.post(f'{BASE_URL}/reports/clients', headers=json_headers, json={})
        print(f"[LOG][{store_code}] Bước 1 (Khởi tạo Client): Status {client_response.status_code}, Phản hồi: {client_response.text}")
        client_response.raise_for_status()
        client_id = client_response.json().get('clientId')
        
        # Xử lý thời gian
        target_date = datetime(int(report_year), int(report_month), 1)
        utc_date = target_date - timedelta(hours=7)
        time_str = utc_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        
        product_codes = []

        # Xây dựng cấu trúc PostObject cho HD01
        post_object_data = {
            "PostObject": {
                "InvoiceTypes": [],
                "IsMonth": "M",
                "FromDate": time_str,
                "Month": time_str,
                "ProductCode": None,
                "ProductCodes": product_codes,
                "CustomerCode": None,
                "DocumentNo": None,
                "InvoiceStatus": None,
                "ReferenceCode": None,
                "Sort": "asc",
                "SortBy": "InvoiceDate",
                "StationCodes": [store_code],
                "CompanyCode": "CT.0000"
            }
        }
        
        instance_payload = {
            "report": "HD01.trdp",
            "parameterValues": {
                "Url": REPORT_API_URL_PARAM,
                "PostObject": json.dumps(post_object_data),
                "Token": f'Bearer {access_token}',
                "TenantCode": config.PVOIL_TENANT_CODE
            }
        }
        
        # Bước 2: Yêu cầu tạo báo cáo (instances)
        instances_url = f'{BASE_URL}/reports/clients/{client_id}/instances'
        instances_response = session.post(instances_url, headers=json_headers, json=instance_payload)
        print(f"[LOG][{store_code}] Bước 2 (Yêu cầu Instance): Status {instances_response.status_code}, Phản hồi: {instances_response.text}")
        instances_response.raise_for_status()
        instance_id = instances_response.json().get('instanceId')
        
        # Bước 3: Định dạng xuất (documents)
        documents_url = f'{BASE_URL}/reports/clients/{client_id}/instances/{instance_id}/documents'
        excel_doc_payload = {"format": "XLSX"}
        excel_doc_response = session.post(documents_url, headers=json_headers, json=excel_doc_payload)
        print(f"[LOG][{store_code}] Bước 3 (Yêu cầu Document XLSX): Status {excel_doc_response.status_code}, Phản hồi: {excel_doc_response.text}")
        excel_doc_response.raise_for_status()
        excel_doc_id = excel_doc_response.json().get('documentId')
        
        # Bước 4: CHỜ PVOIL SINH FILE
        excel_info_url = f'{documents_url}/{excel_doc_id}/info'
        
        for step in range(200): 
            info_response = session.get(excel_info_url, headers=json_headers)
            try:
                info_data = info_response.json()
                is_ready = info_data.get('documentReady')
                
                # CHÈN LOG CHI TIẾT: In toàn bộ JSON nhận được từ máy chủ
                # Điều này giúp ta thấy nếu có trường 'error', 'message' hoặc 'exception' bên trong
                if step % 5 == 0 or is_ready or step == 0:
                    print(f"[LOG][{store_code}] Bước 4 (Đợi file - Lần {step+1}): {json.dumps(info_data)}")
                
                if info_response.ok and is_ready == True:
                    break
            except Exception as e:
                print(f"[LOG][{store_code}] Cảnh báo: Phản hồi không phải JSON ở lần {step+1}: {info_response.text[:200]}")
                
            time.sleep(3)
        else:
            print(f"[LOG][{store_code}] LỖI: Hết thời gian chờ 600 giây.")
            raise TimeoutError("Hết thời gian chờ (300s).")
            
        # Bước 5: Tải file
        final_download_url = f'{documents_url}/{excel_doc_id}'
        file_response = session.get(final_download_url, headers=headers)
        print(f"[LOG][{store_code}] Bước 5 (Tải file): Kết quả Status {file_response.status_code}")
        file_response.raise_for_status()
        
        return pd.read_excel(io.BytesIO(file_response.content), header=None)
        
    except Exception as e:
        print(f"[LOG][{store_code}] LỖI NGHIÊM TRỌNG: {str(e)}")
        return e