# -*- coding: utf-8 -*-
import requests
import json
import io
import time
import pandas as pd
import config

# Cấu hình kỹ thuật riêng cho API
BASE_URL = "https://pos.pvoil.vn/api"
LOGIN_URL_SUFFIX = "/AfKNb8Kab6mKH3Z9Ojiu4w_2oa0TIvXFP5CYPssYyGk="
REPORT_API_URL_PARAM = "https://pos.pvoil.vn/api/report/5HUzmtRCA47J3uA7OeFcbduoU4RVW-yxV7wtD5yPvpDeYuxy-_821uuX7-4hyvozXin4TYpeSaqZJLN6Yk6wHw=="
COMMON_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Content-Type': 'application/json',
    'Referer': f'https://pos.pvoil.vn/{config.PVOIL_TENANT_CODE}/login',
    'TenantCode': config.PVOIL_TENANT_CODE
}

def pvoil_login(session):
    """Thực hiện đăng nhập PVOIL và trả về token."""
    print(" Bắt đầu đăng nhập PVOIL...")
    payload = {"PostObject": {"UserName": config.PVOIL_USERNAME, "Password": config.PVOIL_PASSWORD, "grant_type": "password", "client_id": 1000}}
    try:
        response = session.post(BASE_URL + LOGIN_URL_SUFFIX, json=payload, headers=COMMON_HEADERS)
        response.raise_for_status()
        token = response.json().get('Data', {}).get('access_token')
        if not token:
            print(" Lỗi: Đăng nhập PVOIL thất bại, không nhận được token.")
            return None
        print(" Đăng nhập PVOIL thành công!")
        return token
    except requests.exceptions.RequestException as e:
        print(f" Lỗi nghiêm trọng khi đăng nhập PVOIL: {e}")
        return None

def download_bh03_report(session, access_token, store_code, report_date):
    """Tải báo cáo BH03 cho một cửa hàng và trả về DataFrame hoặc Exception."""
    headers = COMMON_HEADERS.copy()
    headers['Authorization'] = f'Bearer {access_token}'
    headers['Referer'] = f'https://pos.pvoil.vn/{config.PVOIL_TENANT_CODE}/report/report-categories'
    json_headers = headers.copy()
    json_headers['Content-Type'] = 'application/json; charset=UTF-8'
    try:
        client_response = session.post(f'{BASE_URL}/reports/clients', headers=json_headers, json={})
        client_response.raise_for_status()
        client_id = client_response.json().get('clientId')
        from_date = report_date.strftime('%Y-%m-%dT00:00:00.000Z')
        to_date = report_date.strftime('%Y-%m-%dT23:59:59.999Z')
        post_object_data = {"PostObject": {"IsMonth": "D", "FromDate": from_date, "ToDate": to_date, "StationCodes": [store_code], "CompanyCode": "CT.0000"}}
        instance_payload = {"report": "BH03.trdp", "parameterValues": {"Url": REPORT_API_URL_PARAM, "PostObject": json.dumps(post_object_data), "Token": f'Bearer {access_token}', "TenantCode": config.PVOIL_TENANT_CODE}}
        instances_url = f'{BASE_URL}/reports/clients/{client_id}/instances'
        instances_response = session.post(instances_url, headers=json_headers, json=instance_payload)
        instances_response.raise_for_status()
        instance_id = instances_response.json().get('instanceId')
        documents_url = f'{BASE_URL}/reports/clients/{client_id}/instances/{instance_id}/documents'
        excel_doc_payload = {"format": "XLSX"}
        excel_doc_response = session.post(documents_url, headers=json_headers, json=excel_doc_payload)
        excel_doc_response.raise_for_status()
        excel_doc_id = excel_doc_response.json().get('documentId')
        excel_info_url = f'{documents_url}/{excel_doc_id}/info'
        for _ in range(20):
            info_response = session.get(excel_info_url, headers=json_headers)
            if info_response.ok and info_response.json().get('documentReady'):
                break
            time.sleep(2)
        else:
            raise TimeoutError("Hết thời gian chờ file Excel.")
        final_download_url = f'{documents_url}/{excel_doc_id}'
        file_response = session.get(final_download_url, headers=headers)
        file_response.raise_for_status()
        return pd.read_excel(io.BytesIO(file_response.content), header=None)
    except Exception as e:
        return e
