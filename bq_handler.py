# -*- coding: utf-8 -*-
from google.cloud import bigquery
import pandas as pd
import google_handler
import json
import os

# =====================================================================
# BẢNG ÁNH XẠ (MAPPING) TÊN CỘT
# =====================================================================
COLUMN_MAPPING = {
    'Nam_Bao_Cao': 'Nam_Bao_Cao',
    'Thang_Bao_Cao': 'Thang_Bao_Cao',
    'Mã_CHXD': 'Ma_CHXD',
    'Tên CHXD': 'Ten_CHXD',
    'Ký hiệu': 'Ky_Hieu',
    'Số HĐ': 'So_HD',
    'Ngày hóa đơn': 'Ngay_Hoa_Don',
    'Trạng thái HĐ': 'Trang_Thai_HD',
    'Loại HĐ': 'Loai_HD',
    'Mã tra cứu': 'Ma_Tra_Cuu',
    'Số GD': 'So_GD',
    'Mã khách hàng': 'Ma_Khach_Hang',
    'Tên khách hàng': 'Ten_Khach_Hang',
    'Mã số thuế': 'Ma_So_Thue',
    'Hàng hóa': 'Hang_Hoa',
    'ĐVT': 'DVT',
    'Số lượng': 'So_Luong',
    'Đơn giá': 'Don_Gia',
    'Thành tiền (chưa thuế)': 'Tien_Chua_Thue',
    'Tiền thuế': 'Tien_Thue',
    'Tổng tiền thanh toán': 'Tong_Tien'
}

def get_bq_client():
    creds = google_handler.get_google_credentials()
    project_id = None
    try:
        with open('client_secret.json', 'r', encoding='utf-8') as f:
            data = json.load(f)
            project_id = data.get('installed', {}).get('project_id') or data.get('web', {}).get('project_id')
    except Exception:
        pass
        
    if not project_id:
        project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
        
    if not project_id:
        raise ValueError("Lỗi: Không tìm thấy 'project_id' trong file client_secret.json.")
        
    return bigquery.Client(credentials=creds, project=project_id)

def init_bq_table():
    client = get_bq_client()
    dataset_id = f"{client.project}.pvoil_data"
    table_id = f"{dataset_id}.HD01_Master_Data"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "asia-southeast1"
    dataset = client.create_dataset(dataset, exists_ok=True)

    schema = [
        bigquery.SchemaField("Nam_Bao_Cao", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("Thang_Bao_Cao", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("Ma_CHXD", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("Ten_CHXD", "STRING"),
        bigquery.SchemaField("Ky_Hieu", "STRING"),
        bigquery.SchemaField("So_HD", "STRING"),
        bigquery.SchemaField("Ngay_Hoa_Don", "STRING"),
        bigquery.SchemaField("Trang_Thai_HD", "STRING"),
        bigquery.SchemaField("Loai_HD", "STRING"),
        bigquery.SchemaField("Ma_Tra_Cuu", "STRING"),
        bigquery.SchemaField("So_GD", "STRING"),
        bigquery.SchemaField("Ma_Khach_Hang", "STRING"),
        bigquery.SchemaField("Ten_Khach_Hang", "STRING"),
        bigquery.SchemaField("Ma_So_Thue", "STRING"),
        bigquery.SchemaField("Hang_Hoa", "STRING"),
        bigquery.SchemaField("DVT", "STRING"),
        bigquery.SchemaField("So_Luong", "FLOAT"),
        bigquery.SchemaField("Don_Gia", "FLOAT"),
        bigquery.SchemaField("Tien_Chua_Thue", "FLOAT"),
        bigquery.SchemaField("Tien_Thue", "FLOAT"),
        bigquery.SchemaField("Tong_Tien", "FLOAT"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    table.range_partitioning = bigquery.RangePartitioning(
        field="Thang_Bao_Cao",
        range_=bigquery.PartitionRange(start=1, end=13, interval=1)
    )
    table.clustering_fields = ["Ma_CHXD"]
    client.create_table(table, exists_ok=True)
    return table_id

def delete_old_data(store_code, report_month, report_year):
    client = get_bq_client()
    table_id = f"{client.project}.pvoil_data.HD01_Master_Data"
    query = f"DELETE FROM `{table_id}` WHERE Ma_CHXD = '{store_code}' AND Thang_Bao_Cao = {int(report_month)} AND Nam_Bao_Cao = {int(report_year)}"
    try:
        client.query(query).result()
    except Exception as e:
        if "billing" in str(e).lower():
            print("     [Cảnh báo BigQuery] Billing chưa enable, tự động Append.")
        else:
            print(f"     [Lỗi BigQuery] {e}")

def upload_dataframe(df, store_code, report_month, report_year):
    if df is None or df.empty: return
    df['Nam_Bao_Cao'] = int(report_year)
    df['Thang_Bao_Cao'] = int(report_month)
    df['Mã_CHXD'] = store_code
    df_bq = df.rename(columns=COLUMN_MAPPING)
    float_cols = ['So_Luong', 'Don_Gia', 'Tien_Chua_Thue', 'Tien_Thue', 'Tong_Tien']
    for col in float_cols:
        if col in df_bq.columns:
            df_bq[col] = pd.to_numeric(df_bq[col].astype(str).str.replace(',', '').str.replace(' ', ''), errors='coerce').fillna(0)
    valid_cols = list(COLUMN_MAPPING.values())
    df_bq = df_bq[[c for c in valid_cols if c in df_bq.columns]].copy()
    client = get_bq_client()
    table_id = f"{client.project}.pvoil_data.HD01_Master_Data"
    client.load_table_from_dataframe(df_bq, table_id, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")).result()

def get_aggregated_data(report_month, report_year):
    client = get_bq_client()
    table_id = f"`{client.project}.pvoil_data.HD01_Master_Data`"
    sql_prod = f"""
        WITH Deduplicated AS (
            SELECT * FROM {table_id}
            WHERE Thang_Bao_Cao = {int(report_month)} AND Nam_Bao_Cao = {int(report_year)}
            QUALIFY ROW_NUMBER() OVER(PARTITION BY Ma_CHXD, Ky_Hieu, So_HD ORDER BY Ngay_Hoa_Don DESC) = 1
        ),
        ValidData AS (
            SELECT * FROM Deduplicated
            WHERE LOWER(TRIM(Trang_Thai_HD)) IN ('hoàn thành', 'thay thế', 'điều chỉnh tăng', 'điều chỉnh giảm', 'bị thay thế', 'bị điều chỉnh')
        )
        SELECT 
            Ten_CHXD,
            CASE 
                WHEN TRIM(Hang_Hoa) IN ('Xăng RON95 Mức 3', 'Xăng E5 RON92 Mức 2', 'Dầu Điêzen 0,001S Mức 5', 'Dầu Điêzen 0,05S Mức 2') THEN TRIM(Hang_Hoa)
                ELSE 'Mặt hàng khác'
            END AS Nhom_Hang,
            COUNT(1) AS So_Luong_Dong,
            SUM(Tien_Chua_Thue) AS Tien_Chua_Thue,
            SUM(Tien_Thue) AS Tien_Thue,
            SUM(Tong_Tien) AS Tong_Thanh_Toan,
            SUM(CASE WHEN LOWER(Loai_HD) LIKE '%chuyển thẳng%' THEN So_Luong ELSE 0 END) AS SL_ChuyenThang,
            SUM(CASE WHEN Ma_So_Thue LIKE '%0600759399%' THEN So_Luong ELSE 0 END) AS SL_NoiBo
        FROM ValidData
        GROUP BY Ten_CHXD, Nhom_Hang
    """
    sql_status = f"""
        WITH Deduplicated AS (
            SELECT * FROM {table_id}
            WHERE Thang_Bao_Cao = {int(report_month)} AND Nam_Bao_Cao = {int(report_year)}
            QUALIFY ROW_NUMBER() OVER(PARTITION BY Ma_CHXD, Ky_Hieu, So_HD ORDER BY Ngay_Hoa_Don DESC) = 1
        )
        SELECT 
            Ten_CHXD,
            LOWER(TRIM(Trang_Thai_HD)) AS Trang_Thai_Lower,
            COUNT(1) AS So_Luong_Trang_Thai
        FROM Deduplicated
        WHERE LOWER(TRIM(Trang_Thai_HD)) IN ('hoàn thành', 'thay thế', 'điều chỉnh tăng', 'điều chỉnh giảm', 'bị thay thế', 'bị điều chỉnh')
        GROUP BY Ten_CHXD, Trang_Thai_Lower
    """
    return client.query(sql_prod).to_dataframe(), client.query(sql_status).to_dataframe()

def get_raw_hd01_data(month, year, store_code='ALL'):
    """Tính năng mới: Truy vấn 100% cột dữ liệu thô từ BigQuery."""
    client = get_bq_client()
    table_id = f"`{client.project}.pvoil_data.HD01_Master_Data`"
    where_clause = f"WHERE Thang_Bao_Cao = {int(month)} AND Nam_Bao_Cao = {int(year)}"
    if store_code and store_code != 'ALL':
        where_clause += f" AND Ma_CHXD = '{store_code}'"
    query = f"""
        SELECT * FROM {table_id}
        {where_clause}
        QUALIFY ROW_NUMBER() OVER(PARTITION BY Ma_CHXD, Ky_Hieu, So_HD ORDER BY Ngay_Hoa_Don DESC) = 1
        ORDER BY Ten_CHXD, Ngay_Hoa_Don, So_HD
    """
    return client.query(query).to_dataframe()