# -*- coding: utf-8 -*-
from google.cloud import bigquery
import pandas as pd
import google_handler
import json
import os

# =====================================================================
# BẢNG ÁNH XẠ (MAPPING) TÊN CỘT
# BigQuery không chấp nhận dấu cách, dấu ngoặc hay tiếng Việt có dấu.
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
    """Khởi tạo BigQuery Client tận dụng chứng chỉ Google hiện có."""
    creds = google_handler.get_google_credentials()
    
    # Sửa lỗi: Lấy project_id từ file client_secret.json thay vì từ object creds (do user token không có project_id)
    project_id = None
    try:
        with open('client_secret.json', 'r', encoding='utf-8') as f:
            data = json.load(f)
            project_id = data.get('installed', {}).get('project_id') or data.get('web', {}).get('project_id')
    except Exception as e:
        print(f"[Cảnh báo] Không thể đọc client_secret.json: {e}")
        
    # Phương án dự phòng: Đọc từ biến môi trường nếu không có file
    if not project_id:
        project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
        
    if not project_id:
        raise ValueError("Lỗi: Không tìm thấy 'project_id' trong file client_secret.json.")
        
    return bigquery.Client(credentials=creds, project=project_id)

def init_bq_table():
    """Khởi tạo Bảng HD01_Master_Data với Partition (Phân vùng) và Cluster (Gom cụm)."""
    client = get_bq_client()
    dataset_id = f"{client.project}.pvoil_data"
    table_id = f"{dataset_id}.HD01_Master_Data"

    # Tạo Dataset nếu chưa tồn tại
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "asia-southeast1" # Đặt máy chủ tại Singapore
    dataset = client.create_dataset(dataset, exists_ok=True)

    # Khai báo cấu trúc các cột
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
    
    # 1. Kỹ thuật Partition: Chia ngăn kéo theo Tháng (Từ 1 đến 12)
    table.range_partitioning = bigquery.RangePartitioning(
        field="Thang_Bao_Cao",
        range_=bigquery.PartitionRange(start=1, end=13, interval=1)
    )
    # 2. Kỹ thuật Cluster: Gom nhóm theo Cửa hàng để tăng tốc quét
    table.clustering_fields = ["Ma_CHXD"]

    table = client.create_table(table, exists_ok=True)
    print(f"[BigQuery] Đã khởi tạo/Kiểm tra sẵn sàng Bảng: {table_id}")
    return table_id

def delete_old_data(store_code: str, report_month: int, report_year: int):
    """Xóa dữ liệu cũ (Dọn rác) trước khi ghi đè để đảm bảo không bị trùng lặp."""
    client = get_bq_client()
    table_id = f"{client.project}.pvoil_data.HD01_Master_Data"
    
    query = f"""
        DELETE FROM `{table_id}` 
        WHERE Ma_CHXD = '{store_code}' 
          AND Thang_Bao_Cao = {int(report_month)} 
          AND Nam_Bao_Cao = {int(report_year)}
    """
    try:
        job = client.query(query)
        job.result()  # Đợi thực thi xong
    except Exception as e:
        error_msg = str(e).lower()
        if "billing" in error_msg or "403" in error_msg:
            print(f"     [Cảnh báo BigQuery] Máy chủ Google đang chờ đồng bộ Billing. Hệ thống bỏ qua lệnh xóa và tự động bơm nối tiếp.")
        else:
            print(f"     [Lỗi BigQuery] Không thể dọn dữ liệu cũ: {e}")

def upload_dataframe(df: pd.DataFrame, store_code: str, report_month: int, report_year: int):
    """Hành động Bơm (Load) Dữ liệu lên Google Cloud."""
    if df is None or df.empty:
        return
    
    # 1. Bơm thêm 3 cột Nhận diện Hệ thống
    df['Nam_Bao_Cao'] = int(report_year)
    df['Thang_Bao_Cao'] = int(report_month)
    df['Mã_CHXD'] = store_code

    # 2. Đổi tên cột chuẩn mực (Tránh tiếng Việt)
    df_bq = df.rename(columns=COLUMN_MAPPING)
    
    # 3. Ép kiểu chuẩn: Xóa dấu phẩy nghìn, chuyển thành số thực (Float)
    float_cols = ['So_Luong', 'Don_Gia', 'Tien_Chua_Thue', 'Tien_Thue', 'Tong_Tien']
    for col in float_cols:
        if col in df_bq.columns:
            df_bq[col] = pd.to_numeric(
                df_bq[col].astype(str).str.replace(',', '').str.replace(' ', ''), 
                errors='coerce'
            ).fillna(0)
    
    # Chỉ giữ lại các cột có định nghĩa trong BQ
    valid_cols = list(COLUMN_MAPPING.values())
    df_bq = df_bq[[c for c in valid_cols if c in df_bq.columns]].copy()
    
    # 4. Bơm lên BigQuery
    client = get_bq_client()
    table_id = f"{client.project}.pvoil_data.HD01_Master_Data"
    
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = client.load_table_from_dataframe(df_bq, table_id, job_config=job_config)
    job.result()  # Chờ upload hoàn tất

def get_aggregated_data(report_month: int, report_year: int):
    """
    Sức mạnh SQL: Ra lệnh cho BigQuery khử trùng lặp và tính tổng toàn bộ dữ liệu.
    Trả về 2 bảng siêu nhỏ: 1 bảng tổng hợp theo Hàng Hóa, 1 bảng đếm theo Trạng Thái.
    """
    client = get_bq_client()
    table_id = f"`{client.project}.pvoil_data.HD01_Master_Data`"
    
    # BẢNG 1: Tổng hợp Hàng hóa & Tính toán Cột ảo (Chuyển thẳng, Nội bộ)
    sql_prod = f"""
        WITH Deduplicated AS (
            SELECT *
            FROM {table_id}
            WHERE Thang_Bao_Cao = {int(report_month)} AND Nam_Bao_Cao = {int(report_year)}
            -- THUẬT TOÁN MA THUẬT: Chỉ giữ lại hóa đơn được tải lên sau cùng (Khử trùng tuyệt đối)
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

    # BẢNG 2: Đếm số lượng hóa đơn theo Trạng Thái
    sql_status = f"""
        WITH Deduplicated AS (
            SELECT *
            FROM {table_id}
            WHERE Thang_Bao_Cao = {int(report_month)} AND Nam_Bao_Cao = {int(report_year)}
            QUALIFY ROW_NUMBER() OVER(PARTITION BY Ma_CHXD, Ky_Hieu, So_HD ORDER BY Ngay_Hoa_Don DESC) = 1
        ),
        ValidData AS (
            SELECT * FROM Deduplicated
            WHERE LOWER(TRIM(Trang_Thai_HD)) IN ('hoàn thành', 'thay thế', 'điều chỉnh tăng', 'điều chỉnh giảm', 'bị thay thế', 'bị điều chỉnh')
        )
        SELECT 
            Ten_CHXD,
            LOWER(TRIM(Trang_Thai_HD)) AS Trang_Thai_Lower,
            COUNT(1) AS So_Luong_Trang_Thai
        FROM ValidData
        GROUP BY Ten_CHXD, Trang_Thai_Lower
    """

    df_prod = client.query(sql_prod).to_dataframe()
    df_status = client.query(sql_status).to_dataframe()
    return df_prod, df_status