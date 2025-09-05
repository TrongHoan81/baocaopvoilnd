# -*- coding: utf-8 -*-
"""
tasks.py
- Điều phối tải BH03 cho tất cả CHXD trong ngày.
- GIỮ NGUYÊN thuật toán đã ổn định (processor_bh03 đọc MỤC II/III/IV).
- Dùng Google Sheet DSKH cho ánh xạ mã khách.
- Tạo TongHopCongNo & ChiTietCongNo; reset STT theo cửa hàng.
- Cập nhật "Tổng hợp tháng ..." sau khi sinh BCBH.<dd.mm.yyyy>.
- BỔ SUNG:
  + Nếu không có mã khách -> ghi "Không tìm thấy mã khách" (đồng bộ thuật toán cũ).
  + Lọc bỏ các dòng có Customer_Name thuộc danh sách loại trừ (vd: "Công nợ chung") trước khi tổng hợp.
"""
from __future__ import annotations

from datetime import datetime
import time
import json
from typing import Dict, Any, List

import pandas as pd
import requests
import gspread
from googleapiclient.discovery import build
import unicodedata
import re

import config
import google_handler
from monthly_auto_update import update_monthly_after_download

# Tương thích cả 2 cách tổ chức project
try:
    from api_handlers import api_bh03
except Exception:  # pragma: no cover
    import api_bh03  # type: ignore

try:
    from data_processors import processor_bh03
except Exception:  # pragma: no cover
    import processor_bh03  # type: ignore


# ---- Constants ----
NO_CODE_PLACEHOLDER = "Không tìm thấy mã khách"
SKIP_NAMES = {"cong no chung"}  # so khớp theo tên đã chuẩn hoá (bỏ dấu, lower)


def _safe_int(v):
    try:
        return int(v)
    except Exception:
        return 0


def _sse(msg: str):
    return f"data: {msg}\n\n"


def _vn_normalize(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip().lower()
    s = "".join(ch for ch in unicodedata.normalize("NFD", s) if unicodedata.category(ch) != "Mn")
    return re.sub(r"[\s\._\-]+", " ", s).strip()


def download_report_generator(report_date: datetime):
    """Generator SSE cho endpoint /download_report_stream"""
    try:
        yield _sse("Bắt đầu quy trình...")
        # 1. Google Auth
        yield _sse("[1/6] Đang xác thực với Google...")
        creds = google_handler.get_google_credentials()
        gspread_client = gspread.authorize(creds)
        drive_service = build('drive', 'v3', credentials=creds)
        yield _sse("✔ Xác thực Google thành công.")

        # 2. Chuẩn bị Drive structure
        yield _sse("[2/6] Chuẩn bị cấu trúc Google Drive...")
        date_str_dmy = report_date.strftime('%d.%m.%Y')
        year_folder_id = google_handler.get_or_create_gdrive_folder(drive_service, f"Năm {report_date.year}", config.GOOGLE_DRIVE_ROOT_FOLDER_ID)
        month_folder_id = google_handler.get_or_create_gdrive_folder(drive_service, f"Tháng {report_date.month}", year_folder_id)
        spreadsheet_raw = google_handler.get_or_create_gsheet(gspread_client, drive_service, f"BH03.{date_str_dmy}", month_folder_id)
        spreadsheet_summary = google_handler.get_or_create_gsheet(gspread_client, drive_service, f"BCBH.{date_str_dmy}", month_folder_id)
        spreadsheet_debt = google_handler.get_or_create_gsheet(gspread_client, drive_service, f"CongNo.{date_str_dmy}", month_folder_id)
        yield _sse("✔ Cấu trúc Google Drive đã sẵn sàng.")

        # 2.b Đọc DSKH (1 lần)
        yield _sse("[2b] Đang nạp danh mục khách hàng (DSKH) từ Google Sheet...")
        dskh_df = google_handler.load_dskh_dataframe(gspread_client, drive_service, config.GOOGLE_DRIVE_ROOT_FOLDER_ID, filename="DSKH", sheet_name="DSKH")
        yield _sse(f"✔ Đã nạp DSKH: {len(dskh_df)} dòng.")

        # 3. Đăng nhập PVOIL
        yield _sse("[3/6] Đang đăng nhập PVOIL...")
        session = requests.Session()
        access_token = api_bh03.pvoil_login(session)
        if not access_token:
            raise ConnectionError("Đăng nhập PVOIL thất bại.")
        yield _sse("✔ Đăng nhập PVOIL thành công.")

        # 4. Tải & xử lý
        yield _sse("[4/6] Bắt đầu tải và xử lý dữ liệu...")
        app_cfg = config.load_app_config()
        stores_to_process: Dict[str, str] = dict(app_cfg.get("STORE_INFO", {}))

        successful_summaries: List[dict] = []
        all_debt_details: List[dict] = []

        for attempt in range(1, _safe_int(config.MAX_ATTEMPTS) + 1):
            if not stores_to_process:
                break
            yield _sse(f"  → Lượt thử {attempt}/{config.MAX_ATTEMPTS}...")
            failed_this_attempt: Dict[str, str] = {}

            for store_code, store_name in list(stores_to_process.items()):
                yield _sse(f"  -> Đang xử lý: {store_name}...")
                try:
                    report_df = api_bh03.download_bh03_report(session, access_token, store_code, report_date)
                    summary_row = processor_bh03.process_and_validate_bh03(report_df, store_name)
                    if summary_row:
                        successful_summaries.append(summary_row)
                        yield _sse("     ✔ Hợp lệ: Đã tổng hợp BCBH.")
                        # Lưu raw
                        google_handler.upload_df_to_gsheet(spreadsheet_raw, store_name, report_df)
                        # Tách công nợ (dùng DSKH)
                        debt_details = processor_bh03.process_debt_details(report_df, store_name, dskh_df=dskh_df)
                        if debt_details:
                            all_debt_details.extend(debt_details)
                    else:
                        yield _sse("     ❌ Báo cáo không hợp lệ hoặc rỗng.")
                        failed_this_attempt[store_code] = store_name
                except Exception as e:
                    failed_this_attempt[store_code] = store_name
                    yield _sse(f"     ❌ Lỗi khi xử lý {store_name}: {e}")

                time.sleep(0.2)

            stores_to_process = failed_this_attempt
            if stores_to_process:
                time.sleep(_safe_int(config.RETRY_DELAY_SECONDS))

        # 5. Lưu tổng hợp
        # 5a) BCBH
        if successful_summaries:
            df_summary = pd.DataFrame(successful_summaries)
            df_summary.insert(0, 'STT', range(1, 1 + len(df_summary)))
            google_handler.upload_df_to_gsheet(spreadsheet_summary, 'TongHopBCBH', df_summary)
            yield _sse("  -> Đã tải lên file tổng hợp BCBH.")
            try:
                worksheets = spreadsheet_summary.worksheets()
                if len(worksheets) > 1 and worksheets[0].title != 'TongHopBCBH':
                    spreadsheet_summary.del_worksheet(worksheets[0])
            except Exception:
                pass

        # 5b) Công nợ
        if all_debt_details:
            df_debt = pd.DataFrame(all_debt_details)
            for col in ["Store","Customer_Name","Customer_Code","Product","Quantity","Unit_Price","Debt"]:
                if col not in df_debt.columns:
                    df_debt[col] = ""

            # ---- LỌC BỎ KH thuộc danh sách loại trừ (vd: Công nợ chung) trước khi gộp ----
            df_debt["_norm_name"] = df_debt["Customer_Name"].astype(str).apply(_vn_normalize)
            df_debt = df_debt[~df_debt["_norm_name"].isin(SKIP_NAMES)].drop(columns=["_norm_name"])

            # --- TongHopCongNo ---
            agg = df_debt.groupby(['Store','Customer_Name'], as_index=False).agg(Debt=('Debt','sum'))
            codes = (
                df_debt.sort_values(['Store','Customer_Name','Customer_Code'], na_position='last')
                .groupby(['Store','Customer_Name'])['Customer_Code']
                .apply(lambda s: next((x for x in s if str(x).strip()!=''), NO_CODE_PLACEHOLDER))
                .reset_index()
            )
            tonghop = agg.merge(codes, on=['Store','Customer_Name'], how='left')

            rows_th = []
            for store, block in tonghop.sort_values(['Store','Customer_Name']).groupby('Store'):
                rows_th.append({"STT":"", "Tên Khách hàng": store, "Mã khách hàng":"", "Phát sinh nợ":""})
                stt = 1
                for _, r in block.iterrows():
                    code_val = r.get('Customer_Code', '') or NO_CODE_PLACEHOLDER
                    rows_th.append({
                        "STT": stt,
                        "Tên Khách hàng": r["Customer_Name"],
                        "Mã khách hàng": code_val,
                        "Phát sinh nợ": float(r["Debt"]) if pd.notna(r["Debt"]) else 0.0
                    })
                    stt += 1
            df_tonghop = pd.DataFrame(rows_th, columns=["STT","Tên Khách hàng","Mã khách hàng","Phát sinh nợ"])
            google_handler.upload_df_to_gsheet(spreadsheet_debt, 'TongHopCongNo', df_tonghop)

            # --- ChiTietCongNo ---
            rows_ct = []
            totals = df_debt.groupby(['Store','Customer_Name'], as_index=False).agg(Debt=('Debt','sum'))
            for store, df_store in df_debt.sort_values(['Store','Customer_Name']).groupby('Store'):
                rows_ct.append({"STT":"", "Tên Khách hàng": store, "Mã khách hàng":"", "Sản lượng":"", "Đơn giá":"", "Phát sinh nợ":""})
                stt = 1
                for customer, df_cus in df_store.groupby('Customer_Name'):
                    code = next((x for x in df_cus['Customer_Code'].tolist() if str(x).strip()!=''), NO_CODE_PLACEHOLDER)
                    total_debt = float(totals[(totals['Store']==store) & (totals['Customer_Name']==customer)]['Debt'].values[0])
                    rows_ct.append({
                        "STT": stt,
                        "Tên Khách hàng": customer,
                        "Mã khách hàng": code,
                        "Sản lượng": "",
                        "Đơn giá": "",
                        "Phát sinh nợ": total_debt
                    })
                    stt += 1
                    for _, r in df_cus.iterrows():
                        rows_ct.append({
                            "STT": "",
                            "Tên Khách hàng": r["Product"],
                            "Mã khách hàng": "",
                            "Sản lượng": float(r["Quantity"]) if pd.notna(r["Quantity"]) else 0.0,
                            "Đơn giá": float(r["Unit_Price"]) if pd.notna(r["Unit_Price"]) else 0.0,
                            "Phát sinh nợ": float(r["Debt"]) if pd.notna(r["Debt"]) else 0.0
                        })
            df_chitiet = pd.DataFrame(rows_ct, columns=["STT","Tên Khách hàng","Mã khách hàng","Sản lượng","Đơn giá","Phát sinh nợ"])
            google_handler.upload_df_to_gsheet(spreadsheet_debt, 'ChiTietCongNo', df_chitiet)
            yield _sse("  -> Đã tải lên file tổng hợp Công nợ (TongHopCongNo & ChiTietCongNo).")

        # 5c) Cập nhật tổng hợp tháng
        try:
            update_monthly_after_download(report_date)
            yield _sse("[5/6] ✔ Đã cập nhật 'Tổng hợp tháng ...' cho ngày này.")
        except Exception as e:
            yield _sse(f"[5/6] ⚠ Không thể cập nhật 'Tổng hợp tháng': {e}")

        # 6. Thông điệp kết
        success_count = len(successful_summaries)
        total_count = len(config.load_app_config().get('STORE_INFO', {}))
        message = f"Hoàn tất! Xử lý thành công {success_count}/{total_count} cửa hàng."
        if stores_to_process:
            failed_names = ', '.join(stores_to_process.values())
            message += f" | Các cửa hàng thất bại: {failed_names}"
        final_result = {"status": "success", "message": message}
        yield _sse(f"FINAL_MESSAGE:{json.dumps(final_result)}")

    except Exception as e:
        print(f"Lỗi nghiêm trọng trong quá trình tải báo cáo: {e}")
        error_result = {"status": "error", "message": f"Đã xảy ra lỗi không mong muốn: {str(e)}"}
        yield _sse(f"ERROR:{json.dumps(error_result)}")
