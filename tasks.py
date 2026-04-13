# -*- coding: utf-8 -*-
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
import bq_handler  # THÊM MỚI: Import module xử lý BigQuery

try:
    from api_handlers import api_bh03, api_hd01
except Exception:  
    import api_bh03, api_hd01  

try:
    from data_processors import processor_bh03, processor_hd01
except Exception:  
    import processor_bh03, processor_hd01 

NO_CODE_PLACEHOLDER = "Không tìm thấy mã khách"
SKIP_NAMES = {"cong no chung"}

def _safe_int(v):
    try: return int(v)
    except Exception: return 0

def _sse(msg: str):
    return f"data: {msg}\n\n"

def _vn_normalize(s: str) -> str:
    if s is None: return ""
    s = str(s).strip().lower()
    s = "".join(ch for ch in unicodedata.normalize("NFD", s) if unicodedata.category(ch) != "Mn")
    return re.sub(r"[\s\._\-]+", " ", s).strip()

def download_report_generator(report_date: datetime, report_type="BH03", station_code_filter=None, report_year="", report_month=""):
    try:
        yield _sse(f"Bắt đầu quy trình tải báo cáo {report_type}...")
        
        yield _sse("[1/x] Đang xác thực với Google...")
        creds = google_handler.get_google_credentials()
        gspread_client = gspread.authorize(creds)
        drive_service = build('drive', 'v3', credentials=creds)
        yield _sse("✔ Xác thực Google thành công.")

        yield _sse("[2/x] Đang đăng nhập PVOIL...")
        session = requests.Session()
        access_token = api_bh03.pvoil_login(session)
        if not access_token: raise ConnectionError("Đăng nhập PVOIL thất bại.")
        yield _sse("✔ Đăng nhập PVOIL thành công.")

        app_cfg = config.load_app_config()
        all_stores = dict(app_cfg.get("STORE_INFO", {}))
        
        if station_code_filter and station_code_filter in all_stores:
            stores_to_process = {station_code_filter: all_stores[station_code_filter]}
        else:
            stores_to_process = dict(all_stores)

        # ==========================================
        # LUỒNG 1: HD01 - KIẾN TRÚC GHI TRỰC TIẾP BIGQUERY
        # ==========================================
        if report_type == "HD01":
            yield _sse("[3/3] Bắt đầu tải dữ liệu BKHĐ (Ghi trực tiếp vào BigQuery)...")
            if not report_year or not report_month: raise ValueError("Thiếu tham số Năm/Tháng.")
            
            # KHỞI TẠO/KIỂM TRA BẢNG BIGQUERY
            bq_handler.init_bq_table()
            
            # --- TRƯỜNG HỢP 1: TẢI "TẤT CẢ CỬA HÀNG" ---
            if station_code_filter == 'ALL' or not station_code_filter:
                success_count = 0
                failed_stores = []
                stores_list = list(stores_to_process.items())
                total_stores = len(stores_list)

                for idx, (store_code, store_name) in enumerate(stores_list, 1):
                    yield _sse(f"➤ [{idx}/{total_stores}] Đang tải & bơm dữ liệu: {store_name} lên BigQuery...")
                    
                    is_success = False
                    for attempt in range(1, _safe_int(config.MAX_ATTEMPTS) + 1):
                        try:
                            # 1. Tải Data thô
                            df_raw = api_hd01.download_hd01_report(session, access_token, store_code, report_year, report_month)
                            if isinstance(df_raw, Exception): raise df_raw
                            
                            # 2. Làm sạch
                            df_clean = processor_hd01.process_hd01(df_raw, store_name)
                            
                            if not df_clean.empty:
                                if 'Ngày hóa đơn' in df_clean.columns: df_clean['Ngày hóa đơn'] = df_clean['Ngày hóa đơn'].astype(str).str.slice(0, 10)
                                
                                # 3. Xóa dữ liệu cũ (Dọn rác/Idempotent) & Bơm dữ liệu mới
                                bq_handler.delete_old_data(store_code, report_month, report_year)
                                bq_handler.upload_dataframe(df_clean, store_code, report_month, report_year)
                                
                                yield _sse(f"     ✔ Đã bơm thành công {len(df_clean)} dòng lên BigQuery.")
                                success_count += 1
                            else: 
                                # Nếu file rỗng thì vẫn phải xóa data cũ (trường hợp tháng trước có, tháng này PVOIL xóa)
                                bq_handler.delete_old_data(store_code, report_month, report_year)
                                yield _sse("     ❌ Không có dữ liệu.")
                            is_success = True
                            break 
                        except Exception as e:
                            if attempt < _safe_int(config.MAX_ATTEMPTS): yield _sse(f"     ⚠ Lỗi: {e}. Thử lại lần {attempt+1}...")
                            else: yield _sse(f"     ❌ Lỗi tải file: {e}")
                    
                    if not is_success: failed_stores.append(store_name)

                msg = f"Hoàn tất! Đã bơm thành công {success_count}/{total_stores} CHXD lên BigQuery."
                if failed_stores: msg += f" | Thất bại: {', '.join(failed_stores)}"
                yield _sse(f"FINAL_MESSAGE:{json.dumps({'status': 'success', 'message': msg})}")
                return

            # --- TRƯỜNG HỢP 2: TẢI "LẺ 1 CỬA HÀNG" (SMART UPDATE) ---
            else:
                store_code = station_code_filter
                store_name = all_stores[store_code]
                yield _sse(f"➤ ĐANG CẬP NHẬT DỮ LIỆU LÊN BIGQUERY CHO: [{store_name}]...")

                is_success = False
                for attempt in range(1, _safe_int(config.MAX_ATTEMPTS) + 1):
                    try:
                        # 1. Tải Data thô
                        df_raw = api_hd01.download_hd01_report(session, access_token, store_code, report_year, report_month)
                        if isinstance(df_raw, Exception): raise df_raw
                        
                        # 2. Làm sạch
                        df_clean = processor_hd01.process_hd01(df_raw, store_name)
                        
                        if not df_clean.empty:
                            if 'Ngày hóa đơn' in df_clean.columns: df_clean['Ngày hóa đơn'] = df_clean['Ngày hóa đơn'].astype(str).str.slice(0, 10)
                            
                            # 3. Xóa data cũ & Bơm lên BQ
                            bq_handler.delete_old_data(store_code, report_month, report_year)
                            bq_handler.upload_dataframe(df_clean, store_code, report_month, report_year)
                            
                            yield _sse(f"     ✔ Cập nhật thành công {len(df_clean)} dòng.")
                            is_success = True
                        else: 
                            bq_handler.delete_old_data(store_code, report_month, report_year)
                            yield _sse("     ❌ Không có dữ liệu.")
                        break 
                    except Exception as e:
                        if attempt < _safe_int(config.MAX_ATTEMPTS): yield _sse(f"     ⚠ Lỗi: {e}. Thử lại lần {attempt+1}...")
                        else: yield _sse(f"     ❌ Lỗi tải file: {e}")

                if is_success: yield _sse(f"FINAL_MESSAGE:{json.dumps({'status': 'success', 'message': f'Đã cập nhật BigQuery cho {store_name}!'})}")
                else: yield _sse(f"FINAL_MESSAGE:{json.dumps({'status': 'error', 'message': f'Cập nhật thất bại cho {store_name}.'})}")
                return

        # ==========================================
        # LUỒNG 2: BH03 (GIỮ NGUYÊN HOÀN TOÀN)
        # ==========================================
        else:
            yield _sse("[3/6] Đang nạp danh mục khách hàng (DSKH) từ Google Sheet...")
            dskh_df = google_handler.load_dskh_dataframe(gspread_client, drive_service, config.GOOGLE_DRIVE_ROOT_FOLDER_ID, filename="DSKH", sheet_name="DSKH")
            yield _sse(f"✔ Đã nạp DSKH: {len(dskh_df)} dòng.")

            yield _sse("[4/6] Chuẩn bị cấu trúc Google Drive cho BH03...")
            date_str_dmy = report_date.strftime('%d.%m.%Y')
            year_folder_id = google_handler.get_or_create_gdrive_folder(drive_service, f"Năm {report_date.year}", config.GOOGLE_DRIVE_ROOT_FOLDER_ID)
            month_folder_id = google_handler.get_or_create_gdrive_folder(drive_service, f"Tháng {report_date.month}", year_folder_id)
            spreadsheet_raw = google_handler.get_or_create_gsheet(gspread_client, drive_service, f"BH03.{date_str_dmy}", month_folder_id)
            spreadsheet_summary = google_handler.get_or_create_gsheet(gspread_client, drive_service, f"BCBH.{date_str_dmy}", month_folder_id)
            spreadsheet_debt = google_handler.get_or_create_gsheet(gspread_client, drive_service, f"CongNo.{date_str_dmy}", month_folder_id)
            yield _sse("✔ Cấu trúc Google Drive đã sẵn sàng.")

            yield _sse("[5/6] Bắt đầu tải và xử lý dữ liệu BH03...")
            successful_summaries: List[dict] = []
            all_debt_details: List[dict] = []

            for attempt in range(1, _safe_int(config.MAX_ATTEMPTS) + 1):
                if not stores_to_process: break
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
                            google_handler.upload_df_to_gsheet(spreadsheet_raw, store_name, report_df)
                            debt_details = processor_bh03.process_debt_details(report_df, store_name, dskh_df=dskh_df)
                            if debt_details: all_debt_details.extend(debt_details)
                        else:
                            yield _sse("     ❌ Báo cáo không hợp lệ hoặc rỗng.")
                            failed_this_attempt[store_code] = store_name
                    except Exception as e:
                        failed_this_attempt[store_code] = store_name
                        yield _sse(f"     ❌ Lỗi khi xử lý {store_name}: {e}")
                    time.sleep(0.2)

                stores_to_process = failed_this_attempt
                if stores_to_process: time.sleep(_safe_int(config.RETRY_DELAY_SECONDS))

            if successful_summaries:
                df_summary = pd.DataFrame(successful_summaries)
                df_summary.insert(0, 'STT', range(1, 1 + len(df_summary)))
                if station_code_filter and station_code_filter != "ALL":
                    try:
                        existing_summary = google_handler.read_worksheet_as_df(spreadsheet_summary, 'TongHopBCBH')
                        if not existing_summary.empty and 'Tên CHXD' in existing_summary.columns:
                            store_name_to_replace = all_stores[station_code_filter]
                            existing_summary = existing_summary[existing_summary['Tên CHXD'] != store_name_to_replace]
                            df_summary = pd.concat([existing_summary, df_summary], ignore_index=True)
                            df_summary = df_summary.drop(columns=['STT'], errors='ignore')
                            df_summary.insert(0, 'STT', range(1, 1 + len(df_summary)))
                    except Exception: pass
                google_handler.upload_df_to_gsheet(spreadsheet_summary, 'TongHopBCBH', df_summary)
                yield _sse("  -> Đã tải lên file tổng hợp BCBH.")
                try:
                    worksheets = spreadsheet_summary.worksheets()
                    if len(worksheets) > 1 and worksheets[0].title != 'TongHopBCBH':
                        spreadsheet_summary.del_worksheet(worksheets[0])
                except Exception: pass

            if all_debt_details:
                df_debt = pd.DataFrame(all_debt_details)
                for col in ["Store","Customer_Name","Customer_Code","Product","Quantity","Unit_Price","Debt"]:
                    if col not in df_debt.columns: df_debt[col] = ""
                df_debt["_norm_name"] = df_debt["Customer_Name"].astype(str).apply(_vn_normalize)
                df_debt = df_debt[~df_debt["_norm_name"].isin(SKIP_NAMES)].drop(columns=["_norm_name"])

                agg = df_debt.groupby(['Store','Customer_Name'], as_index=False).agg(Debt=('Debt','sum'))
                codes = (df_debt.sort_values(['Store','Customer_Name','Customer_Code'], na_position='last')
                    .groupby(['Store','Customer_Name'])['Customer_Code']
                    .apply(lambda s: next((x for x in s if str(x).strip()!=''), NO_CODE_PLACEHOLDER))
                    .reset_index())
                tonghop = agg.merge(codes, on=['Store','Customer_Name'], how='left')

                rows_th = []
                for store, block in tonghop.sort_values(['Store','Customer_Name']).groupby('Store'):
                    rows_th.append({"STT":"", "Tên Khách hàng": store, "Mã khách hàng":"", "Phát sinh nợ":""})
                    stt = 1
                    for _, r in block.iterrows():
                        code_val = r.get('Customer_Code', '') or NO_CODE_PLACEHOLDER
                        rows_th.append({"STT": stt, "Tên Khách hàng": r["Customer_Name"], "Mã khách hàng": code_val, "Phát sinh nợ": float(r["Debt"]) if pd.notna(r["Debt"]) else 0.0})
                        stt += 1
                df_tonghop = pd.DataFrame(rows_th, columns=["STT","Tên Khách hàng","Mã khách hàng","Phát sinh nợ"])
                google_handler.upload_df_to_gsheet(spreadsheet_debt, 'TongHopCongNo', df_tonghop)

                rows_ct = []
                totals = df_debt.groupby(['Store','Customer_Name'], as_index=False).agg(Debt=('Debt','sum'))
                for store, df_store in df_debt.sort_values(['Store','Customer_Name']).groupby('Store'):
                    rows_ct.append({"STT":"", "Tên Khách hàng": store, "Mã khách hàng":"", "Sản lượng":"", "Đơn giá":"", "Phát sinh nợ":""})
                    stt = 1
                    for customer, df_cus in df_store.groupby('Customer_Name'):
                        code = next((x for x in df_cus['Customer_Code'].tolist() if str(x).strip()!=''), NO_CODE_PLACEHOLDER)
                        total_debt = float(totals[(totals['Store']==store) & (totals['Customer_Name']==customer)]['Debt'].values[0])
                        rows_ct.append({"STT": stt, "Tên Khách hàng": customer, "Mã khách hàng": code, "Sản lượng": "", "Đơn giá": "", "Phát sinh nợ": total_debt})
                        stt += 1
                        for _, r in df_cus.iterrows():
                            rows_ct.append({"STT": "", "Tên Khách hàng": r["Product"], "Mã khách hàng": "", "Sản lượng": float(r["Quantity"]) if pd.notna(r["Quantity"]) else 0.0, "Đơn giá": float(r["Unit_Price"]) if pd.notna(r["Unit_Price"]) else 0.0, "Phát sinh nợ": float(r["Debt"]) if pd.notna(r["Debt"]) else 0.0})
                df_chitiet = pd.DataFrame(rows_ct, columns=["STT","Tên Khách hàng","Mã khách hàng","Sản lượng","Đơn giá","Phát sinh nợ"])
                google_handler.upload_df_to_gsheet(spreadsheet_debt, 'ChiTietCongNo', df_chitiet)
                yield _sse("  -> Đã tải lên file tổng hợp Công nợ.")

            try:
                update_monthly_after_download(report_date)
                yield _sse("[6/6] ✔ Đã cập nhật 'Tổng hợp tháng ...' cho ngày này.")
            except Exception as e:
                yield _sse(f"[6/6] ⚠ Không thể cập nhật 'Tổng hợp tháng': {e}")

            success_count = len(successful_summaries)
            total_count = len(config.load_app_config().get('STORE_INFO', {})) if not station_code_filter else 1
            message = f"Hoàn tất! Xử lý thành công {success_count}/{total_count} cửa hàng."
            if stores_to_process:
                message += f" | Các cửa hàng thất bại: {', '.join(stores_to_process.values())}"
            yield _sse(f"FINAL_MESSAGE:{json.dumps({'status': 'success', 'message': message})}")

    except Exception as e:
        print(f"Lỗi nghiêm trọng: {e}")
        yield _sse(f"ERROR:{json.dumps({'status': 'error', 'message': f'Lỗi: {str(e)}'})}")