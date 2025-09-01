# -*- coding: utf-8 -*-
from datetime import datetime
import requests
import time
import pandas as pd
import json
from monthly_auto_update import update_monthly_after_download


# Import các thư viện cần thiết
import gspread
from googleapiclient.discovery import build

# Import các module đã được tách
import config
import google_handler
from api_handlers import api_bh03
from data_processors import processor_bh03

def download_report_generator(report_date):
    """
    Generator function chứa logic tải báo cáo.
    Hàm này sẽ `yield` các chuỗi log trong quá trình chạy.
    """
    try:
        yield "data: Bắt đầu quy trình...\n\n"
        
        # 1. Xác thực Google
        yield "data: [1/5] Đang xác thực với Google...\n\n"
        creds = google_handler.get_google_credentials()
        gspread_client = gspread.authorize(creds)
        drive_service = build('drive', 'v3', credentials=creds)
        yield "data: ✔ Xác thực Google thành công.\n\n"
        
        # 2. Chuẩn bị thư mục và các file Google Sheet
        yield "data: [2/5] Chuẩn bị cấu trúc Google Drive...\n\n"
        date_str_dmy = report_date.strftime('%d.%m.%Y')
        year_folder_id = google_handler.get_or_create_gdrive_folder(drive_service, f"Năm {report_date.year}", config.GOOGLE_DRIVE_ROOT_FOLDER_ID)
        month_folder_id = google_handler.get_or_create_gdrive_folder(drive_service, f"Tháng {report_date.month}", year_folder_id)
        
        # Tạo file chi tiết BH03
        spreadsheet_raw = google_handler.get_or_create_gsheet(gspread_client, drive_service, f"BH03.{date_str_dmy}", month_folder_id)
        # TẠO FILE TỔNG HỢP MỚI: BCBH
        spreadsheet_summary = google_handler.get_or_create_gsheet(gspread_client, drive_service, f"BCBH.{date_str_dmy}", month_folder_id)
        # TẠO FILE CÔNG NỢ
        spreadsheet_debt = google_handler.get_or_create_gsheet(gspread_client, drive_service, f"CongNo.{date_str_dmy}", month_folder_id)

        yield "data: ✔ Cấu trúc Google Drive đã sẵn sàng.\n\n"
        
        # 3. Đăng nhập PVOIL
        yield "data: [3/5] Đang đăng nhập PVOIL...\n\n"
        session = requests.Session()
        access_token = api_bh03.pvoil_login(session)
        if not access_token: raise ConnectionError("Đăng nhập PVOIL thất bại.")
        yield "data: ✔ Đăng nhập PVOIL thành công.\n\n"
        
        # 4. Tải và xử lý dữ liệu
        yield "data: [4/5] Bắt đầu quy trình tải và kiểm tra dữ liệu...\n\n"
        stores_to_process = dict(config.STORE_INFO)
        successful_summaries = []
        all_debt_details = []  # Danh sách chứa công nợ của TẤT CẢ cửa hàng
        
        for attempt in range(1, config.MAX_ATTEMPTS + 1):
            if not stores_to_process: break
            yield f"data: --- Lượt xử lý {attempt}/{config.MAX_ATTEMPTS}. Cần xử lý {len(stores_to_process)} cửa hàng ---\n\n"
            failed_stores_in_this_attempt = {}
            for store_code, store_name in stores_to_process.items():
                yield f"data:  -> Đang xử lý: {store_name}...\n\n"
                report_df = api_bh03.download_bh03_report(session, access_token, store_code, report_date)
                
                # Xử lý tổng hợp bán hàng (sản lượng, doanh thu, tiền mặt)
                summary_row = processor_bh03.process_and_validate_bh03(report_df, store_name)
                
                if summary_row:
                    yield f"data:     ✔ Thành công: Dữ liệu hợp lệ.\n\n"
                    successful_summaries.append(summary_row)
                    google_handler.upload_df_to_gsheet(spreadsheet_raw, store_name, report_df)
                    
                    # Bóc tách chi tiết công nợ
                    debt_details = processor_bh03.process_debt_details(report_df, store_name)
                    if debt_details:
                        all_debt_details.extend(debt_details)
                else:
                    yield f"data:     ❌ Thất bại: Báo cáo không hợp lệ hoặc lỗi tải.\n\n"
                    failed_stores_in_this_attempt[store_code] = store_name
            
            stores_to_process = failed_stores_in_this_attempt
            if stores_to_process and attempt < config.MAX_ATTEMPTS:
                yield f"data: --> Nghỉ {config.RETRY_DELAY_SECONDS} giây trước khi thử lại...\n\n"
                time.sleep(config.RETRY_DELAY_SECONDS)
        
        final_failed_stores = stores_to_process
        
        # 5. Lưu các file tổng hợp
        yield "data: [5/5] Lưu file tổng hợp và hoàn tất...\n\n"
        
        # 5.1 Lưu file BCBH tổng hợp
        if successful_summaries:
            df_summary = pd.DataFrame(successful_summaries)
            df_summary.insert(0, 'STT', range(1, 1 + len(df_summary)))
            google_handler.upload_df_to_gsheet(spreadsheet_summary, 'TongHopBCBH', df_summary)
            yield "data:  -> Đã tải lên file tổng hợp BCBH.\n\n"
            try:
                worksheets = spreadsheet_summary.worksheets()
                if len(worksheets) > 1 and worksheets[0].title != 'TongHopBCBH':
                    spreadsheet_summary.del_worksheet(worksheets[0])
            except Exception as e:
                yield f"data:  -> Cảnh báo: Không thể xóa sheet mặc định trong file BCBH: {e}\n\n"
        
        # 5.x Cập nhật file "Tổng hợp tháng" CHỈ CHO NGÀY report_date
            try:
                update_monthly_after_download(report_date)
                yield "data:  -> Đã cập nhật 'Tổng hợp tháng' (Sản lượng & Doanh thu) cho NGÀY này.\n\n"
            except Exception as e:
                yield f"data:  -> Cảnh báo: Không thể cập nhật 'Tổng hợp tháng' cho ngày này: {e}\n\n"


        # 5.2 Lưu file Công nợ (Chi tiết + Tổng hợp KH theo CHXD)
        if all_debt_details:
            df_debt = pd.DataFrame(all_debt_details)
            df_debt = df_debt.sort_values(['Store', 'Customer_Name'])
            
            # ================== A) Chi tiết công nợ (giữ nguyên) ==================
            debt_totals = df_debt.groupby(['Store', 'Customer_Name'])['Debt'].sum().reset_index()
            
            final_rows = []
            current_store = None
            current_customer = None
            stt = 1  # STT KH - reset theo cửa hàng
            
            for _, row in df_debt.iterrows():
                # Sang cửa hàng mới
                if row['Store'] != current_store:
                    current_store = row['Store']
                    final_rows.append({
                        'STT': '',
                        'Tên Khách hàng': current_store,
                        'Mã khách hàng': '',
                        'Sản lượng': '',
                        'Đơn giá': '',
                        'Phát sinh nợ': ''
                    })
                    current_customer = None
                    stt = 1  # reset STT cho cửa hàng mới

                # Sang khách hàng mới -> dòng tiêu đề KH (gán STT tại đây)
                if row['Customer_Name'] != current_customer:
                    current_customer = row['Customer_Name']
                    total_debt = debt_totals[
                        (debt_totals['Store'] == current_store) &
                        (debt_totals['Customer_Name'] == current_customer)
                    ]['Debt'].values[0]
                    customer_code = row['Customer_Code']
                    final_rows.append({
                        'STT': stt,
                        'Tên Khách hàng': current_customer,
                        'Mã khách hàng': customer_code,
                        'Sản lượng': '',
                        'Đơn giá': '',
                        'Phát sinh nợ': total_debt
                    })
                    stt += 1

                # Dòng product: không gán STT
                final_rows.append({
                    'STT': '',
                    'Tên Khách hàng': row['Product'],
                    'Mã khách hàng': '',
                    'Sản lượng': row['Quantity'],
                    'Đơn giá': row['Unit_Price'],
                    'Phát sinh nợ': row['Debt']
                })
            
            df_final_detail = pd.DataFrame(final_rows)
            google_handler.upload_df_to_gsheet(spreadsheet_debt, 'ChiTietCongNo', df_final_detail)
            yield "data:  -> Đã tải lên sheet ChiTietCongNo.\n\n"
            try:
                worksheets = spreadsheet_debt.worksheets()
                if len(worksheets) > 1 and worksheets[0].title != 'ChiTietCongNo':
                    spreadsheet_debt.del_worksheet(worksheets[0])
            except Exception as e:
                yield f"data:  -> Cảnh báo: Không thể xóa sheet mặc định trong file CongNo: {e}\n\n"

            # ================== B) Tổng hợp công nợ theo KH (đơn giản) ==================
            # Loại bỏ các KH 'Công nợ chung' / 'Khách hàng chung'
            excluded_names = {'công nợ chung', 'khách hàng chung'}
            mask_exclude = df_debt['Customer_Name'].astype(str).str.strip().str.lower().isin(excluded_names)
            df_debt_simple = df_debt[~mask_exclude].copy()

            # Gom theo CHXD + KH + Mã KH -> tổng Phát sinh nợ
            debt_totals_simple = (
                df_debt_simple
                .groupby(['Store', 'Customer_Name', 'Customer_Code'], dropna=False)['Debt']
                .sum()
                .reset_index()
                .sort_values(['Store', 'Customer_Name'])
            )

            simple_rows = []
            current_store = None
            stt = 1

            for _, r in debt_totals_simple.iterrows():
                if r['Store'] != current_store:
                    # Dòng tiêu đề cửa hàng
                    current_store = r['Store']
                    simple_rows.append({
                        'STT': '',
                        'Cửa hàng': current_store,
                        'Tên Khách hàng': '',
                        'Mã khách hàng': '',
                        'Phát sinh nợ': ''
                    })
                    stt = 1  # reset STT khi sang cửa hàng mới

                simple_rows.append({
                    'STT': stt,
                    'Cửa hàng': current_store,
                    'Tên Khách hàng': r['Customer_Name'],
                    'Mã khách hàng': r['Customer_Code'],
                    'Phát sinh nợ': r['Debt']
                })
                stt += 1

            df_final_simple = pd.DataFrame(simple_rows, columns=['STT','Cửa hàng','Tên Khách hàng','Mã khách hàng','Phát sinh nợ'])
            google_handler.upload_df_to_gsheet(spreadsheet_debt, 'TongHopCongNo', df_final_simple)
            yield "data:  -> Đã tải lên sheet TongHopCongNo.\n\n"

        success_count = len(successful_summaries)
        total_count = len(config.STORE_INFO)
        message = f"Hoàn tất! Xử lý thành công {success_count}/{total_count} cửa hàng."
        if final_failed_stores:
            failed_names = ', '.join(final_failed_stores.values())
            message += f" | Các cửa hàng thất bại: {failed_names}"
        
        final_result = {"status": "success", "message": message}
        yield f"data: FINAL_MESSAGE:{json.dumps(final_result)}\n\n"

    except Exception as e:
        print(f"Lỗi nghiêm trọng trong quá trình tải báo cáo: {e}")
        error_result = {"status": "error", "message": f"Đã xảy ra lỗi không mong muốn: {str(e)}"}
        yield f"data: ERROR:{json.dumps(error_result)}\n\n"
