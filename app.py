# -*- coding: utf-8 -*-
from flask import Flask, render_template, request, jsonify, Response, send_file, stream_with_context
import io
from datetime import datetime
import pandas as pd
import os
import requests
import inspect 

from dotenv import load_dotenv
load_dotenv()

from security import require_internal_api_key
import gspread
from googleapiclient.discovery import build
import config
import google_handler
import reconciliation_handler
from tasks import download_report_generator

PROXY_MODE = os.getenv("PROXY_DOWNLOAD_VIA_VPS", "0") == "1"
VPS_BASE_URL = os.getenv("VPS_BASE_URL", "").rstrip("/")
VPS_KEY = os.getenv("VPS_INTERNAL_API_KEY", "")
PROXY_TIMEOUT = int(os.getenv("PROXY_TIMEOUT_SECONDS", "1200"))

# ========== LỚP 2: JOB MANAGER (VPS) ==========
import threading, queue, time
from collections import deque

class StreamJob:
    def __init__(self, report_date, report_type="BH03", station_code="ALL", report_year="", report_month=""):
        self.report_date = report_date
        self.report_type = report_type
        self.station_code = station_code
        self.report_year = report_year
        self.report_month = report_month
        self.thread = None
        self.subscribers = []            
        self.buffer = deque(maxlen=500)  
        self.lock = threading.Lock()
        self.done = False

    def start_if_needed(self):
        with self.lock:
            if self.thread and self.thread.is_alive(): return
            self.thread = threading.Thread(target=self._run, daemon=True)
            self.thread.start()

    def _run(self):
        try:
            self._broadcast_line("retry: 3000")
            sig = inspect.signature(download_report_generator)
            if 'report_type' in sig.parameters:
                gen = download_report_generator(self.report_date, report_type=self.report_type, station_code_filter=self.station_code if self.station_code != "ALL" else None, report_year=self.report_year, report_month=self.report_month)
            else:
                gen = download_report_generator(self.report_date)

            for raw_chunk in gen:
                for line in raw_chunk.splitlines(): self._broadcast_line(line)
                if "FINAL_MESSAGE:" in raw_chunk or "ERROR:" in raw_chunk: self.done = True
        except Exception as e:
            self._broadcast_line(f'data: ERROR:{{"status":"error","message":"Job crash: {str(e)}"}}')
        finally: self.done = True

    def _broadcast_line(self, line: str):
        self.buffer.append(line)
        dead = []
        for q in self.subscribers:
            try: q.put_nowait(line)
            except Exception: dead.append(q)
        if dead: self.subscribers = [q for q in self.subscribers if q not in dead]

    def subscribe(self):
        q = queue.Queue(maxsize=1000)
        with self.lock:
            for line in self.buffer:
                try: q.put_nowait(line)
                except Exception: break
            self.subscribers.append(q)
        return q

JOBS = {}
JOBS_LOCK = threading.Lock()

def get_or_create_job(report_date, report_type="BH03", station_code="ALL", report_year="", report_month=""):
    key = f"HD01_{report_year}-{report_month}_{station_code}" if report_type == "HD01" else f"BH03_{report_date.strftime('%Y-%m-%d')}_{station_code}"
    with JOBS_LOCK:
        job = JOBS.get(key)
        if not job:
            job = StreamJob(report_date, report_type, station_code, report_year, report_month)
            JOBS[key] = job
    job.start_if_needed()
    return job

# ========== APP ==========
app = Flask(__name__)

@app.route('/')
def index():
    stores = config.STORE_INFO
    return render_template('index.html', stores=stores)

# ==========================================
# API TIỀN KIỂM & TỔNG HỢP HD01
# ==========================================
@app.route('/check_report_exists', methods=['GET'])
def check_report_exists():
    report_type = request.args.get('report_type', 'BH03').strip()
    report_year = request.args.get('report_year', '').strip()
    report_month = request.args.get('report_month', '').strip()
    report_date_str = request.args.get('report_date', '').strip()
    station_code = request.args.get('station_code', 'ALL').strip()

    try:
        creds = google_handler.get_google_credentials()
        drive_service = build('drive', 'v3', credentials=creds)

        if report_type == 'HD01':
            if not report_year or not report_month: return jsonify({"exists": False})
            year_folder_name = f"Năm {report_year}"
            month_folder_name = f"Tháng {int(report_month)}"

            year_folder_id = google_handler._search_file_in_folder(drive_service, year_folder_name, config.GOOGLE_DRIVE_ROOT_FOLDER_ID, "application/vnd.google-apps.folder")
            if not year_folder_id: return jsonify({"exists": False})
            month_folder_id = google_handler._search_file_in_folder(drive_service, month_folder_name, year_folder_id, "application/vnd.google-apps.folder")
            if not month_folder_id: return jsonify({"exists": False})

            # TÌM TẤT CẢ CÁC FILE CÓ TIỀN TỐ BKHĐ.MM.YYYY_ (Theo chuẩn file 1-1)
            query = f"name contains 'BKHĐ.{int(report_month):02d}.{report_year}_' and '{month_folder_id}' in parents and trashed = false"
            results = drive_service.files().list(q=query, fields="files(id, name)").execute()
            files = results.get('files', [])

            if files:
                if station_code != 'ALL':
                    return jsonify({"exists": True, "message": f"Dữ liệu BKHĐ tháng {report_month}/{report_year} đã tồn tại. Dữ liệu cửa hàng bạn chọn sẽ được cập nhật."})
                return jsonify({"exists": True, "message": f"Hệ thống tìm thấy {len(files)} file BKHĐ (Cấu trúc Độc lập) của tháng {report_month}/{report_year}. Việc tải lại sẽ XÓA SẠCH các file cũ và TẢI LẠI TOÀN BỘ."})
            return jsonify({"exists": False})
        else:
            if not report_date_str: return jsonify({"exists": False})
            d = datetime.strptime(report_date_str, '%Y-%m-%d')
            file_name = f"BCBH.{d.strftime('%d.%m.%Y')}"
            year_folder_id = google_handler._search_file_in_folder(drive_service, f"Năm {d.year}", config.GOOGLE_DRIVE_ROOT_FOLDER_ID, "application/vnd.google-apps.folder")
            if not year_folder_id: return jsonify({"exists": False})
            month_folder_id = google_handler._search_file_in_folder(drive_service, f"Tháng {d.month}", year_folder_id, "application/vnd.google-apps.folder")
            if not month_folder_id: return jsonify({"exists": False})
            
            file_id = google_handler._search_file_in_folder(drive_service, file_name, month_folder_id, "application/vnd.google-apps.spreadsheet")
            if file_id: return jsonify({"exists": True, "message": f"Báo cáo BH03 ngày {d.strftime('%d/%m/%Y')} đã tồn tại trên Google Drive."})
            return jsonify({"exists": False})
    except Exception:
        return jsonify({"exists": False})

@app.route('/aggregate_hd01', methods=['GET'])
def aggregate_hd01():
    try:
        month = request.args.get('month', '').strip()
        year = request.args.get('year', '').strip()
        if not month or not year: return "Thiếu tham số tháng/năm", 400
            
        creds = google_handler.get_google_credentials()
        drive_service = build('drive', 'v3', credentials=creds)
        
        year_folder_id = google_handler._search_file_in_folder(drive_service, f"Năm {year}", config.GOOGLE_DRIVE_ROOT_FOLDER_ID, "application/vnd.google-apps.folder")
        if not year_folder_id: return "Không tìm thấy thư mục Năm", 404
        month_folder_id = google_handler._search_file_in_folder(drive_service, f"Tháng {int(month)}", year_folder_id, "application/vnd.google-apps.folder")
        if not month_folder_id: return "Không tìm thấy thư mục Tháng", 404
        
        # SỬ DỤNG QUERY TÌM THEO TÊN FILE CỦA KIẾN TRÚC 1-1
        query = f"name contains 'BKHĐ.{int(month):02d}.{year}_' and '{month_folder_id}' in parents and trashed = false"
        results = drive_service.files().list(q=query, fields="files(id, name)").execute()
        files = results.get('files', [])
        
        if not files: return f"Không tìm thấy dữ liệu BKHĐ của tháng {month}/{year}", 404
        
        dict_dfs = {}
        
        # =====================================================================
        # CHIẾN THUẬT MỚI: XUẤT THẲNG EXCEL QUA API DRIVE (CHỐNG LỖI 500 PANDAS)
        # Giờ đây các file nhỏ gọn, API export sẽ không bao giờ bị dính lỗi 403 SizeLimit
        # =====================================================================
        for f in files:
            file_id = f.get('id')
            try:
                # Ra lệnh Google Drive xuất nguyên file thành file .xlsx dạng nhị phân (Bytes)
                excel_content = drive_service.files().export(
                    fileId=file_id, 
                    mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                ).execute()
                
                # Dùng Pandas đọc bộ nhớ nhị phân này 1 lần duy nhất để lấy TẤT CẢ các sheet
                # dtype=str để ép Pandas không tự ý xóa số 0 đầu dòng của Số Hóa Đơn
                file_dfs = pd.read_excel(io.BytesIO(excel_content), sheet_name=None, dtype=str)
                
                for ws_title, df in file_dfs.items():
                    if ws_title.strip() in ["Trang tính 1", "Sheet1", "Sheet", "Trang tính", "Tổng hợp"]: continue
                    if not df.empty:
                        dict_dfs[ws_title] = df
            except Exception as e:
                print(f"Lỗi khi tải file {f.get('name')}: {e}")
                continue
                
        if not dict_dfs: return "Các file báo cáo đều trống rỗng", 400
            
        from data_processors.processor_hd01 import aggregate_hd01_data
        excel_bytes = aggregate_hd01_data(dict_dfs)
        if not excel_bytes: return "Lỗi khi tạo Pivot Table", 500
            
        return send_file(excel_bytes, as_attachment=True, download_name=f"TongHop_HoaDon_{int(month):02d}_{year}.xlsx", mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as e:
        return f"Lỗi máy chủ nội bộ: {str(e)}", 500

# ==========================
# ROUTE TRUYỀN DỮ LIỆU
# ==========================
@app.get("/internal/download_report_stream")
@require_internal_api_key()
def internal_download_report_stream():
    report_type = request.args.get('report_type', 'BH03').strip()
    station_code = request.args.get('station_code', 'ALL').strip()
    report_year = request.args.get('report_year', '').strip()
    report_month = request.args.get('report_month', '').strip()
    report_date_str = request.args.get('report_date', '').strip()
    
    if report_type == 'BH03' and not report_date_str:
        def err(): yield 'data: {"status": "error", "message": "Vui lòng chọn ngày báo cáo."}\n\n'
        return Response(err(), mimetype='text/event-stream')
    if report_type == 'HD01' and (not report_year or not report_month):
        def err(): yield 'data: {"status": "error", "message": "Vui lòng chọn tháng và năm báo cáo."}\n\n'
        return Response(err(), mimetype='text/event-stream')

    report_date = datetime.now()
    if report_date_str: report_date = datetime.strptime(report_date_str, '%Y-%m-%d')
    elif report_type == 'HD01': report_date = datetime(int(report_year), int(report_month), 1)

    job = get_or_create_job(report_date, report_type, station_code, report_year, report_month)
    q = job.subscribe()

    def stream():
        last = time.monotonic()
        while True:
            try:
                line = q.get(timeout=3)
                yield line + "\n"
                last = time.monotonic()
            except queue.Empty:
                if job.done: break
                now = time.monotonic()
                if now - last > 10:
                    yield "data: 💓 heartbeat\n\n"
                    last = now
        yield "\n"
    return Response(stream_with_context(stream()), mimetype='text/event-stream', headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"})

@app.route('/download_report_stream')
def download_report_stream():
    report_type = request.args.get('report_type', 'BH03').strip()
    station_code = request.args.get('station_code', 'ALL').strip()
    report_year = request.args.get('report_year', '').strip()
    report_month = request.args.get('report_month', '').strip()
    report_date_str = request.args.get('report_date', '').strip()
    
    if report_type == 'BH03' and not report_date_str:
        def err(): yield 'data: {"status": "error", "message": "Vui lòng chọn ngày báo cáo."}\n\n'
        return Response(err(), mimetype='text/event-stream')
    if report_type == 'HD01' and (not report_year or not report_month):
        def err(): yield 'data: {"status": "error", "message": "Vui lòng chọn tháng và năm báo cáo."}\n\n'
        return Response(err(), mimetype='text/event-stream')

    if PROXY_MODE:
        if not VPS_BASE_URL or not VPS_KEY:
            def err(): yield 'data: {"status": "error", "message": "Proxy thiếu VPS_BASE_URL."}\n\n'
            return Response(err(), mimetype='text/event-stream')
        try:
            upstream = requests.get(f"{VPS_BASE_URL}/internal/download_report_stream", params={"report_type": report_type, "report_date": report_date_str, "station_code": station_code, "report_year": report_year, "report_month": report_month}, headers={"X-Internal-Api-Key": VPS_KEY, "Accept": "text/event-stream"}, stream=True, timeout=(10, 20))
        except Exception as ex:
            def err(): yield f'data: ERROR:{{"status":"error","message":"Lỗi VPS: {str(ex)}"}}\n\n'
            return Response(err(), mimetype='text/event-stream')

        def generate():
            from requests.exceptions import ReadTimeout
            upstream.raw.decode_content = True
            buffer = ""
            while True:
                try:
                    chunk = upstream.raw.read(1)
                    if not chunk: break
                    s = chunk.decode('utf-8', errors='ignore') if isinstance(chunk, bytes) else str(chunk)
                    buffer += s
                    if "\n" in buffer:
                        parts = buffer.split("\n")
                        buffer = parts.pop()
                        for line in parts: yield line + "\n"
                except ReadTimeout:
                    yield "data: 💓 heartbeat\n\n"
                    continue
            if buffer: yield buffer + ("\n" if not buffer.endswith("\n") else "")
            yield "\n"
        return Response(stream_with_context(generate()), mimetype="text/event-stream", headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"})

    report_date = datetime.now()
    if report_date_str: report_date = datetime.strptime(report_date_str, '%Y-%m-%d')
    elif report_type == 'HD01': report_date = datetime(int(report_year), int(report_month), 1)
        
    sig = inspect.signature(download_report_generator)
    if 'report_type' in sig.parameters:
        gen = download_report_generator(report_date, report_type=report_type, station_code_filter=station_code if station_code != "ALL" else None, report_year=report_year, report_month=report_month)
    else:
        gen = download_report_generator(report_date)
        
    return Response(stream_with_context(gen), mimetype='text/event-stream', headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"})

# ====================
# ĐỐI SOÁT
# ====================
@app.route('/reconcile', methods=['POST'])
def reconcile():
    try:
        reconcile_date_str = request.form.get('reconcile_date')
        reconcile_type = request.form.get('reconcile_type')

        if 'accounting_file' not in request.files:
            return jsonify({"status": "error", "message": "Vui lòng tải lên file từ phần mềm kế toán."}), 400

        sse_file = request.files['accounting_file']

        creds = google_handler.get_google_credentials()
        gspread_client = gspread.authorize(creds)
        drive_service = build('drive', 'v3', credentials=creds)

        if reconcile_type == 'HoaDon':
            target_month = request.form.get('reconcile_month')
            target_year = request.form.get('reconcile_year')
            
            if not target_month or not target_year:
                return jsonify({"status": "error", "message": "Thiếu tháng/năm đối soát hóa đơn."}), 400

            try:
                tax_df = reconciliation_handler.read_tax_excel_file(sse_file.stream, target_month, target_year)
            except ValueError as ve:
                return jsonify({"status": "error", "message": str(ve)}), 400

            year_folder_id = google_handler._search_file_in_folder(drive_service, f"Năm {target_year}", config.GOOGLE_DRIVE_ROOT_FOLDER_ID, "application/vnd.google-apps.folder")
            if not year_folder_id: return jsonify({"status": "error", "message": f"Không tìm thấy dữ liệu năm {target_year} trên Drive."}), 400
            
            month_folder_id = google_handler._search_file_in_folder(drive_service, f"Tháng {int(target_month)}", year_folder_id, "application/vnd.google-apps.folder")
            if not month_folder_id: return jsonify({"status": "error", "message": f"Không tìm thấy dữ liệu tháng {target_month} trên Drive."}), 400
            
            # SỬ DỤNG QUERY TÌM THEO TÊN FILE CỦA KIẾN TRÚC 1-1 TRONG ĐỐI SOÁT
            query = f"name contains 'BKHĐ.{int(target_month):02d}.{target_year}_' and '{month_folder_id}' in parents and trashed = false"
            results = drive_service.files().list(q=query, fields="files(id, name)").execute()
            files = results.get('files', [])
            
            if not files:
                return jsonify({"status": "report_not_found", "message": f"Không tìm thấy dữ liệu BKHĐ của tháng {target_month}/{target_year} trên Drive."})

            dict_dfs = {}
            # ÁP DỤNG CÙNG CÔNG NGHỆ XUẤT EXCEL CHO ĐỐI SOÁT (Tải 45 file con)
            for f in files:
                file_id = f.get('id')
                try:
                    excel_content = drive_service.files().export(
                        fileId=file_id, 
                        mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                    ).execute()
                    
                    file_dfs = pd.read_excel(io.BytesIO(excel_content), sheet_name=None, dtype=str)
                    
                    for ws_title, df in file_dfs.items():
                        if ws_title.strip() in ["Trang tính 1", "Sheet1", "Sheet", "Trang tính", "Tổng hợp"]: continue
                        if not df.empty:
                            dict_dfs[ws_title] = df
                except Exception as e:
                    print(f"Lỗi khi đọc file đối soát {f.get('name')}: {e}")
                    continue

            reconciliation_results = reconciliation_handler.reconcile_invoice_data(dict_dfs, tax_df)
            return jsonify({"status": "success", "data": reconciliation_results, "reconcile_type": reconcile_type})

        # --- CÁC NHÁNH CŨ GIỮ NGUYÊN (Dữ liệu bé nên dùng gspread an toàn) ---
        if not reconcile_date_str: return jsonify({"status": "error", "message": "Vui lòng chọn ngày đối soát."}), 400
        reconcile_date = datetime.strptime(reconcile_date_str, '%Y-%m-%d')
        date_str_dmy = reconcile_date.strftime('%d.%m.%Y')
        year_folder_id = google_handler.get_or_create_gdrive_folder(drive_service, f"Năm {reconcile_date.year}", config.GOOGLE_DRIVE_ROOT_FOLDER_ID)
        month_folder_id = google_handler.get_or_create_gdrive_folder(drive_service, f"Tháng {reconcile_date.month}", year_folder_id)

        if reconcile_type == 'CongNo':
            try: pos_spreadsheet = gspread_client.open(f"CongNo.{date_str_dmy}", folder_id=month_folder_id)
            except gspread.exceptions.SpreadsheetNotFound: return jsonify({"status": "report_not_found", "message": f"Không tìm thấy báo cáo POS (CongNo) ngày {date_str_dmy}."})
            pos_sheet = pos_spreadsheet.worksheet('TongHopCongNo')
            pos_data = pos_sheet.get_all_values()
            pos_df = pd.DataFrame(pos_data[1:], columns=pos_data[0]) if len(pos_data) > 1 else pd.DataFrame()
        else:
            try: pos_spreadsheet = gspread_client.open(f"BCBH.{date_str_dmy}", folder_id=month_folder_id)
            except gspread.exceptions.SpreadsheetNotFound: return jsonify({"status": "report_not_found", "message": f"Không tìm thấy báo cáo POS (BCBH) ngày {date_str_dmy}."})
            pos_sheet = pos_spreadsheet.worksheet('TongHopBCBH')
            pos_data = pos_sheet.get_all_values()
            pos_df = pd.DataFrame(pos_data[1:], columns=pos_data[0]) if len(pos_data) > 1 else pd.DataFrame()

        if reconcile_type == 'SanLuong':
            sse_df = reconciliation_handler.read_sse_product_xml(sse_file.stream)
            if sse_df is None: return jsonify({"status": "error", "message": "Định dạng file kế toán (sản lượng) không hợp lệ."}), 400
            reconciliation_results = reconciliation_handler.reconcile_product_data(pos_df, sse_df)
        elif reconcile_type == 'TienMat':
            sse_df = reconciliation_handler.read_sse_cash_xml(sse_file.stream, reconcile_date)
            if sse_df is None: return jsonify({"status": "error", "message": "Định dạng file kế toán (tiền mặt) không hợp lệ."}), 400
            reconciliation_results = reconciliation_handler.reconcile_cash_data(pos_df, sse_df)
        elif reconcile_type == 'CongNo':
            sse_df = reconciliation_handler.read_sse_debt_xml(sse_file.stream)
            if sse_df is None: return jsonify({"status": "error", "message": "Định dạng file kế toán (công nợ) không hợp lệ."}), 400
            reconciliation_results = reconciliation_handler.reconcile_debt_data(pos_df, sse_df)
        else:
            return jsonify({"status": "error", "message": "Loại đối soát không hợp lệ."}), 400

        return jsonify({"status": "success", "data": reconciliation_results, "reconcile_type": reconcile_type})

    except Exception as e:
        print(f"Lỗi khi đối soát: {e}")
        return jsonify({"status": "error", "message": f"Đã xảy ra lỗi không mong muốn: {str(e)}"}), 500

@app.route('/download_excel', methods=['POST'])
def download_excel():
    try:
        json_data = request.get_json()
        results_data = json_data.get('data')
        reconcile_type = json_data.get('reconcile_type', 'SanLuong')

        if not results_data: return "No data received", 400

        df = pd.DataFrame(results_data)

        if reconcile_type == 'TienMat':
            column_names = {'chxd_name': 'Cửa hàng', 'product_name': 'Đối tượng', 'pos_value': 'Tiền mặt POS (VND)', 'sse_value': 'Tiền mặt Kế toán (VND)', 'is_match': 'Khớp', 'status': 'Ghi chú'}
        elif reconcile_type == 'CongNo':
            column_names = {'chxd_name': 'Cửa hàng', 'customer_code': 'Mã khách', 'customer_name': 'Tên khách hàng', 'pos_value': 'Phát sinh nợ POS (VND)', 'sse_value': 'Phát sinh nợ Kế toán (VND)', 'is_match': 'Khớp', 'status': 'Ghi chú'}
        elif reconcile_type == 'HoaDon':
            column_names = {'chxd_name': 'Cửa hàng / Phân loại', 'invoice_id': 'Chứng minh thư HĐ', 'pos_value': 'Tổng tiền PVOIL (VND)', 'sse_value': 'Tổng tiền Bảng kê Thuế (VND)', 'is_match': 'Khớp', 'status': 'Ghi chú'}
        else:  # SanLuong
            column_names = {'chxd_name': 'Cửa hàng', 'product_name': 'Mặt hàng', 'pos_value': 'Sản lượng POS', 'sse_value': 'Sản lượng Kế toán', 'is_match': 'Khớp', 'status': 'Ghi chú'}

        df.rename(columns={k: v for k, v in column_names.items() if k in df.columns}, inplace=True)
        if 'Khớp' in df.columns: df['Khớp'] = df['Khớp'].apply(lambda x: 'Khớp' if bool(x) else 'Lệch')

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='KetQuaDoiSoat')
        output.seek(0)

        reconcile_date_str = datetime.now().strftime('%d-%m-%Y')
        filename = f"KetQuaDoiSoat_{reconcile_type}_{reconcile_date_str}.xlsx"

        return send_file(output, as_attachment=True, download_name=filename, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as e:
        print(f"Lỗi khi tạo file Excel: {e}")
        return "Error creating Excel file", 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=False)