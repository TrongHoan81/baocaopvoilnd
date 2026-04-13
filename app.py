# -*- coding: utf-8 -*-
from flask import Flask, render_template, request, jsonify, Response, send_file, stream_with_context
import io
import json
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
        if report_type == 'HD01':
            if not report_year or not report_month: return jsonify({"exists": False})
            
            import bq_handler
            try:
                client = bq_handler.get_bq_client()
                query = f"""
                    SELECT 1 
                    FROM `{client.project}.pvoil_data.HD01_Master_Data` 
                    WHERE Thang_Bao_Cao = {int(report_month)} AND Nam_Bao_Cao = {int(report_year)} 
                    LIMIT 1
                """
                job = client.query(query)
                results = list(job.result())
                
                if len(results) > 0:
                    if station_code != 'ALL':
                        return jsonify({"exists": True, "message": f"Dữ liệu BKHĐ tháng {report_month}/{report_year} đã tồn tại trên BigQuery. Dữ liệu cửa hàng bạn chọn sẽ được cập nhật/ghi đè nối tiếp."})
                    return jsonify({"exists": True, "message": f"Dữ liệu BKHĐ của tháng {report_month}/{report_year} đã có sẵn trên BigQuery. Việc tải lại sẽ cập nhật thêm các hóa đơn mới."})
                return jsonify({"exists": False})
            except Exception as e:
                print(f"Lỗi khi check BigQuery: {e}")
                return jsonify({"exists": False})

        else:
            # Luồng BH03
            creds = google_handler.get_google_credentials()
            drive_service = build('drive', 'v3', credentials=creds)
            
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
    except Exception as e:
        print(f"Lỗi hàm check_report_exists: {e}")
        return jsonify({"exists": False})

@app.route('/aggregate_hd01', methods=['GET'])
def aggregate_hd01():
    try:
        month = request.args.get('month', '').strip()
        year = request.args.get('year', '').strip()
        if not month or not year: return "Thiếu tham số tháng/năm", 400
        
        import bq_handler
        from data_processors.processor_hd01 import generate_excel_from_bq

        agg_prod, agg_status = bq_handler.get_aggregated_data(month, year)
        
        if agg_prod.empty:
            return f"Không tìm thấy dữ liệu hóa đơn của tháng {month}/{year} trên BigQuery.", 404
            
        excel_bytes = generate_excel_from_bq(agg_prod, agg_status)
        if not excel_bytes: return "Lỗi khi tạo Pivot Table", 500
            
        return send_file(excel_bytes, as_attachment=True, download_name=f"TongHop_HoaDon_{int(month):02d}_{year}.xlsx", mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as e:
        print(f"Lỗi Aggregate BigQuery: {e}")
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
        reconcile_type = request.form.get('reconcile_type')

        if 'accounting_file' not in request.files:
            return jsonify({"status": "error", "message": "Vui lòng tải lên file từ phần mềm kế toán."}), 400

        sse_file = request.files['accounting_file']
        
        # Rất Quan Trọng: Đọc toàn bộ file vào RAM ngay lập tức để tránh Request bị đóng ngắt giữa chừng
        file_bytes = sse_file.read()
        file_name = sse_file.filename

        # Lấy trước các biến Form
        target_month = request.form.get('reconcile_month')
        target_year = request.form.get('reconcile_year')
        reconcile_date_str = request.form.get('reconcile_date')

        # === ĐỊNH NGHĨA GENERATOR ĐỂ XẢ DATA VỀ TRÌNH DUYỆT LIÊN TỤC ===
        def generate():
            q = queue.Queue()

            # Hàm con để gửi Log vào hàng chờ
            def progress_callback(msg):
                q.put({"type": "log", "message": msg})

            # Hàm Công nhân: Làm việc mệt nhọc ở Background Thread
            def worker():
                try:
                    file_stream = io.BytesIO(file_bytes)
                    file_stream.name = file_name # Fake tên file để Pandas nhận diện định dạng

                    # --- NHÁNH 1: ĐỐI SOÁT HÓA ĐƠN TRÊN BIGQUERY ---
                    if reconcile_type == 'HoaDon':
                        if not target_month or not target_year:
                            q.put({"type": "result", "status": "error", "message": "Thiếu tháng/năm đối soát hóa đơn."})
                            return

                        progress_callback("... Đang kiểm tra dữ liệu BigQuery.....")
                        import bq_handler
                        client = bq_handler.get_bq_client()
                        query = f"SELECT 1 FROM `{client.project}.pvoil_data.HD01_Master_Data` WHERE Thang_Bao_Cao = {int(target_month)} AND Nam_Bao_Cao = {int(target_year)} LIMIT 1"
                        job = client.query(query)
                        
                        if len(list(job.result())) == 0:
                            q.put({'type': 'result', 'status': 'report_not_found', 'message': f'Dữ liệu hóa đơn tháng {target_month}/{target_year} chưa có trên hệ thống BigQuery.'})
                            return

                        import reconciliation_handler
                        tax_df = reconciliation_handler.read_tax_excel_file(file_stream, target_month=target_month, target_year=target_year, progress_callback=progress_callback)
                        results = reconciliation_handler.reconcile_invoice_data_bq(target_month, target_year, tax_df, progress_callback=progress_callback)

                        progress_callback("...... Đang vẽ bảng kết quả........")
                        q.put({'type': 'result', 'status': 'success', 'reconcile_type': reconcile_type, 'data': results})

                    # --- NHÁNH 2: ĐỐI SOÁT BH03 TRÊN GOOGLE DRIVE (Giữ nguyên logic cũ) ---
                    else:
                        progress_callback("... Đang xác thực với Google Drive.....")
                        creds = google_handler.get_google_credentials()
                        gspread_client = gspread.authorize(creds)
                        drive_service = build('drive', 'v3', credentials=creds)

                        if not reconcile_date_str:
                            q.put({"type": "result", "status": "error", "message": "Vui lòng chọn ngày đối soát."})
                            return

                        reconcile_date = datetime.strptime(reconcile_date_str, '%Y-%m-%d')
                        date_str_dmy = reconcile_date.strftime('%d.%m.%Y')

                        progress_callback(f"... Đang tìm báo cáo ngày {date_str_dmy}.....")
                        year_folder_id = google_handler.get_or_create_gdrive_folder(drive_service, f"Năm {reconcile_date.year}", config.GOOGLE_DRIVE_ROOT_FOLDER_ID)
                        month_folder_id = google_handler.get_or_create_gdrive_folder(drive_service, f"Tháng {reconcile_date.month}", year_folder_id)

                        if reconcile_type == 'CongNo':
                            try: pos_spreadsheet = gspread_client.open(f"CongNo.{date_str_dmy}", folder_id=month_folder_id)
                            except gspread.exceptions.SpreadsheetNotFound: 
                                q.put({"type": "result", "status": "report_not_found", "message": f"Không tìm thấy báo cáo POS (CongNo) ngày {date_str_dmy}."})
                                return
                            
                            progress_callback("... Đang tải dữ liệu POS từ Google Sheet.....")
                            pos_sheet = pos_spreadsheet.worksheet('TongHopCongNo')
                            pos_data = pos_sheet.get_all_values()
                            pos_df = pd.DataFrame(pos_data[1:], columns=pos_data[0]) if len(pos_data) > 1 else pd.DataFrame()
                        else:
                            try: pos_spreadsheet = gspread_client.open(f"BCBH.{date_str_dmy}", folder_id=month_folder_id)
                            except gspread.exceptions.SpreadsheetNotFound: 
                                q.put({"type": "result", "status": "report_not_found", "message": f"Không tìm thấy báo cáo POS (BCBH) ngày {date_str_dmy}."})
                                return
                            
                            progress_callback("... Đang tải dữ liệu POS từ Google Sheet.....")
                            pos_sheet = pos_spreadsheet.worksheet('TongHopBCBH')
                            pos_data = pos_sheet.get_all_values()
                            pos_df = pd.DataFrame(pos_data[1:], columns=pos_data[0]) if len(pos_data) > 1 else pd.DataFrame()

                        import reconciliation_handler
                        progress_callback("... Đang đọc file Kế toán (XML/Excel).....")
                        
                        if reconcile_type == 'SanLuong':
                            sse_df = reconciliation_handler.read_sse_product_xml(file_stream)
                            if sse_df is None:
                                q.put({"type": "result", "status": "error", "message": "Định dạng file kế toán (sản lượng) không hợp lệ."})
                                return
                            progress_callback("... Bắt đầu so khớp dữ liệu Sản lượng.....")
                            reconciliation_results = reconciliation_handler.reconcile_product_data(pos_df, sse_df)
                            
                        elif reconcile_type == 'TienMat':
                            sse_df = reconciliation_handler.read_sse_cash_xml(file_stream, reconcile_date)
                            if sse_df is None:
                                q.put({"type": "result", "status": "error", "message": "Định dạng file kế toán (tiền mặt) không hợp lệ."})
                                return
                            progress_callback("... Bắt đầu so khớp dữ liệu Tiền mặt.....")
                            reconciliation_results = reconciliation_handler.reconcile_cash_data(pos_df, sse_df)
                            
                        elif reconcile_type == 'CongNo':
                            sse_df = reconciliation_handler.read_sse_debt_xml(file_stream)
                            if sse_df is None:
                                q.put({"type": "result", "status": "error", "message": "Định dạng file kế toán (công nợ) không hợp lệ."})
                                return
                            progress_callback("... Bắt đầu so khớp dữ liệu Công nợ.....")
                            reconciliation_results = reconciliation_handler.reconcile_debt_data(pos_df, sse_df)
                        else:
                            q.put({"type": "result", "status": "error", "message": "Loại đối soát không hợp lệ."})
                            return

                        progress_callback("...... Đang vẽ bảng kết quả........")
                        q.put({"type": "result", "status": "success", "data": reconciliation_results, "reconcile_type": reconcile_type})

                except Exception as e:
                    print(f"Lỗi khi đối soát trong luồng nền: {e}")
                    import traceback
                    traceback.print_exc()
                    q.put({"type": "result", "status": "error", "message": f"Đã xảy ra lỗi không mong muốn: {str(e)}"})

            # Khởi động luồng chạy song song
            t = threading.Thread(target=worker)
            t.start()

            # Trả dữ liệu ra API liên tục
            while True:
                item = q.get()
                yield json.dumps(item) + "\n"  # Định dạng NDJSON chuẩn
                if item.get("type") == "result":
                    break

        return Response(stream_with_context(generate()), mimetype='application/x-ndjson')

    except Exception as e:
        print(f"Lỗi khởi tạo luồng đối soát (Main thread): {e}")
        return jsonify({"status": "error", "message": f"Lỗi khởi tạo luồng đối soát: {str(e)}"}), 500

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