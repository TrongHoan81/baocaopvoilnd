# -*- coding: utf-8 -*-
from flask import Flask, render_template, request, jsonify, Response, send_file, stream_with_context
import io
from datetime import datetime
import pandas as pd
import os
import requests

# Load .env sớm để đọc các biến cấu hình
from dotenv import load_dotenv
load_dotenv()

# Bảo vệ API nội bộ
from security import require_internal_api_key

# Google
import gspread
from googleapiclient.discovery import build

# Modules nghiệp vụ (GIỮ NGUYÊN)
import config
import google_handler
import reconciliation_handler

# Cấu hình PROXY (Render) vs DIRECT (VPS/Local)
PROXY_MODE = os.getenv("PROXY_DOWNLOAD_VIA_VPS", "0") == "1"
VPS_BASE_URL = os.getenv("VPS_BASE_URL", "").rstrip("/")
VPS_KEY = os.getenv("VPS_INTERNAL_API_KEY", "")
PROXY_TIMEOUT = int(os.getenv("PROXY_TIMEOUT_SECONDS", "1200"))  # giây

# Tasks (generator phát SSE – GIỮ NGUYÊN)
from tasks import download_report_generator

app = Flask(__name__)

@app.route('/')
def index():
    """Hiển thị trang giao diện chính."""
    return render_template('index.html')

# ==========================
# 1) ROUTE NỘI BỘ TRÊN VPS
# ==========================
@app.get("/internal/download_report_stream")
@require_internal_api_key()
def internal_download_report_stream():
    """Endpoint NỘI BỘ trên VPS: Render gọi vào đây để nhận SSE."""
    report_date_str = request.args.get('report_date', '').strip()
    if not report_date_str:
        def error_generator():
            yield 'data: {"status": "error", "message": "Vui lòng chọn ngày báo cáo."}\n\n'
        return Response(error_generator(), mimetype='text/event-stream')

    report_date = datetime.strptime(report_date_str, '%Y-%m-%d')
    # Phát NGUYÊN giá trị từ generator của nghiệp vụ
    return Response(
        stream_with_context(download_report_generator(report_date)),
        mimetype='text/event-stream',
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
    )

# =====================================
# 2) ROUTE UI – DIRECT hoặc PROXY (ENV)
# =====================================
@app.route('/download_report_stream')
def download_report_stream():
    """
    Endpoint để tải báo cáo và truyền log về giao diện:
      - DIRECT (local/VPS): gọi thẳng generator.
      - PROXY (Render): stream từ VPS kèm HEARTBEAT để tránh timeout.
    """
    report_date_str = request.args.get('report_date', '').strip()
    if not report_date_str:
        def error_generator():
            yield 'data: {"status": "error", "message": "Vui lòng chọn ngày báo cáo."}\n\n'
        return Response(error_generator(), mimetype='text/event-stream')

    # === Nhánh PROXY: dùng trên Render ===
    if PROXY_MODE:
        if not VPS_BASE_URL or not VPS_KEY:
            def misconf():
                yield 'data: {"status": "error", "message": "Proxy thiếu VPS_BASE_URL hoặc VPS_INTERNAL_API_KEY."}\n\n'
            return Response(misconf(), mimetype='text/event-stream')

        try:
            # Đặt timeout dạng (connect, read). Read-timeout đặt ngắn để có thể bơm heartbeat khi im lặng.
            upstream = requests.get(
                f"{VPS_BASE_URL}/internal/download_report_stream",
                params={"report_date": report_date_str},
                headers={
                    "X-Internal-Api-Key": VPS_KEY,
                    "Accept": "text/event-stream",
                },
                stream=True,
                timeout=(10, 20),  # connect=10s, read=20s -> nếu im quá 20s sẽ raise ReadTimeout
            )
        except requests.RequestException as ex:
            def err_gen():
                yield f'data: ERROR:{{"status":"error","message":"Không kết nối được VPS: {str(ex)}"}}\n\n'
            return Response(err_gen(), mimetype='text/event-stream')

        def generate():
            """
            Proxy SSE với HEARTBEAT:
            - Đọc từng byte từ upstream; khi ghép đủ dòng thì forward ngay cho trình duyệt.
            - Nếu im lặng > read-timeout (20s) -> requests ném ReadTimeout -> bơm heartbeat và tiếp tục đọc.
            """
            import time
            from requests.exceptions import ReadTimeout

            # Cho phép truy cập socket thô để đọc từng phần nhỏ
            upstream.raw.decode_content = True
            buffer = ""
            last_any_output = time.monotonic()

            while True:
                try:
                    chunk = upstream.raw.read(1)  # đọc 1 byte để phản ứng nhanh
                    if not chunk:
                        # Upstream đã đóng kết nối
                        break
                    try:
                        s = chunk.decode('utf-8', errors='ignore')
                    except Exception:
                        s = str(chunk)
                    buffer += s

                    # Đẩy các dòng hoàn chỉnh
                    if "\n" in buffer:
                        parts = buffer.split("\n")
                        buffer = parts.pop()
                        for line in parts:
                            # pass-through mỗi dòng
                            yield (line + "\n")
                            last_any_output = time.monotonic()

                except ReadTimeout:
                    # Không nhận dữ liệu mới trong 20s -> gửi heartbeat để giữ kết nối phía Render/Gunicorn
                    yield "data: 💓 heartbeat\n\n"
                    last_any_output = time.monotonic()
                    continue

            # Kết thúc stream
            if buffer:
                yield buffer + ("\n" if not buffer.endswith("\n") else "")
            yield "\n"

        return Response(
            stream_with_context(generate()),
            mimetype="text/event-stream",
            headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
        )

    # === Nhánh DIRECT: local PC / VPS chạy trực tiếp như cũ ===
    report_date = datetime.strptime(report_date_str, '%Y-%m-%d')
    return Response(
        stream_with_context(download_report_generator(report_date)),
        mimetype='text/event-stream',
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
    )

# ====================
# 3) CÁC ROUTE KHÁC
# ====================
@app.route('/reconcile', methods=['POST'])
def reconcile():
    """Xử lý yêu cầu đối soát: SanLuong | TienMat | CongNo."""
    try:
        reconcile_date_str = request.form.get('reconcile_date')
        reconcile_type = request.form.get('reconcile_type')

        if not reconcile_date_str:
            return jsonify({"status": "error", "message": "Vui lòng chọn ngày đối soát."}), 400
        if 'accounting_file' not in request.files:
            return jsonify({"status": "error", "message": "Vui lòng tải lên file từ phần mềm kế toán."}), 400

        sse_file = request.files['accounting_file']
        reconcile_date = datetime.strptime(reconcile_date_str, '%Y-%m-%d')
        date_str_dmy = reconcile_date.strftime('%d.%m.%Y')

        # Kết nối Google
        creds = google_handler.get_google_credentials()
        gspread_client = gspread.authorize(creds)
        drive_service = build('drive', 'v3', credentials=creds)

        year_folder_id = google_handler.get_or_create_gdrive_folder(
            drive_service, f"Năm {reconcile_date.year}", config.GOOGLE_DRIVE_ROOT_FOLDER_ID
        )
        month_folder_id = google_handler.get_or_create_gdrive_folder(
            drive_service, f"Tháng {reconcile_date.month}", year_folder_id
        )

        # LẤY DỮ LIỆU POS THEO LOẠI ĐỐI SOÁT
        if reconcile_type == 'CongNo':
            try:
                pos_spreadsheet = gspread_client.open(f"CongNo.{date_str_dmy}", folder_id=month_folder_id)
            except gspread.exceptions.SpreadsheetNotFound:
                return jsonify({"status": "report_not_found", "message": f"Không tìm thấy báo cáo POS (CongNo) ngày {date_str_dmy}."})
            pos_sheet = pos_spreadsheet.worksheet('TongHopCongNo')
            pos_data = pos_sheet.get_all_values()
            pos_df = pd.DataFrame(pos_data[1:], columns=pos_data[0]) if len(pos_data) > 1 else pd.DataFrame()
        else:
            try:
                pos_spreadsheet = gspread_client.open(f"BCBH.{date_str_dmy}", folder_id=month_folder_id)
            except gspread.exceptions.SpreadsheetNotFound:
                return jsonify({"status": "report_not_found", "message": f"Không tìm thấy báo cáo POS (BCBH) ngày {date_str_dmy}."})
            pos_sheet = pos_spreadsheet.worksheet('TongHopBCBH')
            pos_data = pos_sheet.get_all_values()
            pos_df = pd.DataFrame(pos_data[1:], columns=pos_data[0]) if len(pos_data) > 1 else pd.DataFrame()

        # PHÂN LUỒNG ĐỐI SOÁT
        if reconcile_type == 'SanLuong':
            sse_df = reconciliation_handler.read_sse_product_xml(sse_file.stream)
            if sse_df is None:
                return jsonify({"status": "error", "message": "Định dạng file kế toán (sản lượng) không hợp lệ hoặc không thể đọc."}), 400
            reconciliation_results = reconciliation_handler.reconcile_product_data(pos_df, sse_df)
        elif reconcile_type == 'TienMat':
            sse_df = reconciliation_handler.read_sse_cash_xml(sse_file.stream, reconcile_date)
            if sse_df is None:
                return jsonify({"status": "error", "message": "Định dạng file kế toán (tiền mặt) không hợp lệ hoặc không thể đọc."}), 400
            reconciliation_results = reconciliation_handler.reconcile_cash_data(pos_df, sse_df)
        elif reconcile_type == 'CongNo':
            sse_df = reconciliation_handler.read_sse_debt_xml(sse_file.stream)
            if sse_df is None:
                return jsonify({"status": "error", "message": "Định dạng file kế toán (công nợ) không hợp lệ hoặc không thể đọc."}), 400
            reconciliation_results = reconciliation_handler.reconcile_debt_data(pos_df, sse_df)
        else:
            return jsonify({"status": "error", "message": "Loại đối soát không hợp lệ."}), 400

        return jsonify({"status": "success", "data": reconciliation_results, "reconcile_type": reconcile_type})

    except Exception as e:
        print(f"Lỗi khi đối soát: {e}")
        return jsonify({"status": "error", "message": f"Đã xảy ra lỗi không mong muốn: {str(e)}"}), 500

@app.route('/download_excel', methods=['POST'])
def download_excel():
    """Tạo và trả về file Excel từ dữ liệu đối soát."""
    try:
        json_data = request.get_json()
        results_data = json_data.get('data')
        reconcile_type = json_data.get('reconcile_type', 'SanLuong')

        if not results_data:
            return "No data received", 400

        df = pd.DataFrame(results_data)

        # Đặt tên cột theo loại đối soát
        if reconcile_type == 'TienMat':
            column_names = {
                'chxd_name': 'Cửa hàng',
                'product_name': 'Đối tượng',
                'pos_value': 'Tiền mặt POS (VND)',
                'sse_value': 'Tiền mặt Kế toán (VND)',
                'is_match': 'Khớp',
                'status': 'Ghi chú'
            }
        elif reconcile_type == 'CongNo':
            column_names = {
                'chxd_name': 'Cửa hàng',
                'customer_code': 'Mã khách',
                'customer_name': 'Tên khách hàng',
                'pos_value': 'Phát sinh nợ POS (VND)',
                'sse_value': 'Phát sinh nợ Kế toán (VND)',
                'is_match': 'Khớp',
                'status': 'Ghi chú'
            }
        else:  # SanLuong
            column_names = {
                'chxd_name': 'Cửa hàng',
                'product_name': 'Mặt hàng',
                'pos_value': 'Sản lượng POS',
                'sse_value': 'Sản lượng Kế toán',
                'is_match': 'Khớp',
                'status': 'Ghi chú'
            }

        # Chỉ đổi tên các cột có trong DataFrame
        df.rename(columns={k: v for k, v in column_names.items() if k in df.columns}, inplace=True)
        if 'Khớp' in df.columns:
            df['Khớp'] = df['Khớp'].apply(lambda x: 'Khớp' if bool(x) else 'Lệch')

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='KetQuaDoiSoat')
        output.seek(0)

        reconcile_date_str = datetime.now().strftime('%d-%m-%Y')
        filename = f"KetQuaDoiSoat_{reconcile_type}_{reconcile_date_str}.xlsx"

        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    except Exception as e:
        print(f"Lỗi khi tạo file Excel: {e}")
        return "Error creating Excel file", 500

if __name__ == '__main__':
    # Local dev only
    app.run(host='0.0.0.0', port=8080, debug=False)
