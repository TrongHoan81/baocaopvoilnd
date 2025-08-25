# -*- coding: utf-8 -*-
from flask import Flask, render_template, request, jsonify, Response, send_file
import io 
from datetime import datetime
import pandas as pd

# Import các thư viện cần thiết
import gspread
from googleapiclient.discovery import build

# Import các module đã được tách
import config
import google_handler
import reconciliation_handler

# Import logic tải báo cáo từ file tasks.py
from tasks import download_report_generator

app = Flask(__name__)

@app.route('/')
def index():
    """Hiển thị trang giao diện chính."""
    return render_template('index.html')

@app.route('/download_report_stream')
def download_report_stream():
    """Endpoint để tải báo cáo và truyền log về giao diện."""
    report_date_str = request.args.get('report_date')
    if not report_date_str:
        # Trả về một generator báo lỗi nếu không có ngày
        def error_generator():
            yield 'data: {"status": "error", "message": "Vui lòng chọn ngày báo cáo."}\n\n'
        return Response(error_generator(), mimetype='text/event-stream')

    report_date = datetime.strptime(report_date_str, '%Y-%m-%d')
    # Gọi trực tiếp generator từ file tasks
    return Response(download_report_generator(report_date), mimetype='text/event-stream')

@app.route('/reconcile', methods=['POST'])
def reconcile():
    """Endpoint để xử lý yêu cầu đối soát."""
    try:
        reconcile_date_str = request.form.get('reconcile_date')
        if not reconcile_date_str:
            return jsonify({"status": "error", "message": "Vui lòng chọn ngày đối soát."}), 400
        
        if 'accounting_file' not in request.files:
            return jsonify({"status": "error", "message": "Vui lòng tải lên file từ phần mềm kế toán."}), 400

        sse_file = request.files['accounting_file']
        reconcile_date = datetime.strptime(reconcile_date_str, '%Y-%m-%d')
        date_str_dmy = reconcile_date.strftime('%d.%m.%Y')
        
        creds = google_handler.get_google_credentials()
        gspread_client = gspread.authorize(creds)
        drive_service = build('drive', 'v3', credentials=creds)
        
        year_folder_id = google_handler.get_or_create_gdrive_folder(drive_service, f"Năm {reconcile_date.year}", config.GOOGLE_DRIVE_ROOT_FOLDER_ID)
        month_folder_id = google_handler.get_or_create_gdrive_folder(drive_service, f"Tháng {reconcile_date.month}", year_folder_id)
        
        try:
            pos_spreadsheet = gspread_client.open(f"SanLuong.{date_str_dmy}", folder_id=month_folder_id)
        except gspread.exceptions.SpreadsheetNotFound:
            return jsonify({"status": "report_not_found", "message": f"Không tìm thấy báo cáo POS ngày {date_str_dmy}."})

        pos_sheet = pos_spreadsheet.worksheet('TongHopSanLuong')
        pos_data = pos_sheet.get_all_values()
        if len(pos_data) < 2:
            pos_df = pd.DataFrame()
        else:
            pos_df = pd.DataFrame(pos_data[1:], columns=pos_data[0])
        
        sse_df = reconciliation_handler.read_sse_xml(sse_file.stream)
        if sse_df is None:
            return jsonify({"status": "error", "message": "Định dạng file kế toán không hợp lệ hoặc không thể đọc."}), 400

        reconciliation_results = reconciliation_handler.reconcile_data(pos_df, sse_df)
        
        return jsonify({"status": "success", "data": reconciliation_results})

    except Exception as e:
        print(f"Lỗi khi đối soát: {e}")
        return jsonify({"status": "error", "message": f"Đã xảy ra lỗi không mong muốn: {str(e)}"}), 500

@app.route('/download_excel', methods=['POST'])
def download_excel():
    """Tạo và trả về file Excel từ dữ liệu đối soát."""
    try:
        results_data = request.get_json()
        if not results_data:
            return "No data received", 400

        df = pd.DataFrame(results_data)
        df.rename(columns={
            'chxd_name': 'Cửa hàng',
            'product_name': 'Mặt hàng',
            'pos_quantity': 'Sản lượng POS',
            'sse_quantity': 'Sản lượng Kế toán',
            'is_match': 'Khớp'
        }, inplace=True)
        df['Khớp'] = df['Khớp'].apply(lambda x: 'Khớp' if x else 'Lệch')

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='KetQuaDoiSoat')
        output.seek(0)

        reconcile_date = datetime.now().strftime('%d-%m-%Y')
        filename = f"KetQuaDoiSoat_{reconcile_date}.xlsx"
        
        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    except Exception as e:
        print(f"Lỗi khi tạo file Excel: {e}")
        return "Error creating Excel file", 500

# Gunicorn sẽ sử dụng đối tượng 'app' này
# Không cần if __name__ == '__main__': app.run() nữa cho production
