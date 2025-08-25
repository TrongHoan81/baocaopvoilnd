# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
import pytz
import re

# Import hàm xử lý chính từ file tasks.py
from tasks import download_report_generator

def run_daily_job():
    """
    Hàm chính để chạy tác vụ tự động.
    - Xác định ngày hôm qua theo múi giờ Việt Nam.
    - Gọi và xử lý log từ hàm download_report_generator.
    """
    # Thiết lập múi giờ Việt Nam
    vietnam_tz = pytz.timezone('Asia/Ho_Chi_Minh')
    
    # Lấy ngày hôm nay và tính ra ngày hôm qua theo múi giờ VN
    today_in_vietnam = datetime.now(vietnam_tz)
    yesterday_in_vietnam = today_in_vietnam - timedelta(days=1)
    
    print(f"--- Bắt đầu tác vụ tự động lúc {today_in_vietnam.strftime('%Y-%m-%d %H:%M:%S')} ---")
    print(f"--- Tải báo cáo cho ngày: {yesterday_in_vietnam.strftime('%Y-%m-%d')} ---")

    # Gọi generator để thực hiện công việc
    report_generator = download_report_generator(yesterday_in_vietnam)
    
    final_message = ""

    # Lặp qua các log được trả về và in ra console
    for log in report_generator:
        # Dọn dẹp chuỗi log để hiển thị đẹp hơn trên console
        cleaned_log = re.sub(r'^data:\s*|\s*\n\n\s*$', '', log)
        
        if cleaned_log.startswith("FINAL_MESSAGE:"):
            final_message = cleaned_log.replace("FINAL_MESSAGE:", "")
            print("--- Tác vụ thành công ---")
            print(f"Kết quả: {final_message}")
            break 
        elif cleaned_log.startswith("ERROR:"):
            final_message = cleaned_log.replace("ERROR:", "")
            print("--- Tác vụ thất bại ---")
            print(f"Lỗi: {final_message}")
            break
        else:
            print(cleaned_log)
    
    print(f"--- Kết thúc tác vụ lúc {datetime.now(vietnam_tz).strftime('%Y-%m-%d %H:%M:%S')} ---")

if __name__ == '__main__':
    run_daily_job()
