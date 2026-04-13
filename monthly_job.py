# -*- coding: utf-8 -*-
import sys
import traceback
import datetime as dt
from zoneinfo import ZoneInfo
import tasks  # Import bộ điều phối tác vụ chính

def run_hd01_for_last_month():
    """
    Hàm tính toán lùi 1 tháng và gọi lệnh tải báo cáo HD01.
    Ví dụ: Chạy vào lúc 04:00 ngày 01/04/2026 -> Tải báo cáo Tháng 03/2026.
    """
    # 1. Lấy giờ hệ thống hiện tại theo múi giờ VN
    now = dt.datetime.now(ZoneInfo("Asia/Ho_Chi_Minh"))
    
    # 2. Tính toán tháng trước (Bằng cách lấy ngày đầu tiên của tháng này trừ đi 1 ngày)
    first_day_this_month = now.replace(day=1)
    last_day_last_month = first_day_this_month - dt.timedelta(days=1)
    
    target_month = last_day_last_month.month
    target_year = last_day_last_month.year

    print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] BẮT ĐẦU TỰ ĐỘNG TẢI HD01 - KỲ BÁO CÁO: {target_month}/{target_year}")

    try:
        # 3. Gọi generator tải báo cáo (Truyền chính xác tham số của luồng HD01)
        gen = tasks.download_report_generator(
            report_date=None,  # HD01 không cần ngày
            report_type='HD01',
            station_code_filter='ALL',
            report_year=str(target_year),
            report_month=str(target_month)
        )
        
        # 4. In Log ra file để theo dõi tiến trình
        for chunk in gen:
            # Làm sạch log JSON chuẩn bị cho việc đọc trên Terminal
            if isinstance(chunk, str):
                clean_chunk = chunk.replace('data: ', '').strip()
                if clean_chunk:
                    print(clean_chunk)
            else:
                print(chunk)
                
        print(f"[{dt.datetime.now(ZoneInfo('Asia/Ho_Chi_Minh')).strftime('%Y-%m-%d %H:%M:%S')}] => HOÀN TẤT TẢI HD01 THÁNG {target_month}/{target_year}")
        
    except Exception as e:
        print("!! LỖI NGHIÊM TRỌNG khi tải HD01:", type(e).__name__, e)
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    run_hd01_for_last_month()