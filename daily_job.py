# -*- coding: utf-8 -*-
import sys, traceback, datetime as dt
from zoneinfo import ZoneInfo
import tasks  # dùng download_report_generator(report_date)
try:
    from monthly_summary_gsheet import update_monthly_for_single_day
except Exception:
    update_monthly_for_single_day = None

def run_for_date(d: dt.date):
    now = dt.datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] BẮT ĐẦU tải báo cáo cho ngày {d:%d/%m/%Y}")
    try:
        gen = tasks.download_report_generator(d)
        for chunk in gen:
            print(chunk, end="" if isinstance(chunk, str) else "\n")
        print(f"=> HOÀN TẤT tải & tổng hợp ngày {d}")
    except Exception as e:
        print("!! LỖI khi tải báo cáo:", type(e).__name__, e)
        traceback.print_exc()
        return 1

    if update_monthly_for_single_day:
        try:
            update_monthly_for_single_day(d)
            print(f"=> ĐÃ cập nhật 'Tổng hợp tháng' cho ngày {d}")
        except Exception as e:
            print("!! LỖI khi cập nhật Tổng hợp tháng:", type(e).__name__, e)
            traceback.print_exc()
    print(f"=> DONE ngày {d}")
    return 0

def main():
    tz = ZoneInfo("Asia/Ho_Chi_Minh")
    report_date = dt.datetime.now(tz).date() - dt.timedelta(days=1)  # hôm qua
    if len(sys.argv) >= 2:
        report_date = dt.date.fromisoformat(sys.argv[1])  # cho phép chạy thử 1 ngày
    return run_for_date(report_date)

if __name__ == "__main__":
    sys.exit(main())
