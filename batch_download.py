# -*- coding: utf-8 -*-
"""
batch_download.py
Chạy tự động chức năng "tải báo cáo" cho nhiều ngày liên tiếp, sử dụng lại
generator download_report_generator() có sẵn trong tasks.py.

Cách dùng:
  1) Theo tháng:
     python batch_download.py --year 2025 --month 8 --delay 10

  2) Theo khoảng ngày:
     python batch_download.py --start 2025-08-01 --end 2025-08-31 --delay 5
"""
import argparse
import sys
import time
from datetime import datetime, timedelta

# Import đúng module hiện có trong dự án
from tasks import download_report_generator

def daterange(start_date: datetime, end_date: datetime):
    """Sinh lần lượt các ngày (inclusive) từ start_date đến end_date."""
    cur = start_date
    while cur <= end_date:
        yield cur
        cur += timedelta(days=1)

def run_for_range(start_date: datetime, end_date: datetime, delay_seconds: int):
    """Chạy tải báo cáo cho từng ngày trong khoảng [start_date, end_date]."""
    total_days = (end_date - start_date).days + 1
    print(f"===> Bắt đầu batch: {total_days} ngày, từ {start_date:%d/%m/%Y} đến {end_date:%d/%m/%Y}. Delay mỗi ngày: {delay_seconds}s\n")

    for idx, day in enumerate(daterange(start_date, end_date), start=1):
        print(f"\n========== [Ngày {idx}/{total_days}] {day:%d/%m/%Y} ==========")
        try:
            # Gọi lại generator sẵn có để tận dụng toàn bộ logic tải/ghép/tạo tổng hợp
            for line in download_report_generator(day):
                # Mỗi 'line' là 1 dòng SSE bắt đầu bằng "data: ...\n\n"
                # In ra console cho dễ theo dõi.
                text = line.strip()
                if text.startswith("data:"):
                    text = text[len("data:"):].strip()
                print(text)
        except KeyboardInterrupt:
            print("\n==> Đã nhận Ctrl+C. Dừng batch.")
            sys.exit(1)
        except Exception as e:
            print(f"!! Lỗi ngoài ý muốn trong ngày {day:%d/%m/%Y}: {e}")

        if day < end_date:
            print(f"---- Hoàn tất {day:%d/%m/%Y}. Nghỉ {delay_seconds}s trước khi chạy ngày kế tiếp... ----")
            time.sleep(delay_seconds)

    print("\n===> Batch hoàn tất!")

def parse_args():
    p = argparse.ArgumentParser(description="Chạy batch tải báo cáo POS nhiều ngày.")
    p.add_argument("--start", type=str, help="Ngày bắt đầu, định dạng YYYY-MM-DD (vd: 2025-08-01)")
    p.add_argument("--end", type=str, help="Ngày kết thúc, định dạng YYYY-MM-DD (vd: 2025-08-31)")
    p.add_argument("--year", type=int, help="Năm (vd: 2025)")
    p.add_argument("--month", type=int, help="Tháng (1-12)")
    p.add_argument("--delay", type=int, default=5, help="Số giây nghỉ giữa các ngày (mặc định 5s)")
    args = p.parse_args()

    # Trường hợp 1: chỉ định start & end
    if args.start and args.end:
        try:
            start_date = datetime.strptime(args.start, "%Y-%m-%d")
            end_date = datetime.strptime(args.end, "%Y-%m-%d")
        except ValueError:
            print("Lỗi: --start/--end phải theo định dạng YYYY-MM-DD, ví dụ 2025-08-01")
            sys.exit(2)
        return start_date, end_date, args.delay

    # Trường hợp 2: chỉ định theo tháng
    if args.year and args.month:
        if not (1 <= args.month <= 12):
            print("Lỗi: --month phải nằm trong 1..12")
            sys.exit(2)
        # Ngày đầu tháng
        start_date = datetime(args.year, args.month, 1)
        # Tính ngày cuối tháng (đi đến ngày 1 của tháng sau, trừ đi 1 ngày)
        if args.month == 12:
            end_date = datetime(args.year + 1, 1, 1) - timedelta(days=1)
        else:
            end_date = datetime(args.year, args.month + 1, 1) - timedelta(days=1)
        return start_date, end_date, args.delay

    print("Bạn phải chọn 1 trong 2 cách:")
    print("  - Theo tháng: --year 2025 --month 8 [--delay 10]")
    print("  - Theo khoảng ngày: --start 2025-08-01 --end 2025-08-31 [--delay 5]")
    sys.exit(2)

if __name__ == "__main__":
    s, e, d = parse_args()
    run_for_range(s, e, d)
