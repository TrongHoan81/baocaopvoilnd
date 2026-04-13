#!/usr/bin/env bash
set -e

# Đi đến chính xác thư mục chứa dự án trên VPS
cd /root/PVOIL_KinhDoanh

# Thiết lập môi trường
export TZ=Asia/Ho_Chi_Minh
export PYTHONUNBUFFERED=1

# Kích hoạt môi trường ảo (Đã sửa lại thành .venv khớp 100% với hệ thống của bạn)
source .venv/bin/activate

# Lấy nhãn thời gian (Năm-Tháng) để đặt tên file log
ts=$(date +%Y-%m)
mkdir -p logs

# Chạy file Python và ghi toàn bộ kết quả vào thư mục logs
python monthly_job.py >> "logs/monthly_hd01_${ts}.log" 2>&1