#!/usr/bin/env bash
set -e
cd /root/PVOIL_KinhDoanh
export TZ=Asia/Ho_Chi_Minh
export PYTHONUNBUFFERED=1
source .venv/bin/activate
ts=$(date +%Y-%m-%d)
mkdir -p logs
python daily_job.py >> "logs/daily_${ts}.log" 2>&1
