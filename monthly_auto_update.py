# -*- coding: utf-8 -*-
"""
monthly_auto_update.py
Hook nhẹ để gọi cập nhật "Tổng hợp tháng" cho đúng NGÀY vừa tải báo cáo.
"""

from datetime import datetime
from monthly_summary_gsheet import update_monthly_for_single_day

def update_monthly_after_download(report_date: datetime):
    """
    Gọi sau khi file BCBH.{dd.MM.yyyy} đã được tạo xong.
    Chỉ cập nhật cột ngày tương ứng trong file 'Tổng hợp tháng m.yyyy'.
    """
    update_monthly_for_single_day(report_date)
