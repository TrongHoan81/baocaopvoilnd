# -*- coding: utf-8 -*-
"""
processor_bh03.py
GIỮ NGUYÊN thuật toán ổn định để đọc BH03 dạng biểu mẫu (.xlsx) theo MỤC II/III/IV.
CHỈ THÊM:
 - Ánh xạ mã khách từ Google Sheet DSKH (truyền từ tasks.py)
 - Điền "Không tìm thấy mã khách" khi không map được
 - Loại trừ "Công nợ chung" (không xem như 1 khách hàng)
"""
from __future__ import annotations

import re
import unicodedata
from typing import Optional, List, Dict, Tuple

import pandas as pd
import config


# ---------- Constants ----------
NO_CODE_PLACEHOLDER = "Không tìm thấy mã khách"
# Danh sách tên KH cần bỏ (so khớp theo dạng đã chuẩn hoá)
SKIP_NAMES = {"cong no chung"}


# ---------- Helpers ----------
def _vn_normalize(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip().lower()
    s = "".join(ch for ch in unicodedata.normalize("NFD", s) if unicodedata.category(ch) != "Mn")
    return re.sub(r"[\s\._\-]+", " ", s).strip()


def _is_digit_or_stt(s: str) -> bool:
    if not s:
        return False
    s = str(s).strip()
    if s.endswith('.'):
        s = s[:-1]
    return s.isdigit()


def _to_float(x) -> float:
    try:
        if pd.isna(x):
            return 0.0
        return float(str(x).replace(',', ''))
    except Exception:
        return 0.0


# ---------- Tổng hợp BCBH theo MỤC IV + dòng Tổng cộng ----------
def process_and_validate_bh03(df: pd.DataFrame, store_name: str) -> Optional[dict]:
    """
    Trả về dict {'Tên CHXD', <các sản phẩm>, 'Tổng sản lượng','Doanh thu','Tiền mặt'}
    hoặc None nếu thực sự rỗng.
    """
    if not isinstance(df, pd.DataFrame) or df.empty:
        return None

    # 1) Chuẩn bị
    target_products: List[str] = list(getattr(config, "TARGET_PRODUCTS_BH03", []) or [])
    summary_data = {prod: 0.0 for prod in target_products}
    revenue = 0.0
    cash_payment = 0.0

    # 2) MỤC IV: 'Tổng cộng mặt hàng' -> cộng SL cột 7 (index 6)
    start_index = -1
    for i, row in df.iterrows():
        a_val = str(row.iloc[0]).strip() if len(row) > 0 and pd.notna(row.iloc[0]) else ''
        if a_val.startswith('IV'):
            start_index = i + 1
            break

    if start_index != -1:
        end_index = len(df)
        for i in range(start_index, len(df)):
            a_val = str(df.iloc[i, 0]).strip() if pd.notna(df.iloc[i, 0]) else ''
            if a_val.startswith(('V.', 'VI.')):
                end_index = i
                break
        section_df = df.iloc[start_index:end_index]
        for _, row in section_df.iterrows():
            product_name = row.iloc[1] if len(row) > 1 else None
            if isinstance(product_name, str):
                cleaned = product_name.strip()
                if cleaned in summary_data:
                    qty = _to_float(row.iloc[6] if len(row) > 6 else 0)
                    summary_data[cleaned] += qty

    # 3) Doanh thu & Tiền mặt
    for _, row in df.iterrows():
        colA = str(row.iloc[0]).strip() if len(row) > 0 and pd.notna(row.iloc[0]) else ''
        colB = str(row.iloc[1]).strip() if len(row) > 1 and pd.notna(row.iloc[1]) else ''
        if colB == 'Tổng cộng':
            revenue = _to_float(row.iloc[16] if len(row) > 16 else 0)
        if colA == 'I' and colB == 'Xuất bán lẻ':
            cash_payment = _to_float(row.iloc[16] if len(row) > 16 else 0)

    total_quantity = sum(summary_data.values())
    if total_quantity > 0 or revenue > 0 or cash_payment > 0:
        final_row = {"Tên CHXD": store_name}
        final_row.update(summary_data)
        final_row["Tổng sản lượng"] = total_quantity
        final_row["Doanh thu"] = revenue
        final_row["Tiền mặt"] = cash_payment
        return final_row
    return None


# ---------- DSKH helpers ----------
def _build_customer_index(dskh_df: pd.DataFrame) -> Tuple[Dict[str,str], Dict[str,str]]:
    """Tạo map: TenKhachHang/alias -> MaKhach"""
    if dskh_df is None or dskh_df.empty:
        return {}, {}
    cols_lower = {str(c).lower(): c for c in dskh_df.columns}
    name_col = cols_lower.get("tenkhachhang") or cols_lower.get("ten khach hang")
    code_col = cols_lower.get("makhach") or cols_lower.get("ma khach")
    alias_col = cols_lower.get("alias")
    if not name_col or not code_col:
        # fallback vị trí
        try:
            name_col = dskh_df.columns[1]
            code_col = dskh_df.columns[2] if len(dskh_df.columns) > 2 else dskh_df.columns[1]
            if str(code_col).lower() not in ("makhach", "ma khach") and len(dskh_df.columns) > 3:
                code_col = dskh_df.columns[3]
        except Exception:
            raise ValueError("DSKH thiếu cột 'TenKhachHang'/'MaKhach'.")
    exact, alias = {}, {}
    for _, r in dskh_df.iterrows():
        name = _vn_normalize(r.get(name_col))
        code = str(r.get(code_col) or '').strip()
        if name and code:
            exact[name] = code
        if alias_col:
            raw = str(r.get(alias_col) or '')
            if raw.strip():
                for al in raw.split(';'):
                    al_norm = _vn_normalize(al)
                    if al_norm:
                        alias[al_norm] = code
    return exact, alias


def _resolve_customer_code(customer_name: str, exact_map: Dict[str,str], alias_map: Dict[str,str]) -> str:
    """Trả về mã khách; nếu không tìm thấy thì trả 'Không tìm thấy mã khách'."""
    key = _vn_normalize(customer_name)
    if not key:
        return NO_CODE_PLACEHOLDER
    if key in exact_map:
        return exact_map[key]
    if key in alias_map:
        return alias_map[key]
    return NO_CODE_PLACEHOLDER


# ---------- Bóc tách công nợ theo MỤC II/III ----------
def process_debt_details(df: pd.DataFrame, store_name: str, dskh_df: pd.DataFrame) -> List[dict]:
    """
    Trả về danh sách dict:
      {'Store','Customer_Name','Customer_Code','Product','Quantity','Unit_Price','Debt'}
    Giữ nguyên cách đọc: 
      - Vào vùng II/III => đọc STT + tên KH (cột A/B)
      - Dòng sản phẩm: cột A trống, cột B là tên SP; số lượng ở cột 7, đơn giá cột 8, nợ cột 17
    """
    results: List[dict] = []
    if df is None or df.empty:
        return results

    exact_map, alias_map = _build_customer_index(dskh_df)

    in_debt_section = False
    current_customer: Optional[str] = None

    for _, row in df.iterrows():
        col_A = str(row.iloc[0]).strip() if len(row) > 0 and pd.notna(row.iloc[0]) else ''
        col_B = str(row.iloc[1]).strip() if len(row) > 1 and pd.notna(row.iloc[1]) else ''

        # Bắt đầu vùng công nợ ở II hoặc III
        if col_A in ['II', 'III']:
            in_debt_section = True
            current_customer = None
            continue

        # Kết thúc vùng công nợ khi sang IV (Tổng cộng mặt hàng) hoặc sau nữa
        if in_debt_section and col_A.startswith('IV'):
            in_debt_section = False
            current_customer = None
            continue

        if not in_debt_section:
            continue

        # Dòng tiêu đề khách hàng: STT ở cột A
        if col_A and col_A.rstrip('.').isdigit() and col_B:
            # BỎ QUA khách thuộc danh sách loại trừ (vd: Công nợ chung)
            if _vn_normalize(col_B) in SKIP_NAMES:
                current_customer = None
                continue
            current_customer = col_B
            continue

        # Dòng sản phẩm thuộc khách hiện tại
        if (col_A == '' or col_A is None) and col_B and current_customer:
            product = col_B
            quantity = _to_float(row.iloc[6] if len(row) > 6 else 0)
            unit_price = _to_float(row.iloc[7] if len(row) > 7 else 0)
            debt = _to_float(row.iloc[16] if len(row) > 16 else 0)

            code = _resolve_customer_code(current_customer, exact_map, alias_map)
            results.append({
                'Store': store_name,
                'Customer_Name': current_customer,
                'Customer_Code': code,
                'Product': product,
                'Quantity': quantity,
                'Unit_Price': unit_price,
                'Debt': debt
            })

    return results
