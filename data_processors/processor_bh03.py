# -*- coding: utf-8 -*-
from __future__ import annotations
import re, unicodedata
from typing import Optional, List, Dict, Tuple
import pandas as pd
import config

NO_CODE_PLACEHOLDER = "Không tìm thấy mã khách"
SKIP_NAMES = {"cong no chung"}
# Ưu tiên cột J (index 9), fallback cột thứ 17 (index 16) cho các chỗ KHÁC doanh thu/tiền mặt
AMOUNT_INDEXES = (9, 16)

# ---------- Helpers ----------
def _vn_normalize(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip().lower()
    s = unicodedata.normalize('NFKC', s)
    s = re.sub(r"\s+", " ", s)
    return s

def _norm_label(s: str) -> str:
    # Chuẩn hoá nhãn để so sánh: lower + bỏ ':' '.' + rút gọn khoảng trắng
    s = _vn_normalize(s)
    s = s.replace(":", "").replace(".", "")
    return s

def _is_digit_or_stt(s: str) -> bool:
    if not isinstance(s, str):
        s = str(s)
    s = s.strip()
    if not s:
        return False
    return bool(re.match(r"^(?:\d+\.?|stt)$", s.lower()))

def _to_float(x) -> float:
    """
    Ép số an toàn, bảo toàn phần thập phân cho các kiểu:
    - "1.234,56" (vi_VN), "1,234.56" (en_US)
    - "(1.234,56)" số âm dạng ngoặc
    - có/không khoảng trắng cứng
    """
    if x is None:
        return 0.0
    s = str(x).strip()
    if s == "":
        return 0.0
    neg = False
    if s.startswith("(") and s.endswith(")"):
        neg = True
        s = s[1:-1].strip()
    s = s.replace("\u00A0", " ").replace("\u202F", " ")
    s = re.sub(r"\s+", "", s)

    try:
        val = float(s)
        return -val if neg and val >= 0 else val
    except Exception:
        pass

    has_d = "." in s
    has_c = "," in s
    if has_d and has_c:
        last_d = s.rfind(".")
        last_c = s.rfind(",")
        if last_d > last_c:
            s2 = s.replace(",", "")
        else:
            s2 = s.replace(".", "").replace(",", ".")
        try:
            val = float(s2)
            return -val if neg and val >= 0 else val
        except Exception:
            pass

    try:
        if has_d:
            if s.count(".") == 1 and re.match(r".+\.\d{1,3}$", s):
                pass
            else:
                s = s.replace(".", "")
        elif has_c:
            if s.count(",") == 1 and re.match(r".+,\d{1,3}$", s):
                s = s.replace(",", ".")
            else:
                s = s.replace(",", "")
        elif has_d and s.count(".") > 1:
            s = s.replace(".", "")
        s = re.sub(r"[^0-9\.\-]", "", s)
        if s in ("", ".", "-", "-.", ".-"):
            return 0.0
        val = float(s)
        return -val if neg and val >= 0 else val
    except Exception:
        s2 = re.sub(r"[^0-9]", "", s)
        try:
            if s2 in ("", "-"):
                return 0.0
            val = float(s2)
            return -val if neg and val >= 0 else val
        except Exception:
            return 0.0

def _get_amount_from_row(row: pd.Series) -> float:
    """
    Hàm chung cho các nơi KHÁC doanh thu/tiền mặt:
    - Thử các cột ưu tiên (J, 17) rồi fallback quét lùi 6 ô cuối.
    """
    for idx in AMOUNT_INDEXES:
        if len(row) > idx and pd.notna(row.iloc[idx]):
            v = _to_float(row.iloc[idx])
            if v != 0.0:
                return v
    n = len(row)
    for i in range(n-1, max(-1, n-7), -1):
        try:
            cell = row.iloc[i]
        except Exception:
            continue
        if pd.isna(cell):
            continue
        v = _to_float(cell)
        if v != 0.0:
            return v
    return 0.0

def _get_amount_col_j(row: pd.Series) -> float:
    """
    CHUYÊN DỤNG cho 'Doanh thu' và 'Tiền mặt' của BH03:
    - LUÔN lấy đúng cột J (index 9). Không fallback.
    """
    if len(row) > 9 and pd.notna(row.iloc[9]):
        return _to_float(row.iloc[9])
    return 0.0

# ---------- BCBH (Mục IV + Doanh thu/Tiền mặt) ----------
def process_and_validate_bh03(df: pd.DataFrame, store_name: str) -> Optional[dict]:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return None

    target_products: List[str] = config.TARGET_PRODUCTS_BH03
    summary_data = {p: 0.0 for p in target_products}
    revenue = 0.0
    cash_payment = 0.0

    # MỤC IV: cộng SL ở cột 5 (index 4)
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
                    # lấy sản lượng từ cột E (index 4)
                    qty = _to_float(row.iloc[4] if len(row) > 4 else 0)
                    summary_data[cleaned] += qty

    # Doanh thu & Tiền mặt
    label_tong_cong = _norm_label("Tổng cộng")
    label_xuat_ban_le = _norm_label("Xuất bán lẻ")

    for _, row in df.iterrows():
        colA_raw = row.iloc[0] if len(row) > 0 else ""
        colB_raw = row.iloc[1] if len(row) > 1 else ""
        colA = str(colA_raw).strip() if pd.notna(colA_raw) else ""
        colB = str(colB_raw).strip() if pd.notna(colB_raw) else ""

        normA = _norm_label(colA)
        normB = _norm_label(colB)

        # 1) Doanh thu: 'Tổng cộng' có thể ở cột A hoặc B
        if normA == label_tong_cong or normB == label_tong_cong:
            revenue = _get_amount_col_j(row)  # CHỈ cột J
            continue

        # 2) Tiền mặt: hàng "I. Xuất bán lẻ"
        if normA == "i" and normB == label_xuat_ban_le:
            cash_payment = _get_amount_col_j(row)  # CHỈ cột J
            continue

    total_quantity = float(sum(summary_data.values()))
    if total_quantity > 0 or revenue > 0 or cash_payment > 0:
        final_row = {"Tên CHXD": store_name}
        final_row.update(summary_data)
        final_row["Tổng sản lượng"] = round(total_quantity, 3)
        final_row["Doanh thu"] = revenue
        final_row["Tiền mặt"] = cash_payment
        return final_row
    return None

# ---------- DSKH helpers ----------
def _build_customer_index(dskh_df: pd.DataFrame) -> Tuple[Dict[str,str], Dict[str,str]]:
    if dskh_df is None or dskh_df.empty:
        return {}, {}
    cols_lower = {str(c).lower(): c for c in dskh_df.columns}
    name_col = cols_lower.get("tenkhachhang") or cols_lower.get("ten khach hang")
    code_col = cols_lower.get("makhachhang") or cols_lower.get("ma khach hang")
    alias_col = cols_lower.get("tenthuonggoi") or cols_lower.get("ten thuong goi")
    if not name_col or not code_col:
        return {}, {}
    exact_map: Dict[str, str] = {}
    alias_map: Dict[str, str] = {}
    for _, row in dskh_df.iterrows():
        name = _vn_normalize(row.get(name_col, ""))
        code = str(row.get(code_col, "")).strip()
        alias = _vn_normalize(row.get(alias_col, "")) if alias_col else ""
        if name:
            exact_map[name] = code
        if alias:
            alias_map[alias] = code
    return exact_map, alias_map

def _resolve_customer_code(customer_name: str, exact_map: Dict[str,str], alias_map: Dict[str,str]) -> str:
    name_norm = _vn_normalize(customer_name or "")
    if not name_norm or name_norm in SKIP_NAMES:
        return NO_CODE_PLACEHOLDER
    if name_norm in exact_map:
        return exact_map[name_norm]
    if name_norm in alias_map:
        return alias_map[name_norm]
    m = re.match(r"^([^:\-]+)[:\-].*$", customer_name or "")
    if m:
        base = _vn_normalize(m.group(1))
        if base in exact_map:
            return exact_map[base]
        if base in alias_map:
            return alias_map[base]
    return NO_CODE_PLACEHOLDER

# ---------- Chi tiết công nợ (MỤC II/III) ----------
def process_debt_details(df: pd.DataFrame, store_name: str, dskh_df: pd.DataFrame) -> List[dict]:
    results: List[dict] = []
    if df is None or df.empty:
        return results

    exact_map, alias_map = _build_customer_index(dskh_df)
    in_debt_section = False
    current_customer: Optional[str] = None

    for _, row in df.iterrows():
        col_A = str(row.iloc[0]).strip() if len(row) > 0 and pd.notna(row.iloc[0]) else ''
        col_B = str(row.iloc[1]).strip() if len(row) > 1 and pd.notna(row.iloc[1]) else ''

        if col_A.startswith("II") or col_A.startswith("III"):
            in_debt_section = True
            current_customer = None
            continue

        if not in_debt_section:
            continue

        # ---- BẮT ĐẦU SỬA ĐỔI ----
        if col_A.startswith("IV"):
            break
        # ---- KẾT THÚC SỬA ĐỔI ----

        if _is_digit_or_stt(col_A) and col_B:
            current_customer = col_B
            continue

        if current_customer and not _is_digit_or_stt(col_A):
            product = col_B
            # Giữ nguyên logic cũ cho phần công nợ (không yêu cầu đổi)
            quantity = _to_float(row.iloc[6] if len(row) > 6 else 0)
            unit_price = _to_float(row.iloc[7] if len(row) > 7 else 0)
            debt = _get_amount_from_row(row)
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