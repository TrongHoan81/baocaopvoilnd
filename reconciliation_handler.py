# -*- coding: utf-8 -*-
import pandas as pd
import config
import xml.etree.ElementTree as ET
import re
from datetime import datetime
import logging
import unicodedata
import uuid

logger = logging.getLogger(__name__)

# ==============================================================================
# HÀM DÙNG CHUNG
# ==============================================================================

def _strip_diacritics(s: str) -> str:
    """Bỏ dấu tiếng Việt (đủ dùng cho so khớp mềm)."""
    if s is None:
        return ''
    s = str(s)
    repl = {
        'à':'a','á':'a','ả':'a','ã':'a','ạ':'a','ă':'a','ằ':'a','ắ':'a','ẳ':'a','ẵ':'a','ặ':'a','â':'a','ầ':'a','ấ':'a','ẩ':'a','ẫ':'a','ậ':'a',
        'è':'e','é':'e','ẻ':'e','ẽ':'e','ẹ':'e','ê':'e','ề':'e','ế':'e','ể':'e','ễ':'e','ệ':'e',
        'ì':'i','í':'i','ỉ':'i','ĩ':'i','ị':'i',
        'ò':'o','ó':'o','ỏ':'o','õ':'o','ọ':'o','ô':'o','ồ':'o','ố':'o','ổ':'o','ỗ':'o','ộ':'o','ơ':'o','ờ':'o','ớ':'o','ở':'o','ỡ':'o','ợ':'o',
        'ù':'u','ú':'u','ủ':'u','ũ':'u','ụ':'u','ư':'u','ừ':'u','ứ':'u','ử':'u','ữ':'u','ự':'u',
        'ỳ':'y','ý':'y','ỷ':'y','ỹ':'y','ỵ':'y',
        'đ':'d','À':'A','Á':'A','Ả':'A','Ã':'A','Ạ':'A','Ă':'A','Ằ':'A','Ắ':'A','Ẳ':'A','Ẵ':'A','Ặ':'A','Â':'A','Ầ':'A','Ấ':'A','Ẩ':'A','Ẫ':'A','Ậ':'A',
        'È':'E','É':'E','Ẻ':'E','Ẽ':'E','Ẹ':'E','Ê':'E','Ề':'E','Ế':'E','Ể':'E','Ễ':'E','Ệ':'E',
        'Ì':'I','Í':'I','Ỉ':'I','Ĩ':'I','Ị':'I',
        'Ò':'O','Ó':'O','Ỏ':'O','Õ':'O','Ọ':'O','Ô':'O','Ồ':'O','Ố':'O','Ổ':'O','Ỗ':'O','Ộ':'O','Ơ':'O','Ờ':'O','Ớ':'O','Ở':'O','Ỡ':'O','Ợ':'O',
        'Ù':'U','Ú':'U','Ủ':'U','Ũ':'U','Ụ':'U','Ư':'U','Ừ':'U','Ứ':'U','Ử':'U','Ữ':'U','Ự':'U',
        'Ỳ':'Y','Ý':'Y','Ỷ':'Y','Ỹ':'Y','Ỵ':'Y','Đ':'D'
    }
    return ''.join(repl.get(c, c) for c in s)

def _norm_key(s: str) -> str:
    """Chuẩn hoá để so khớp: lower + bỏ dấu + gộp khoảng trắng."""
    s = _strip_diacritics(s).lower().strip()
    return re.sub(r'\s+', ' ', s)

def _canon_store_key(name: str) -> str:
    """Khoá CHXD thống nhất để ghép: bỏ phần trong ngoặc cuối, lower + bỏ dấu + gộp khoảng trắng."""
    if not isinstance(name, str):
        name = ''
    s = re.sub(r'\s*\(.*?\)\s*$', '', name.strip())  # bỏ mọi "(...)" ở cuối
    return _norm_key(s)

def _canon_store_display(name: str) -> str:
    """Tên CHXD hiển thị: bỏ hậu tố '(...)' cuối, giữ nguyên hoa/thường."""
    if not isinstance(name, str):
        return ''
    return re.sub(r'\s*\(.*?\)\s*$', '', name.strip())

def _norm_code(s: str) -> str:
    """Chuẩn hoá mã KH/đơn vị: upper, bỏ khoảng trắng, đổi O→0 khi ngay trước số."""
    s = '' if s is None else str(s)
    s = re.sub(r'\s+', '', s).upper()
    s = re.sub(r'O(?=\d)', '0', s)  # KDNLO72 ~ KDNL072
    return s

def _codes_equal(a: str, b: str) -> bool:
    return _norm_code(a) == _norm_code(b)

def find_header_row_index(all_rows):
    """Tìm dòng tiêu đề: chứa 1 trong các cột khoá."""
    must_have_any = {'ma khach', 'ma kh', 'ten khach', 'ten khach hang', 'phat sinh no', 'ps no', 'stt'}
    for i, row in enumerate(all_rows):
        safe = [str(x) if x is not None else '' for x in row]
        if _norm_key(' '.join(safe)) == '':
            continue
        keys = {_norm_key(c) for c in safe if c}
        if keys & must_have_any:
            return i
    return -1

def clean_and_convert_to_numeric(series):
    return pd.to_numeric(
        series.astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False),
        errors='coerce'
    ).fillna(0).round(0)

# ==============================================================================
# SẢN LƯỢNG
# ==============================================================================

def find_date_in_headers(header_list):
    for header in header_list:
        if header:
            m = re.search(r'Ngày \d{2}/\d{2}', header)
            if m:
                return m.group(0)
    return None

def read_sse_product_xml(file_stream):
    try:
        xml_bytes = file_stream.read()
        try:
            xml_content = xml_bytes.decode('utf-8')
        except UnicodeDecodeError:
            xml_content = xml_bytes.decode('windows-1252')
        xml_content = xml_content.replace('xmlns="urn:schemas-microsoft-com:office:spreadsheet"', '')
        root = ET.fromstring(xml_content)

        all_rows = [[cell.findtext("Data") for cell in row.findall("Cell")]
                    for row in root.findall(".//Table/Row")]

        header_row_index = find_header_row_index(all_rows)
        if header_row_index == -1 or len(all_rows) < header_row_index + 2:
            raise ValueError("File XML sản lượng không hợp lệ (không thấy tiêu đề).")

        header_row = all_rows[header_row_index]
        date_str = find_date_in_headers(header_row)
        if not date_str:
            raise ValueError("Không xác định được ngày từ tiêu đề XML sản lượng.")

        colmap = {
            'sse_ma_khach': 'Mã khách',
            'sse_ten_khach': 'Tên khách',
            'Dầu Điêzen 0,001S Mức 5': f'{date_str} Dầu DO 0,001S-V',
            'Dầu mỡ nhờn': f'{date_str} Dầu mỡ nhờn',
            'Dầu Điêzen 0,05S Mức 2': f'{date_str} DO',
            'Xăng RON95 Mức 3': f'{date_str} Xăng A95',
            'Xăng E5 RON92 Mức 2': f'{date_str} Xăng E5'
        }
        idx = {}
        for k, colname in colmap.items():
            if colname in header_row:
                idx[k] = header_row.index(colname)
        if 'sse_ma_khach' not in idx:
            for alt in ['Mã KH', 'Mã khách hàng']:
                if alt in header_row:
                    idx['sse_ma_khach'] = header_row.index(alt); break
        if 'sse_ma_khach' not in idx:
            raise ValueError("Không tìm thấy cột 'Mã khách' trong XML sản lượng.")

        data = []
        for row in all_rows[header_row_index + 2:]:
            if any(row):
                rec = {k: (row[i] if i < len(row) else None) for k, i in idx.items()}
                data.append(rec)

        sse_df = pd.DataFrame(data)
        sse_df['sse_ma_khach'] = sse_df['sse_ma_khach'].astype(str).str.strip()
        sse_df = sse_df[sse_df['sse_ma_khach'].notna() & (sse_df['sse_ma_khach'] != '') & (sse_df['sse_ma_khach'] != 'None')].copy()
        for p in list(colmap.keys())[2:]:
            if p in sse_df.columns:
                sse_df[p] = pd.to_numeric(sse_df[p], errors='coerce').fillna(0).round(0)
        return sse_df
    except Exception as e:
        print(f"Lỗi nghiêm trọng khi đọc file XML sản lượng: {e}")
        logger.exception("Lỗi nghiêm trọng khi đọc file XML sản lượng")
        return None

def reconcile_product_data(pos_df, sse_df):
    for product in config.TARGET_PRODUCTS_BH03:
        if product in pos_df.columns:
            pos_df[product] = clean_and_convert_to_numeric(pos_df[product])

    results = []
    all_pos_chxd_names = set(config.STORE_INFO.values())
    processed = set()
    sse2pos = {sse: config.STORE_INFO.get(pos) for sse, pos in config.STORE_MAPPING_SSE_TO_POS.items() if config.STORE_INFO.get(pos)}

    for _, r in sse_df.iterrows():
        pos_name = sse2pos.get(r['sse_ma_khach'])
        if not pos_name:
            continue
        processed.add(pos_name)
        prows = pos_df[pos_df['Tên CHXD'] == pos_name]
        if prows.empty:
            for p in config.TARGET_PRODUCTS_BH03:
                if r.get(p, 0) != 0:
                    results.append({"chxd_name": f"{pos_name} (Không có trên POS)", "product_name": p, "pos_value": "N/A", "sse_value": float(r.get(p,0)), "is_match": False})
            continue
        for p in config.TARGET_PRODUCTS_BH03:
            pv = prows.iloc[0].get(p, 0); sv = r.get(p, 0)
            ok = int(pv) == int(sv)
            if not ok or pv != 0 or sv != 0:
                results.append({"chxd_name": pos_name, "product_name": p, "pos_value": float(pv), "sse_value": float(sv), "is_match": ok})

    for ch in (all_pos_chxd_names - processed):
        prows = pos_df[pos_df['Tên CHXD'] == ch]
        if not prows.empty:
            for p in config.TARGET_PRODUCTS_BH03:
                pv = prows.iloc[0].get(p, 0)
                if pv != 0:
                    results.append({"chxd_name": f"{ch} (Không có trên file KT)", "product_name": p, "pos_value": float(pv), "sse_value": "N/A", "is_match": False})
    return results

# ==============================================================================
# TIỀN MẶT
# ==============================================================================

def read_sse_cash_xml(file_stream, reconcile_date: datetime):
    try:
        xml_bytes = file_stream.read()
        try:
            xml_content = xml_bytes.decode('utf-8')
        except UnicodeDecodeError:
            xml_content = xml_bytes.decode('windows-1252')
        xml_content = xml_content.replace('xmlns="urn:schemas-microsoft-com:office:spreadsheet"', '')
        root = ET.fromstring(xml_content)

        all_rows = [[cell.findtext("Data") for cell in row.findall("Cell")]
                    for row in root.findall(".//Table/Row")]

        header_row_index = find_header_row_index(all_rows)
        if header_row_index == -1 or len(all_rows) < header_row_index + 2:
            raise ValueError("XML tiền mặt không hợp lệ (không thấy tiêu đề).")

        header = all_rows[header_row_index]
        date_dm = reconcile_date.strftime('%d/%m')
        col_cash = f"Bán - {date_dm}"
        if col_cash not in header or 'Mã ĐV' not in header:
            raise ValueError(f"Thiếu cột '{col_cash}' hoặc 'Mã ĐV' trong XML tiền mặt.")

        ix_cash = header.index(col_cash); ix_code = header.index('Mã ĐV')
        data = []
        for row in all_rows[header_row_index + 2:]:
            if len(row) > max(ix_cash, ix_code):
                code = row[ix_code]; val = row[ix_cash]
                if code and str(code).strip():
                    data.append({'sse_ma_dv': str(code).strip(), 'sse_tien_mat': val})

        sse_df = pd.DataFrame(data)
        sse_df['sse_tien_mat'] = pd.to_numeric(sse_df['sse_tien_mat'], errors='coerce').fillna(0).round(0)
        return sse_df
    except Exception as e:
        print(f"Lỗi nghiêm trọng khi đọc file XML tiền mặt: {e}")
        logger.exception("Lỗi nghiêm trọng khi đọc file XML tiền mặt")
        return None

def reconcile_cash_data(pos_df, sse_df):
    if 'Tiền mặt' in pos_df.columns:
        pos_df['Tiền mặt'] = clean_and_convert_to_numeric(pos_df['Tiền mặt'])
    else:
        pos_df['Tiền mặt'] = 0

    results = []
    all_pos_names = set(config.STORE_INFO.values()); processed = set()
    sse2pos = {sse: config.STORE_INFO.get(pos) for sse,pos in config.STORE_MAPPING_CASH_SSE_TO_POS.items() if config.STORE_INFO.get(pos)}

    for _, r in sse_df.iterrows():
        pos_name = sse2pos.get(r['sse_ma_dv'])
        if not pos_name: continue
        processed.add(pos_name)
        prow = pos_df[pos_df['Tên CHXD'] == pos_name]
        sse_cash = r.get('sse_tien_mat', 0); pos_cash = 0
        if prow.empty:
            if sse_cash != 0:
                results.append({"chxd_name": f"{pos_name} (Không có trên POS)","product_name":"Tiền mặt","pos_value":"N/A","sse_value":float(sse_cash),"is_match":False})
            continue
        pos_cash = prow.iloc[0].get('Tiền mặt', 0)
        ok = int(pos_cash) == int(sse_cash)
        if not ok or pos_cash != 0 or sse_cash != 0:
            results.append({"chxd_name": pos_name,"product_name":"Tiền mặt","pos_value":float(pos_cash),"sse_value":float(sse_cash),"is_match":ok})

    for ch in (all_pos_names - processed):
        prow = pos_df[pos_df['Tên CHXD'] == ch]
        if not prow.empty:
            pos_cash = prow.iloc[0].get('Tiền mặt', 0)
            if pos_cash != 0:
                results.append({"chxd_name": f"{ch} (Không có trên file KT)","product_name":"Tiền mặt","pos_value":float(pos_cash),"sse_value":"N/A","is_match":False})
    return results

# ==============================================================================
# CÔNG NỢ
# ==============================================================================

def read_sse_debt_xml(file_stream):
    """
    Đọc file XML 'Sổ đối chiếu công nợ' từ SSE → DataFrame:
      store_display, store_key, sse_ma_khach, sse_ten_khach, sse_phat_sinh_no
    """
    try:
        xml_bytes = file_stream.read()
        try:
            xml_content = xml_bytes.decode('utf-8')
        except UnicodeDecodeError:
            xml_content = xml_bytes.decode('windows-1252')
        xml_content = xml_content.replace('xmlns="urn:schemas-microsoft-com:office:spreadsheet"', '')
        root = ET.fromstring(xml_content)

        all_rows = [[cell.findtext("Data") for cell in row.findall("Cell")]
                    for row in root.findall(".//Table/Row")]

        hidx = find_header_row_index(all_rows)
        if hidx == -1 or len(all_rows) < hidx + 2:
            raise ValueError("XML công nợ không hợp lệ (không thấy tiêu đề).")

        header = [str(h) if h is not None else '' for h in all_rows[hidx]]
        # vị trí cột cần thiết
        idx_stt  = header.index('STT') if 'STT' in header else 0
        idx_code = header.index('Mã khách') if 'Mã khách' in header else header.index('Mã KH')
        idx_name = header.index('Tên khách')
        idx_psno = header.index('Phát sinh nợ') if 'Phát sinh nợ' in header else header.index('PS nợ')

        records = []
        cur_store_code = None
        cur_store_name_disp = None
        cur_store_key = None

        for row in all_rows[hidx + 2:]:
            row = list(row) if row is not None else []
            while len(row) < len(header):
                row.append(None)

            colA = (row[0] or '').strip()
            colB = (row[1] or '').strip()
            colC = (row[2] or '').strip()

            if (colA == '') and colB and colC and _norm_key(colC).startswith('chxd'):
                cur_store_code = colB
                cur_store_name_disp = _canon_store_display(colC)
                cur_store_key = _canon_store_key(colC)
                continue

            if not colA or not colA.replace('.', '').isdigit() or cur_store_key is None:
                continue

            sse_code = (row[idx_code] or '').strip()
            sse_name = (row[idx_name] or '').strip()
            ps_raw   = row[idx_psno]

            is_same_code = _codes_equal(sse_code, cur_store_code)
            name_key = _canon_store_key(sse_name)
            is_same_name = (name_key == cur_store_key) or name_key.startswith(cur_store_key) or cur_store_key.startswith(name_key)
            if is_same_code or is_same_name:
                continue

            nm = _norm_key(sse_name)
            if nm in {'khach hang chung','cong no chung','khach hang chung (cong ty)','cong no chung (cong ty)'}:
                continue

            try:
                psv = float(ps_raw) if ps_raw not in (None, '') else 0.0
            except Exception:
                psv = pd.to_numeric(str(ps_raw).replace('.', '').replace(',', '.'), errors='coerce')
                psv = 0.0 if pd.isna(psv) else float(psv)

            records.append({
                'store_display': cur_store_name_disp,         # hiển thị
                'store_key': cur_store_key,                   # ghép
                'sse_ma_khach': sse_code,
                'sse_ten_khach': sse_name,
                'sse_phat_sinh_no': round(psv)
            })

        if not records:
            raise ValueError("Không trích xuất được dòng dữ liệu công nợ nào từ XML.")

        df = pd.DataFrame(records)
        df['sse_ma_khach'] = df['sse_ma_khach'].fillna('').astype(str).str.strip()
        df['sse_ten_khach'] = df['sse_ten_khach'].fillna('').astype(str).str.strip()
        df['sse_phat_sinh_no'] = pd.to_numeric(df['sse_phat_sinh_no'], errors='coerce').fillna(0).round(0)
        return df

    except Exception as e:
        print(f"Lỗi nghiêm trọng khi đọc file XML công nợ: {e}")
        logger.exception("Lỗi nghiêm trọng khi đọc file XML công nợ")
        return None

def _pos_expand_store_from_tonghop(pos_df: pd.DataFrame) -> pd.DataFrame:
    cols_needed = ['Tên Khách hàng', 'Mã khách hàng', 'Phát sinh nợ']
    for c in cols_needed:
        if c not in pos_df.columns:
            raise KeyError(f"Thiếu cột bắt buộc trong POS: '{c}'")
    records = []
    current_store = None
    for _, row in pos_df.iterrows():
        ten_kh = str(row['Tên Khách hàng']).strip()
        if _norm_key(ten_kh).startswith('chxd'):
            current_store = ten_kh
            continue
        if not current_store:
            continue
        records.append({
            'Cửa hàng': current_store,
            'Tên Khách hàng': ten_kh,
            'Mã khách hàng': ('' if pd.isna(row['Mã khách hàng']) else str(row['Mã khách hàng']).strip()),
            'Phát sinh nợ': row['Phát sinh nợ']
        })
    df = pd.DataFrame(records) if records else pd.DataFrame(columns=['Cửa hàng','Tên Khách hàng','Mã khách hàng','Phát sinh nợ'])
    return df

def reconcile_debt_data(pos_df: pd.DataFrame, sse_df: pd.DataFrame):
    try:
        print(">>> DEBUG[Debt]: Raw POS columns:", list(pos_df.columns))
        pos = _pos_expand_store_from_tonghop(pos_df)
    except Exception as e:
        print(">>> ERROR[Debt]: Không thể dựng POS chuẩn từ TongHopCongNo:", e)
        logger.exception("Debt: Failed to expand POS TongHopCongNo")
        raise

    if pos.empty:
        print(">>> WARNING[Debt]: POS (sau parse) rỗng – có thể sheet TongHopCongNo không đúng cấu trúc.")
        logger.warning("Debt: POS parsed is empty")

    pos['Cửa hàng'] = pos['Cửa hàng'].astype(str).map(_canon_store_display)
    pos['store_key'] = pos['Cửa hàng'].map(_canon_store_key)
    pos['Mã khách hàng'] = pos['Mã khách hàng'].fillna('').astype(str).str.strip()
    pos['Phát sinh nợ'] = clean_and_convert_to_numeric(pos['Phát sinh nợ'])
    pos = pos[~pos['Tên Khách hàng'].astype(str).str.strip().str.lower().isin(
        ['khách hàng chung','khach hang chung','công nợ chung','cong no chung']
    )].copy()

    print(">>> DEBUG[Debt]: SSE columns:", list(sse_df.columns))
    sse = sse_df.copy()
    sse['store_display'] = sse['store_display'].astype(str)
    sse['store_key'] = sse['store_key'].astype(str)
    sse['sse_ma_khach'] = sse['sse_ma_khach'].fillna('').astype(str).str.strip()
    sse['sse_ten_khach'] = sse['sse_ten_khach'].fillna('').astype(str).str.strip()
    sse['sse_phat_sinh_no'] = pd.to_numeric(sse['sse_phat_sinh_no'], errors='coerce').fillna(0).round(0)

    results = []

    all_keys = sorted(set(pos['store_key']).union(set(sse['store_key'])))
    print(">>> DEBUG[Debt]: Tổng số store_key để so khớp:", len(all_keys))

    for skey in all_keys:
        pos_store = pos[pos['store_key'] == skey]
        sse_store = sse[sse['store_key'] == skey]

        if pos_store.empty and sse_store.empty:
            continue

        display_name = pos_store['Cửa hàng'].iloc[0] if not pos_store.empty else sse_store['store_display'].iloc[0]

        # 1) Ghép theo MÃ KH
        pos_by_code = (pos_store[pos_store['Mã khách hàng'] != '']
                       .groupby(['Mã khách hàng','Tên Khách hàng'], as_index=False)['Phát sinh nợ'].sum())
        sse_by_code = (sse_store[sse_store['sse_ma_khach'] != '']
                       .groupby(['sse_ma_khach','sse_ten_khach'], as_index=False)['sse_phat_sinh_no'].sum())

        pos_code_map = {_norm_code(r['Mã khách hàng']):(r['Tên Khách hàng'], float(r['Phát sinh nợ'])) for _,r in pos_by_code.iterrows()}
        sse_code_map = {_norm_code(r['sse_ma_khach']):(r['sse_ten_khach'], float(r['sse_phat_sinh_no'])) for _,r in sse_by_code.iterrows()}

        codes = set(pos_code_map.keys()).union(set(sse_code_map.keys()))
        matched_pos_names = set(); matched_sse_names = set()

        for c in sorted(codes):
            pn, pv = pos_code_map.get(c, (None, 0.0))
            sn, sv = sse_code_map.get(c, (None, 0.0))
            cname = pn or sn or ''
            ok = int(round(pv)) == int(round(sv))
            status = ''
            if c not in pos_code_map:
                status = 'Có trên file KT, thiếu trên POS'
            elif c not in sse_code_map:
                status = 'Có trên POS, thiếu trên file KT'
            if (not ok) or (pv != 0) or (sv != 0):
                results.append({
                    'chxd_name': display_name,
                    'customer_code': c,
                    'customer_name': cname,
                    'pos_value': float(round(pv)),
                    'sse_value': float(round(sv)),
                    'is_match': ok,
                    'status': status
                })
            if pn: matched_pos_names.add(_norm_key(pn))
            if sn: matched_sse_names.add(_norm_key(sn))

        # 2) Ghép theo TÊN
        pos_no_code = pos_store[(pos_store['Mã khách hàng'] == '') | (pos_store['Mã khách hàng'].str.lower() == 'không tìm thấy mã khách')] \
                                .groupby('Tên Khách hàng', as_index=False)['Phát sinh nợ'].sum()
        sse_no_code = sse_store[sse_store['sse_ma_khach'] == ''] \
                                .groupby('sse_ten_khach', as_index=False)['sse_phat_sinh_no'].sum()

        pos_name_map = {_norm_key(r['Tên Khách hàng']):(r['Tên Khách hàng'], float(r['Phát sinh nợ'])) for _,r in pos_no_code.iterrows()}
        sse_name_map = {_norm_key(r['sse_ten_khach']):(r['sse_ten_khach'], float(r['sse_phat_sinh_no'])) for _,r in sse_no_code.iterrows()}

        names = set(pos_name_map.keys()).union(set(sse_name_map.keys()))
        for nk in sorted(names):
            if nk in matched_pos_names or nk in matched_sse_names:
                continue
            pn, pv = pos_name_map.get(nk, ('', 0.0))
            sn, sv = sse_name_map.get(nk, ('', 0.0))
            disp = pn or sn
            ok = int(round(pv)) == int(round(sv))
            status = ''
            if nk not in pos_name_map:
                status = 'Có trên file KT, thiếu trên POS (không mã)'
            elif nk not in sse_name_map:
                status = 'Có trên POS, thiếu trên file KT (không mã)'
            if (not ok) or (pv != 0) or (sv != 0):
                results.append({
                    'chxd_name': display_name,
                    'customer_code': '',
                    'customer_name': disp,
                    'pos_value': float(round(pv)),
                    'sse_value': float(round(sv)),
                    'is_match': ok,
                    'status': status
                })

    return results

# ==============================================================================
# PHẦN MỚI BỔ SUNG: ĐỐI SOÁT HÓA ĐƠN (HD01) VỚI BIGQUERY
# ==============================================================================

def _vn_normalize(s: str) -> str:
    """Chuẩn hóa chuỗi (Bỏ dấu, in thường, xóa khoảng trắng thừa)."""
    if pd.isna(s) or s is None:
        return ""
    s = str(s).strip().lower()
    
    # Xử lý ký tự 'đ' và 'Đ' của tiếng Việt trước khi dùng unicodedata
    s = s.replace('đ', 'd').replace('Đ', 'd')
    
    s = "".join(ch for ch in unicodedata.normalize("NFD", s) if unicodedata.category(ch) != "Mn")
    return re.sub(r"[\s\._\-]+", " ", s).strip()

def read_tax_excel_file(file_stream, target_month=None, target_year=None, progress_callback=None):
    """
    CHIẾN LƯỢC TỐI ƯU RAM: CHỈ ĐỌC CÁC CỘT CẦN THIẾT TỪ FILE 50MB DƯỚI DẠNG TEXT (dtype=str).
    Sử dụng Thuật toán Định vị Không va chạm + Blacklist để loại bỏ cột Người Bán.
    """
    try:
        if progress_callback: progress_callback(".... Đang tìm kiếm các cột mục tiêu.....")
        
        # BƯỚC 1: Đọc nhanh 30 dòng đầu để dò tìm Header
        df_preview = pd.read_excel(file_stream, header=None, nrows=30)
        header_idx = -1
        
        for i in range(len(df_preview)):
            row_vals = [_vn_normalize(str(x)) for x in df_preview.iloc[i].values if pd.notna(x)]
            if 'stt' in row_vals or 'so tt' in row_vals or 'so thu tu' in row_vals:
                header_idx = i
                break
                
        if header_idx == -1:
            raise ValueError("Không nhận diện được cấu trúc Bảng kê Thuế (Không tìm thấy dòng tiêu đề chứa 'STT').")

        header_row = [_vn_normalize(str(x)) for x in df_preview.iloc[header_idx].values]
        
        # TỪ KHÓA MỞ RỘNG
        kw_mapping = {
            'KyHieu': ['ky hieu hoa don', 'ky hieu'], 
            'SoHD': ['so hoa don', 'so hd'],
            'KhachHang': ['ten nguoi mua', 'ten nguoi mua (trong nuoc)', 'ten don vi', 'ten khach hang', 'nguoi mua', 'khach hang'],
            'MST': ['ma so thue nguoi mua', 'mst nguoi mua', 'ma so thue', 'mst'],
            'TienChuaThue': ['tong tien chua thue', 'tien chua thue', 'doanh thu chua thue', 'cong tien hang', 'tien hang'],
            'TienThue': ['tong tien thue', 'tien thue gtgt', 'thue gtgt', 'tien thue'],
            'TongTien': ['tong tien thanh toan', 'tong tien', 'tong cong', 'thanh toan'],
            'NgayLap': ['ngay lap', 'ngay hoa don', 'ngay thang nam lap', 'thoi gian']
        }
        
        # FIX 1: TỪ KHÓA BLACKLIST (Loại bỏ triệt để các cột của Người Bán)
        blacklist = ['nguoi ban', 'xuat hang']
        
        # BƯỚC 2: Định vị Index Không Va Chạm
        col_indices = {}
        used_indices = set()
        
        for col_key, keywords in kw_mapping.items():
            # Ưu tiên 1: Quét tìm khớp chính xác 100%
            for idx, col_name in enumerate(header_row):
                if idx in used_indices: continue
                # Chống nhiễu lấy nhầm cột của Người Bán
                if (col_key in ['MST', 'KhachHang']) and any(b in col_name for b in blacklist):
                    continue
                    
                if col_name in keywords:
                    col_indices[col_key] = idx
                    used_indices.add(idx)
                    break
            
            # Ưu tiên 2: Quét tìm chứa từ khóa
            if col_key not in col_indices:
                for idx, col_name in enumerate(header_row):
                    if idx in used_indices: continue
                    # Chống nhiễu lấy nhầm cột của Người Bán
                    if (col_key in ['MST', 'KhachHang']) and any(b in col_name for b in blacklist):
                        continue
                        
                    if any(kw in col_name for kw in keywords):
                        if col_key == 'KyHieu' and 'mau so' in col_name: continue
                        if col_key == 'SoHD' and ('thue' in col_name or 'ma so' in col_name): continue
                        if col_key == 'KhachHang' and ('mst' in col_name or 'ma so' in col_name): continue # Ngăn Khách Hàng cướp nhầm cột của MST
                        
                        col_indices[col_key] = idx
                        used_indices.add(idx)
                        break

        missing_cols = [k for k in kw_mapping.keys() if k not in col_indices]
        if missing_cols:
            print("====== BÁO CÁO LỖI ĐỌC FILE BẢNG KÊ THUẾ ======")
            print(f"CÁC CỘT TÌM THẤY TRONG FILE: {header_row}")
            print(f"CÁC THUỘC TÍNH BỊ THIẾU: {missing_cols}")
            print("===============================================")
            raise ValueError(f"Không tìm thấy cột tương ứng cho {', '.join(missing_cols)} trong Bảng kê Thuế.")

        sorted_indices = sorted(list(col_indices.values()))

        if progress_callback: progress_callback(".... Đang đọc và hút dữ liệu từ bảng kê của cơ quan thuế....")

        # BƯỚC 3: Đọc file lần 2 (FIX 2: Thêm dtype=str để không bao giờ bị mất số 0 ở đầu)
        file_stream.seek(0)
        tax_df = pd.read_excel(file_stream, header=header_idx, usecols=sorted_indices, dtype=str)
        
        # BƯỚC 4: Gán lại tên cột chuẩn xác 100%
        new_col_names = []
        for original_idx in sorted_indices:
            found_key = next(k for k, v in col_indices.items() if v == original_idx)
            new_col_names.append(found_key)
            
        tax_df.columns = new_col_names

        # Kiểm tra tính toàn vẹn của Thời gian (Sử dụng dayfirst=True để an toàn với định dạng ngày VN)
        if 'NgayLap' in tax_df.columns:
            tax_df['NgayLap'] = pd.to_datetime(tax_df['NgayLap'], errors='coerce', dayfirst=True)
            tax_df = tax_df.dropna(subset=['NgayLap']) 
            unique_months = tax_df['NgayLap'].dt.to_period('M').unique()
            if len(unique_months) > 1:
                raise ValueError(f"Bảng kê thuế chứa dữ liệu của nhiều tháng khác nhau ({', '.join([str(m) for m in unique_months])}).")
            
            if target_month and target_year and len(unique_months) == 1:
                file_m = unique_months[0].month
                file_y = unique_months[0].year
                if str(file_m) != str(target_month) or str(file_y) != str(target_year):
                    raise ValueError(f"Bảng kê thuế bạn tải lên là của tháng {file_m}/{file_y}, không khớp với lựa chọn ({target_month}/{target_year}).")

        # Chuẩn hóa kiểu dữ liệu trước khi đẩy lên mây
        for c in ['TienChuaThue', 'TienThue', 'TongTien']:
            if c in tax_df.columns:
                tax_df[c] = pd.to_numeric(tax_df[c].astype(str).str.replace(',', '').str.replace(' ', ''), errors='coerce').fillna(0.0)
                
        for c in ['KyHieu', 'SoHD', 'KhachHang', 'MST']:
            if c in tax_df.columns: tax_df[c] = tax_df[c].astype(str).fillna('')

        if progress_callback: progress_callback("..... Đang tạo Chứng minh thư Hóa đơn (HD_ID) .....")

        # Xóa các hậu tố ".0" (nếu có do Excel sinh ra) và tạo ID
        tx_sohd_str = tax_df['SoHD'].str.replace(r'\.0$', '', regex=True)
        tax_df['HD_ID'] = tax_df['KyHieu'].str.strip() + "_" + tx_sohd_str.str.replace("'", "").str.strip().str.lstrip('0')
        tax_df = tax_df[~tax_df['HD_ID'].str.contains('nan', case=False, na=False)]

        return tax_df

    except Exception as e:
        raise ValueError(f"Lỗi đọc file Thuế: {str(e)}")

def reconcile_invoice_data_bq(report_month, report_year, tax_df, progress_callback=None):
    """
    THUẬT TOÁN ĐỐI SOÁT ĐÁM MÂY (FULL OUTER JOIN TRÊN BIGQUERY)
    """
    import bq_handler
    
    if tax_df is None or tax_df.empty:
        return []

    client = bq_handler.get_bq_client()
    
    # 1. Tạo Tên Bảng Tạm ngẫu nhiên
    temp_table_id = f"{client.project}.pvoil_data.Temp_Tax_Data_{uuid.uuid4().hex[:8]}"
    
    try:
        if progress_callback: progress_callback("..... Đang tải dữ liệu bảng kê thuế lên BigQuery.....")
        
        # 2. Bơm file Thuế (đã rút gọn chỉ còn vài MB) lên BigQuery
        job_config = bq_handler.bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        job = client.load_table_from_dataframe(tax_df, temp_table_id, job_config=job_config)
        job.result() # Đợi upload xong

        if progress_callback: progress_callback("...... Đang chờ kết quả đối soát từ BigQuery (So khớp chéo)......")
        
        # 3. Kích hoạt Lệnh SQL So Khớp Chéo
        sql_query = f"""
        WITH PVOIL_Data AS (
            SELECT 
                Ten_CHXD,
                Ky_Hieu AS KyHieu,
                So_HD AS SoHD,
                Ten_Khach_Hang AS KhachHang,
                Ma_So_Thue AS MST,
                Tien_Chua_Thue AS TienChuaThue,
                Tien_Thue AS TienThue,
                Tong_Tien AS TongTien,
                CONCAT(TRIM(Ky_Hieu), '_', LTRIM(TRIM(So_HD), '0')) AS HD_ID
            FROM `{client.project}.pvoil_data.HD01_Master_Data`
            WHERE Thang_Bao_Cao = {int(report_month)} AND Nam_Bao_Cao = {int(report_year)}
            QUALIFY ROW_NUMBER() OVER(PARTITION BY Ma_CHXD, Ky_Hieu, So_HD ORDER BY Ngay_Hoa_Don DESC) = 1
        ),
        TAX_Data AS (
            SELECT 
                KyHieu,
                SoHD,
                KhachHang,
                MST,
                TienChuaThue,
                TienThue,
                TongTien,
                HD_ID
            FROM `{temp_table_id}`
        )
        SELECT 
            COALESCE(p.Ten_CHXD, 'Bảng kê Thuế') AS chxd_name,
            COALESCE(p.HD_ID, t.HD_ID) AS invoice_id,
            p.TongTien AS pos_value,
            t.TongTien AS sse_value,
            p.KyHieu AS KyHieu_pv, t.KyHieu AS KyHieu_tx,
            p.KhachHang AS KhachHang_pv, t.KhachHang AS KhachHang_tx,
            p.MST AS MST_pv, t.MST AS MST_tx,
            p.TienChuaThue AS TienChuaThue_pv, t.TienChuaThue AS TienChuaThue_tx,
            p.TienThue AS TienThue_pv, t.TienThue AS TienThue_tx,
            p.TongTien AS TongTien_pv, t.TongTien AS TongTien_tx
        FROM PVOIL_Data p
        FULL OUTER JOIN TAX_Data t ON p.HD_ID = t.HD_ID
        WHERE 
            p.HD_ID IS NULL -- Thiếu trên PVOIL
            OR t.HD_ID IS NULL -- Thiếu trên Thuế
            OR ABS(COALESCE(p.TongTien, 0) - COALESCE(t.TongTien, 0)) >= 1.0
            OR ABS(COALESCE(p.TienChuaThue, 0) - COALESCE(t.TienChuaThue, 0)) >= 1.0
            OR ABS(COALESCE(p.TienThue, 0) - COALESCE(t.TienThue, 0)) >= 1.0
            OR REPLACE(LOWER(IFNULL(p.MST, '')), ' ', '') != REPLACE(LOWER(IFNULL(t.MST, '')), ' ', '')
            OR LOWER(TRIM(IFNULL(p.KhachHang, ''))) != LOWER(TRIM(IFNULL(t.KhachHang, '')))
        """

        mismatched_df = client.query(sql_query).to_dataframe()
        
        if progress_callback: progress_callback("...... Đang nhận kết quả từ BigQuery.........")
        
    finally:
        # 5. DỌN DẸP CHIẾN TRƯỜNG
        client.delete_table(temp_table_id, not_found_ok=True)

    # 6. XỬ LÝ KẾT QUẢ
    results = []
    for _, row in mismatched_df.iterrows():
        pv_val_tong = row['pos_value'] if pd.notna(row.get('pos_value')) else 0.0
        tx_val_tong = row['sse_value'] if pd.notna(row.get('sse_value')) else 0.0

        status_msgs = []
        is_missing = False
        if pd.isna(row.get('KyHieu_pv')):
            status_msgs.append("❗ Chỉ có trên Bảng kê Thuế (Không có trên bảng kê POS)")
            is_missing = True
        elif pd.isna(row.get('KyHieu_tx')):
            status_msgs.append("❗ Chỉ có trên Bảng kê POS (Không có trên Thuế)")
            is_missing = True
        else:
            name_pv = _vn_normalize(str(row.get('KhachHang_pv', '')))
            name_tx = _vn_normalize(str(row.get('KhachHang_tx', '')))
            mst_pv = str(row.get('MST_pv', '')).replace('nan', '').replace(' ', '').lower()
            mst_tx = str(row.get('MST_tx', '')).replace('nan', '').replace(' ', '').lower()
            
            is_money_diff = False
            if abs(row.get('TienChuaThue_pv', 0) - row.get('TienChuaThue_tx', 0)) >= 1.0:
                status_msgs.append("Lệch Tiền Chưa Thuế")
                is_money_diff = True
            if abs(row.get('TienThue_pv', 0) - row.get('TienThue_tx', 0)) >= 1.0:
                status_msgs.append("Lệch Tiền Thuế")
                is_money_diff = True
            if abs(row.get('TongTien_pv', 0) - row.get('TongTien_tx', 0)) >= 1.0:
                status_msgs.append(f"Lệch Tổng Tiền (PV: {pv_val_tong:,.0f} vs Thuế: {tx_val_tong:,.0f})")
                is_money_diff = True
                
            is_mst_diff = (mst_pv != mst_tx)
            if is_mst_diff:
                status_msgs.append(f"Khác MST (PV: '{row.get('MST_pv', '')}' vs Thuế: '{row.get('MST_tx', '')}')")
                
            if name_pv != name_tx:
                status_msgs.append(f"Khác Tên KH (PV: '{row.get('KhachHang_pv', '')}' vs Thuế: '{row.get('KhachHang_tx', '')}')")
            elif not is_money_diff and not is_mst_diff:
                continue 

        results.append({
            'chxd_name': row['chxd_name'],
            'invoice_id': str(row['invoice_id']).replace('_', ' '),
            'pos_value': pv_val_tong,
            'sse_value': tx_val_tong,
            'is_match': False,
            'status': " - ".join(status_msgs)
        })

    if not results:
        results.append({
            'chxd_name': 'TẤT CẢ CỬA HÀNG',
            'invoice_id': 'N/A',
            'pos_value': 0,
            'sse_value': 0,
            'is_match': True,
            'status': '✅ TUYỆT VỜI! Toàn bộ hóa đơn khớp 100%. Không có bất thường nào.'
        })

    results.sort(key=lambda x: (x['is_match'], x['chxd_name']))
    return results