# -*- coding: utf-8 -*-
import pandas as pd
import config
import xml.etree.ElementTree as ET
import re
from datetime import datetime

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
    # bỏ mọi "(...)" ở cuối
    s = re.sub(r'\s*\(.*?\)\s*$', '', name.strip())
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
# SẢN LƯỢNG (giữ nguyên các hàm cũ)
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
# TIỀN MẶT (giữ nguyên)
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
# CÔNG NỢ (SỬA: đồng nhất tên CHXD; lọc “Loại 2” mạnh; ưu tiên ghép MÃ KH)
# ==============================================================================

def read_sse_debt_xml(file_stream):
    """
    Đọc file XML 'Sổ đối chiếu công nợ' từ SSE → DataFrame:
      store_name, sse_ma_khach, sse_ten_khach, sse_phat_sinh_no
    - Bỏ 4 dòng đầu; dòng 5 tiêu đề; dòng 6 tổng → bỏ; từ dòng 7 xét dữ liệu.
    - Nhận diện header CHXD: cột A rỗng, cột B = mã ĐV, cột C bắt đầu 'CHXD'.
    - Dòng chi tiết: cột A là số thứ tự.
    - Loại bỏ:
        + Dòng “treo CHXD” (Loại 2): mã KH ≈ mã ĐV (O↔0) **hoặc** tên KH ~= tên CHXD (bỏ hậu tố trong ngoặc; so equals/startswith).
        + 'Khách hàng chung' / 'Công nợ chung'.
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
        # Chủ đích: tên cột trong file của bạn LUÔN là 'Tên khách'
        idx_stt  = header.index('STT') if 'STT' in header else 0
        idx_code = header.index('Mã khách') if 'Mã khách' in header else header.index('Mã KH')
        idx_name = header.index('Tên khách')  # bạn xác nhận luôn là 'Tên khách'
        # Phát sinh nợ có thể là 'Phát sinh nợ' hoặc 'PS nợ'
        idx_psno = header.index('Phát sinh nợ') if 'Phát sinh nợ' in header else header.index('PS nợ')

        records = []
        cur_store_code = None
        cur_store_name_disp = None   # để hiển thị
        cur_store_key = None         # để ghép

        for row in all_rows[hidx + 2:]:
            row = list(row) if row is not None else []
            while len(row) < len(header):
                row.append(None)

            colA = (row[0] or '').strip()
            colB = (row[1] or '').strip()
            colC = (row[2] or '').strip()

            # Header CHXD
            if (colA == '') and colB and colC and _norm_key(colC).startswith('chxd'):
                cur_store_code = colB
                cur_store_name_disp = _canon_store_display(colC)
                cur_store_key = _canon_store_key(colC)
                continue

            # Dòng chi tiết
            if not colA or not colA.replace('.', '').isdigit() or cur_store_key is None:
                continue

            sse_code = (row[idx_code] or '').strip()
            sse_name = (row[idx_name] or '').strip()
            ps_raw   = row[idx_psno]
            # Lọc Loại 2: treo vào CHXD
            is_same_code = _codes_equal(sse_code, cur_store_code)
            name_key = _canon_store_key(sse_name)
            is_same_name = (name_key == cur_store_key) or name_key.startswith(cur_store_key) or cur_store_key.startswith(name_key)
            if is_same_code or is_same_name:
                continue

            # Lọc KH chung
            nm = _norm_key(sse_name)
            if nm in {'khach hang chung','cong no chung','khach hang chung (cong ty)','cong no chung (cong ty)'}:
                continue

            try:
                psv = float(ps_raw) if ps_raw not in (None, '') else 0.0
            except Exception:
                psv = pd.to_numeric(str(ps_raw).replace('.', '').replace(',', '.'), errors='coerce')
                psv = 0.0 if pd.isna(psv) else float(psv)

            records.append({
                'store_display': cur_store_name_disp,         # giữ để hiển thị
                'store_key': cur_store_key,                   # để ghép
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
        return None

def reconcile_debt_data(pos_df: pd.DataFrame, sse_df: pd.DataFrame):
    """
    Ghép và đối soát công nợ theo CHXD.
    - Ưu tiên theo MÃ KH; nếu một phía thiếu mã/không hợp lệ → ghép theo TÊN KH.
    """
    # POS: chuẩn hoá
    pos = pos_df.copy()
    pos['Cửa hàng'] = pos['Cửa hàng'].astype(str).fillna('').map(_canon_store_display)
    pos['store_key'] = pos['Cửa hàng'].map(_canon_store_key)
    pos['Mã khách hàng'] = pos['Mã khách hàng'].fillna('').astype(str).str.strip()
    pos['Phát sinh nợ'] = clean_and_convert_to_numeric(pos['Phát sinh nợ'])
    # loại 'khách hàng chung' nếu còn
    pos = pos[~pos['Tên Khách hàng'].astype(str).str.strip().str.lower().isin(
        ['khách hàng chung','khach hang chung','công nợ chung','cong no chung']
    )].copy()

    # SSE: chuẩn hoá
    sse = sse_df.copy()
    sse['store_display'] = sse['store_display'].astype(str)
    sse['store_key'] = sse['store_key'].astype(str)
    sse['sse_ma_khach'] = sse['sse_ma_khach'].fillna('').astype(str).str.strip()
    sse['sse_ten_khach'] = sse['sse_ten_khach'].fillna('').astype(str).str.strip()
    sse['sse_phat_sinh_no'] = pd.to_numeric(sse['sse_phat_sinh_no'], errors='coerce').fillna(0).round(0)

    results = []

    # CHXD hợp của 2 nguồn (theo store_key)
    all_keys = sorted(set(pos['store_key']).union(set(sse['store_key'])))

    for skey in all_keys:
        pos_store = pos[pos['store_key'] == skey]
        sse_store = sse[sse['store_key'] == skey]

        # tên hiển thị: ưu tiên POS nếu có, không thì lấy SSE
        display_name = pos_store['Cửa hàng'].iloc[0] if not pos_store.empty else sse_store['store_display'].iloc[0]

        # ===== 1) GHÉP THEO MÃ =====
        pos_by_code = (pos_store[(pos_store['Mã khách hàng'] != '') & (pos_store['Mã khách hàng'].notna())]
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
                    'customer_code': c,  # đã chuẩn hoá để nhìn dễ
                    'customer_name': cname,
                    'pos_value': float(round(pv)),
                    'sse_value': float(round(sv)),
                    'is_match': ok,
                    'status': status
                })
            if pn: matched_pos_names.add(_norm_key(pn))
            if sn: matched_sse_names.add(_norm_key(sn))

        # ===== 2) GHÉP THEO TÊN (khi thiếu mã) =====
        pos_no_code = pos_store[(pos_store['Mã khách hàng'] == '') | (pos_store['Mã khách hàng'].str.lower() == 'không tìm thấy mã khách')] \
                                .groupby('Tên Khách hàng', as_index=False)['Phát sinh nợ'].sum()
        sse_no_code = sse_store[sse_store['sse_ma_khach'] == ''] \
                                .groupby('sse_ten_khach', as_index=False)['sse_phat_sinh_no'].sum()

        pos_name_map = {_norm_key(r['Tên Khách hàng']):(r['Tên Khách hàng'], float(r['Phát sinh nợ'])) for _,r in pos_no_code.iterrows()}
        sse_name_map = {_norm_key(r['sse_ten_khach']):(r['sse_ten_khach'], float(r['sse_phat_sinh_no'])) for _,r in sse_no_code.iterrows()}

        names = set(pos_name_map.keys()).union(set(sse_name_map.keys()))
        for nk in sorted(names):
            if nk in matched_pos_names or nk in matched_sse_names:  # tránh match trùng do tên đã xuất hiện ở bước theo mã
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
