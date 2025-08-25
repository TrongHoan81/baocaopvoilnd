# -*- coding: utf-8 -*-
import pandas as pd
import config
import io
import xml.etree.ElementTree as ET
import re

def find_date_in_headers(header_list):
    """Tìm kiếm và trích xuất chuỗi ngày (ví dụ: 'Ngày 22/08') từ danh sách tiêu đề."""
    for header in header_list:
        if header:
            # Sử dụng regex để tìm chuỗi có dạng "Ngày dd/mm"
            match = re.search(r'Ngày \d{2}/\d{2}', header)
            if match:
                return match.group(0)
    return None

def read_sse_xml(file_stream):
    """
    Đọc file XML từ phần mềm kế toán (SSE) và chuyển thành DataFrame.
    """
    try:
        # 1. Đọc và giải mã file
        xml_bytes = file_stream.read()
        try:
            xml_content = xml_bytes.decode('utf-8')
        except UnicodeDecodeError:
            xml_content = xml_bytes.decode('windows-1252')
            
        xml_content = xml_content.replace('xmlns="urn:schemas-microsoft-com:office:spreadsheet"', '')
        root = ET.fromstring(xml_content)
        
        # 2. Trích xuất tất cả dữ liệu thô từ các ô
        all_rows = []
        for row in root.findall(".//Table/Row"):
            cells = [cell.findtext("Data") for cell in row.findall("Cell")]
            all_rows.append(cells)

        if len(all_rows) < 7:
            raise ValueError("File XML không có đủ số dòng dữ liệu cần thiết.")

        # 3. Tự động xác định các cột cần lấy dữ liệu
        header_row = all_rows[4] 
        date_str = find_date_in_headers(header_row)
        if not date_str:
            raise ValueError("Không thể xác định được ngày báo cáo từ tiêu đề file XML.")

        column_mapping_config = {
            'sse_ma_khach': 'Mã khách',
            'sse_ten_khach': 'Tên khách',
            'Dầu Điêzen 0,001S Mức 5': f'{date_str} Dầu DO 0,001S-V',
            'Dầu mỡ nhờn': f'{date_str} Dầu mỡ nhờn',
            'Dầu Điêzen 0,05S Mức 2': f'{date_str} DO',
            'Xăng RON95 Mức 3': f'{date_str} Xăng A95',
            'Xăng E5 RON92 Mức 2': f'{date_str} Xăng E5'
        }

        column_indices = {}
        for standard_name, xml_header in column_mapping_config.items():
            try:
                column_indices[standard_name] = header_row.index(xml_header)
            except ValueError:
                raise ValueError(f"Không tìm thấy cột '{xml_header}' trong file XML.")

        # 4. Lọc và xây dựng lại dữ liệu
        extracted_data = []
        for row in all_rows[6:]:
            if len(row) > max(column_indices.values()):
                record = {
                    standard_name: row[index]
                    for standard_name, index in column_indices.items()
                }
                extracted_data.append(record)
        
        if not extracted_data:
            raise ValueError("Không trích xuất được dòng dữ liệu nào từ file.")

        # 5. Chuyển thành DataFrame và làm sạch
        sse_df = pd.DataFrame(extracted_data)
        sse_df['sse_ma_khach'] = sse_df['sse_ma_khach'].astype(str).str.strip()
        sse_df = sse_df[sse_df['sse_ma_khach'].notna() & (sse_df['sse_ma_khach'] != '') & (sse_df['sse_ma_khach'] != 'None')].copy()

        product_columns = list(column_mapping_config.keys())[2:]
        for col in product_columns:
            # Chuyển sang số, các giá trị lỗi sẽ thành NaN
            sse_df[col] = pd.to_numeric(sse_df[col], errors='coerce')
        
        # Điền 0 vào các ô rỗng và làm tròn đến hàng đơn vị
        sse_df = sse_df.fillna(0)
        for col in product_columns:
            sse_df[col] = sse_df[col].round(0)
        
        return sse_df

    except Exception as e:
        print(f"Lỗi nghiêm trọng khi đọc file SSE XML: {e}")
        return None

def reconcile_data(pos_df, sse_df):
    """
    Thực hiện đối soát giữa dữ liệu POS và SSE.
    """
    # === SỬA LỖI: Chuẩn hóa định dạng số và làm tròn trong DataFrame từ POS ===
    for product in config.TARGET_PRODUCTS_BH03:
        if product in pos_df.columns:
            # 1. Xóa dấu '.' (ngăn cách hàng ngàn).
            # 2. Thay thế dấu ',' (thập phân) bằng dấu '.'.
            # 3. Chuyển thành số float, điền 0 cho các giá trị lỗi.
            # 4. Làm tròn đến hàng đơn vị.
            pos_df[product] = pd.to_numeric(
                pos_df[product].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False),
                errors='coerce'
            ).fillna(0).round(0)
    # =============================================================

    results = []
    all_pos_chxd_names = set(config.STORE_INFO.values())
    processed_chxd_from_sse = set()
    
    sse_code_to_pos_name = {
        sse_code: config.STORE_INFO.get(pos_code)
        for sse_code, pos_code in config.STORE_MAPPING_SSE_TO_POS.items()
        if config.STORE_INFO.get(pos_code)
    }

    # 1. Lặp qua dữ liệu từ file Kế toán (SSE)
    for _, sse_row in sse_df.iterrows():
        sse_ma_khach = sse_row['sse_ma_khach']
        pos_chxd_name = sse_code_to_pos_name.get(sse_ma_khach)

        if not pos_chxd_name:
            continue

        processed_chxd_from_sse.add(pos_chxd_name)
        pos_row = pos_df[pos_df['Tên CHXD'] == pos_chxd_name]

        if pos_row.empty:
            for product in config.TARGET_PRODUCTS_BH03:
                sse_quantity = sse_row.get(product, 0)
                if sse_quantity != 0:
                    results.append({
                        "chxd_name": f"{pos_chxd_name} (Không có trên POS)",
                        "product_name": product,
                        "pos_quantity": "N/A",
                        "sse_quantity": float(sse_quantity),
                        "is_match": False
                    })
            continue

        for product in config.TARGET_PRODUCTS_BH03:
            pos_quantity = pos_row.iloc[0].get(product, 0)
            sse_quantity = sse_row.get(product, 0)
            
            # So sánh số nguyên sau khi đã làm tròn
            is_match = int(pos_quantity) == int(sse_quantity)

            if not is_match or pos_quantity != 0 or sse_quantity != 0:
                results.append({
                    "chxd_name": pos_chxd_name,
                    "product_name": product,
                    "pos_quantity": float(pos_quantity),
                    "sse_quantity": float(sse_quantity),
                    "is_match": is_match
                })

    # 2. Tìm các cửa hàng có trên POS nhưng không có trong file SSE
    missing_in_sse = all_pos_chxd_names - processed_chxd_from_sse
    for chxd_name in missing_in_sse:
        pos_row = pos_df[pos_df['Tên CHXD'] == chxd_name]
        if not pos_row.empty:
            for product in config.TARGET_PRODUCTS_BH03:
                pos_quantity = pos_row.iloc[0].get(product, 0)
                if pos_quantity != 0:
                    results.append({
                        "chxd_name": f"{chxd_name} (Không có trên file KT)",
                        "product_name": product,
                        "pos_quantity": float(pos_quantity),
                        "sse_quantity": "N/A",
                        "is_match": False
                    })

    return results
