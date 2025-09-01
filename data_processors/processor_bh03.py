# -*- coding: utf-8 -*-
import pandas as pd
import config

# Load DSKH.xlsx once for mapping customer name to code
dskh_df = pd.read_excel('DSKH.xlsx', sheet_name='DSKH')

def process_and_validate_bh03(df, store_name):
    """
    Xử lý DataFrame báo cáo BH03, bóc tách Sản lượng, Doanh thu, Tiền mặt.
    Trả về dòng tổng hợp nếu hợp lệ, ngược lại trả về None.
    """
    if not isinstance(df, pd.DataFrame) or df.empty:
        return None

    # --- 1. Khởi tạo các biến lưu trữ ---
    summary_data = {prod: 0 for prod in config.TARGET_PRODUCTS_BH03}
    revenue = 0
    cash_payment = 0

    # --- 2. Xử lý bóc tách Sản lượng (Mục IV) ---
    start_index = -1
    for i, row in df.iterrows():
        if str(row.iloc[0]).strip().startswith('IV'):
            start_index = i + 1
            break
    
    if start_index != -1:
        end_index = len(df)
        for i in range(start_index, len(df)):
            if str(df.iloc[i, 0]).strip().startswith(('V.', 'VI.')):
                end_index = i
                break
        
        section_df = df.iloc[start_index:end_index]
        for _, row in section_df.iterrows():
            product_name = row.iloc[1]
            if isinstance(product_name, str):
                cleaned_name = product_name.strip()
                if cleaned_name in summary_data:
                    quantity = pd.to_numeric(row.iloc[6], errors='coerce')
                    if pd.notna(quantity):
                        summary_data[cleaned_name] += quantity

    # --- 3. Xử lý bóc tách Doanh thu và Tiền mặt (Toàn bộ file) ---
    # Duyệt toàn bộ file để tìm các dòng đặc biệt
    for _, row in df.iterrows():
        col_A_val = str(row.iloc[0]).strip()
        col_B_val = str(row.iloc[1]).strip()
        
        # Lấy Doanh thu: Cột B là "Tổng cộng"
        if col_B_val == 'Tổng cộng':
            # Cột Q tương ứng với index 16
            revenue_val = pd.to_numeric(row.iloc[16], errors='coerce')
            if pd.notna(revenue_val):
                revenue = revenue_val

        # Lấy Tiền mặt: Cột A là 'I' và Cột B là 'Xuất bán lẻ'
        if col_A_val == 'I' and col_B_val == 'Xuất bán lẻ':
            # Cột Q tương ứng với index 16
            cash_val = pd.to_numeric(row.iloc[16], errors='coerce')
            if pd.notna(cash_val):
                cash_payment = cash_val

    # --- 4. Tổng hợp và trả về kết quả ---
    total_quantity = sum(summary_data.values())
    
    # Chỉ trả về kết quả nếu có sản lượng hoặc doanh thu để tránh báo cáo rỗng
    if total_quantity > 0 or revenue > 0:
        final_row = {"Tên CHXD": store_name}
        final_row.update(summary_data)
        final_row["Tổng sản lượng"] = total_quantity
        # Thêm 2 cột mới vào file tổng hợp
        final_row["Doanh thu"] = revenue
        final_row["Tiền mặt"] = cash_payment
        return final_row
    else:
        return None

def process_debt_details(df, store_name):
    """
    Xử lý DataFrame báo cáo BH03 để bóc tách chi tiết công nợ.
    Trả về list of dict cho các dòng product.
    """
    debt_details = []
    current_customer = None
    in_debt_section = False

    for i, row in df.iterrows():
        col_A = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ''
        col_B = str(row.iloc[1]).strip() if pd.notna(row.iloc[1]) else ''

        if col_A in ['II', 'III']:
            in_debt_section = True
            continue

        if col_A.startswith('IV'):
            in_debt_section = False
            continue

        if not in_debt_section:
            continue

        if col_A.isdigit() or (col_A.endswith('.') and col_A[:-1].isdigit()):  # e.g., '1', '2', '1.', '2.'
            current_customer = col_B
            continue  # Không thêm row ngay, chờ product

        if col_A == '' and col_B and current_customer:  # Dòng product
            product = col_B
            # Convert to Series to handle fillna safely
            quantity_series = pd.Series([row.iloc[6]]).astype(float)
            unit_price_series = pd.Series([row.iloc[7]]).astype(float)
            debt_series = pd.Series([row.iloc[16] if len(row) > 16 else 0]).astype(float)

            quantity = quantity_series.iloc[0]
            unit_price = unit_price_series.iloc[0]
            debt = debt_series.iloc[0]

            # Tìm mã KH
            customer_code = 'Không tìm thấy mã khách'
            matching_rows = dskh_df[dskh_df.iloc[:,1].str.strip().str.lower() == current_customer.strip().lower()]
            if not matching_rows.empty:
                customer_code = matching_rows.iloc[0, 3]

            debt_details.append({
                'Store': store_name,
                'Customer_Name': current_customer,
                'Customer_Code': customer_code,
                'Product': product,
                'Quantity': quantity if pd.notna(quantity) else 0,
                'Unit_Price': unit_price if pd.notna(unit_price) else 0,
                'Debt': debt if pd.notna(debt) else 0
            })

    return debt_details