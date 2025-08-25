# -*- coding: utf-8 -*-
import pandas as pd
import config

def process_and_validate_bh03(df, store_name):
    """
    Xử lý DataFrame báo cáo BH03 và kiểm tra tính hợp lệ.
    Trả về dòng tổng hợp nếu hợp lệ, ngược lại trả về None.
    """
    if not isinstance(df, pd.DataFrame) or df.empty:
        return None
        
    summary_data = {prod: 0 for prod in config.TARGET_PRODUCTS_BH03}
    start_index = -1
    for i, row in df.iterrows():
        if str(row.iloc[0]).strip().startswith('IV'):
            start_index = i + 1
            break
    
    if start_index == -1:
        return None

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
    
    total_quantity = sum(summary_data.values())
    
    if total_quantity > 0:
        final_row = {"Tên CHXD": store_name}
        final_row.update(summary_data)
        final_row["Tổng sản lượng"] = total_quantity
        return final_row
    else:
        return None
