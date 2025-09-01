# -*- coding: utf-8 -*-
"""
monthly_summary_gsheet.py
Các hàm tạo/cập nhật Google Sheets tổng hợp theo tháng (đặt trong thư mục "Năm {year}") tên:
  "Tổng hợp tháng {month}.{year}"

File gồm 2 sheet:
  - "Sản lượng tháng {month}": STT | Tên CHXD | 1..last_day | Lũy kế | Bình quân ngày
  - "Doanh thu tháng {month}": STT | Tên CHXD | 1..last_day | Lũy kế | Bình quân ngày

MỚI: update **một ngày** sau khi tải xong BCBH.<dd.mm.yyyy>, không quét cả tháng.
- Bảo toàn phần thập phân kiểu VN (dấu , là thập phân).
- Xóa sheet mặc định trống ("Trang tính 1", "Sheet1", ...).

Có thể chạy độc lập để build cả tháng (giữ lại cho tiện debug):
  python monthly_summary_gsheet.py --year 2025 --month 8
"""

import argparse
import time
from datetime import datetime, timedelta
from collections import defaultdict
import re

import gspread
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import config
import google_handler

# ===== cấu hình retry (chống mạng chập chờn / throttle) =====
RETRIES_PER_CALL = 5
BASE_DELAY = 0.8      # giây
SLEEP_BETWEEN_OPS = 0.1


# ============================ Helpers chung ============================

def month_range(year: int, month: int):
    s = datetime(year, month, 1)
    e = (datetime(year + (month // 12), (month % 12) + 1, 1) - timedelta(days=1))
    return s, e

def get_clients():
    creds = google_handler.get_google_credentials()
    gclient = gspread.authorize(creds)
    drive = build("drive", "v3", credentials=creds)
    return gclient, drive

def drive_list_with_retry(drive, **kwargs):
    last_err = None
    for att in range(RETRIES_PER_CALL):
        try:
            return drive.files().list(**kwargs).execute()
        except HttpError as e:
            last_err = e
            time.sleep(BASE_DELAY * (2 ** att))
    raise last_err

def get_year_folder_id(drive, year: int) -> str:
    return google_handler.get_or_create_gdrive_folder(
        drive, f"Năm {year}", config.GOOGLE_DRIVE_ROOT_FOLDER_ID
    )

def get_month_folder_id(drive, year_folder_id: str, month: int) -> str:
    # Thư mục do app tạo tự động -> dùng đúng "Tháng {month}"
    return google_handler.get_or_create_gdrive_folder(
        drive, f"Tháng {month}", year_folder_id
    )

def find_file_exact_in_folder(drive, folder_id: str, name: str):
    """Tìm đúng file Google Sheet theo tên tuyệt đối trong 1 thư mục."""
    q = (
        f"'{folder_id}' in parents and "
        f"name = '{name}' and "
        f"mimeType = 'application/vnd.google-apps.spreadsheet' and trashed = false"
    )
    resp = drive_list_with_retry(
        drive,
        q=q,
        fields="files(id,name)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    )
    files = resp.get("files", [])
    return files[0]["id"] if files else None

def find_file_exact_global(drive, name: str):
    """Fallback: tìm đúng tên trên toàn Drive (ít khi cần)."""
    q = (
        f"name = '{name}' and "
        f"mimeType = 'application/vnd.google-apps.spreadsheet' and trashed = false"
    )
    resp = drive_list_with_retry(
        drive,
        q=q,
        fields="files(id,name,parents)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    )
    files = resp.get("files", [])
    return files[0]["id"] if files else None

def open_values_with_retry(gclient, file_id: str, sheet_name="TongHopBCBH"):
    """Mở sheet và trả (header(list), rows(list[list])) hoặc (None,None)."""
    for att in range(RETRIES_PER_CALL):
        try:
            ss = gclient.open_by_key(file_id)
            ws = ss.worksheet(sheet_name)
            values = ws.get_all_values()
            if not values or len(values) < 2:
                return None, None
            header = [h.strip() for h in values[0]]
            return header, values[1:]
        except Exception:
            time.sleep(BASE_DELAY * (2 ** att))
    return None, None

# ---- Parse số: bảo toàn dấu thập phân VN ----
_DEC_TAIL_RE = re.compile(r"[.,]\d{1,6}$")
def to_number_preserve(s):
    """
    Chuyển chuỗi -> float, GIỮ phần thập phân theo quy tắc:
      - Nếu có cả '.' và ',', ký tự xuất hiện SAU CÙNG là dấu thập phân.
      - Nếu chỉ có ',', và đuôi ',\d+$'  => ',' là thập phân.
      - Nếu chỉ có '.', và đuôi '.\d+$' => '.' là thập phân.
      - Còn lại '.'/',' là phân tách nghìn -> loại bỏ.
    """
    if s is None:
        return 0.0
    t = str(s).strip().replace(" ", "")
    if t == "":
        return 0.0
    has_dot = "." in t
    has_com = "," in t
    if has_dot and has_com:
        if t.rfind(",") > t.rfind("."):
            t = t.replace(".", "")
            t = t.replace(",", ".")
        else:
            t = t.replace(",", "")
    elif has_com:
        if _DEC_TAIL_RE.search(t):
            t = t.replace(".", "")
            t = t.replace(",", ".")
        else:
            t = t.replace(",", "")
    elif has_dot:
        if _DEC_TAIL_RE.search(t):
            pass
        else:
            t = t.replace(".", "")
    try:
        return float(t)
    except Exception:
        return 0.0

# ============================ Tạo/đọc file Tổng hợp tháng ============================

def ensure_summary_spreadsheet(gclient, drive, year: int, month: int):
    """Tạo/mở file 'Tổng hợp tháng {month}.{year}' trong thư mục 'Năm {year}'."""
    year_folder_id = get_year_folder_id(drive, year)
    file_name = f"Tổng hợp tháng {month}.{year}"

    resp = drive_list_with_retry(
        drive,
        q=(
            f"'{year_folder_id}' in parents and "
            f"name = '{file_name}' and "
            f"mimeType='application/vnd.google-apps.spreadsheet' and trashed=false"
        ),
        fields="files(id,name)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    )
    files = resp.get("files", [])
    if files:
        ss = gclient.open_by_key(files[0]["id"])
        created = False
    else:
        ss = gclient.create(file_name)
        try:
            drive.files().update(
                fileId=ss.id,
                addParents=year_folder_id,
                removeParents="root",
                fields="id, parents",
                supportsAllDrives=True,
            ).execute()
        except Exception:
            pass
        created = True
    return ss, created

def ensure_worksheet(ss, title, min_cols):
    """Đảm bảo tồn tại worksheet có tiêu đề `title`."""
    try:
        ws = ss.worksheet(title)
    except Exception:
        ws = ss.add_worksheet(title=title, rows=2000, cols=max(40, min_cols))
    return ws

def remove_empty_default_sheets(ss, keep_titles):
    """Xóa sheet mặc định trống (Trang tính 1, Sheet1, ...) nếu không thuộc keep_titles."""
    default_names = {"Trang tính 1", "Sheet1", "Sheet", "Trang tính"}
    for ws in ss.worksheets():
        name = ws.title.strip()
        if name in keep_titles:
            continue
        if name in default_names:
            try:
                ss.del_worksheet(ws)
            except Exception:
                pass

def read_sheet_as_df(ws, last_day: int, kind: str) -> pd.DataFrame:
    """
    Đọc ws -> DataFrame với cột chuẩn: STT, Tên CHXD, 1..last_day, Lũy kế, Bình quân ngày
    Nếu sheet rỗng: tạo DataFrame rỗng với header chuẩn (chưa có hàng).
    """
    try:
        values = ws.get_all_values()
    except Exception:
        values = []
    days = [str(i) for i in range(1, last_day + 1)]
    cols = ["STT", "Tên CHXD"] + days + ["Lũy kế", "Bình quân ngày"]

    if not values:
        return pd.DataFrame(columns=cols)

    header = [h.strip() for h in values[0]]
    rows = values[1:]

    # Map theo header hiện tại -> chuẩn hóa về bộ cột đích
    df = pd.DataFrame(rows, columns=header)
    # Thêm cột thiếu
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    # Bỏ cột thừa (không nằm trong chuẩn)
    df = df[cols]
    # Chuẩn hóa STT về số nếu có
    if not df.empty:
        try:
            df["STT"] = pd.to_numeric(df["STT"], errors="coerce")
        except Exception:
            pass
    return df

def write_df(ws, df: pd.DataFrame):
    """Ghi DataFrame vào worksheet (ghi cả header)."""
    if df is None or df.empty:
        ws.update(values=[["STT", "Tên CHXD", "1"]], range_name="A1")  # tối thiểu header
        return
    values = [list(df.columns)] + df.astype(object).values.tolist()
    ws.update(values=values, range_name="A1", value_input_option="USER_ENTERED")

# ============================ Lấy số liệu từ BCBH một ngày ============================

def find_bcbh_file_for_date(drive, report_date: datetime):
    """Tìm fileId cho BCBH.<dd.mm.yyyy> ưu tiên đúng thư mục 'Năm/Tháng'."""
    year = report_date.year
    month = report_date.month
    day = report_date.day
    fname = f"BCBH.{day:02d}.{month:02d}.{year}"

    year_folder_id = get_year_folder_id(drive, year)
    month_folder_id = get_month_folder_id(drive, year_folder_id, month)

    # Tìm trong thư mục tháng trước
    fid = find_file_exact_in_folder(drive, month_folder_id, fname)
    if fid:
        return fid
    # Fallback toàn Drive
    fid = find_file_exact_global(drive, fname)
    return fid

def extract_daily_values_from_bcbh(gclient, file_id: str):
    """
    Đọc sheet TongHopBCBH của file ngày -> trả về:
      - sl_by_store: dict[store] = sản lượng (float, giữ 3 số lẻ)
      - dt_by_store: dict[store] = doanh thu (float)
    """
    header, rows = open_values_with_retry(gclient, file_id, "TongHopBCBH")
    if not header:
        return {}, {}

    hidx = {name: i for i, name in enumerate(header)}
    idx_store = hidx.get("Tên CHXD", hidx.get("Cửa hàng", None))
    idx_tongsl = hidx.get("Tổng sản lượng", None)
    idx_rev = hidx.get("Doanh thu", None)
    product_idxs = [hidx[p] for p in config.TARGET_PRODUCTS_BH03 if p in hidx]

    sl_by_store = defaultdict(float)
    dt_by_store = defaultdict(float)

    for r in rows:
        if idx_store is None or idx_store >= len(r):
            continue
        store = r[idx_store]

        # Sản lượng
        if idx_tongsl is not None and idx_tongsl < len(r):
            v_sl = to_number_preserve(r[idx_tongsl])
        else:
            v_sl = sum(to_number_preserve(r[i]) for i in product_idxs if i < len(r))
        sl_by_store[store] += v_sl

        # Doanh thu
        if idx_rev is not None and idx_rev < len(r):
            v_dt = to_number_preserve(r[idx_rev])
            dt_by_store[store] += v_dt

    return sl_by_store, dt_by_store

# ============================ Cập nhật MỘT NGÀY vào file tổng hợp ============================

def _recalc_totals_and_avg(df: pd.DataFrame, last_day: int, kind: str):
    """Tính lại Lũy kế, Bình quân ngày dựa trên cột 1..last_day (giữ 3 số lẻ cho SL)."""
    day_cols = [str(i) for i in range(1, last_day + 1)]
    totals = []
    avgs = []
    for _, row in df.iterrows():
        s = 0.0
        n = 0
        for c in day_cols:
            val = row[c]
            if val == "" or val is None:
                continue
            v = to_number_preserve(val)
            s += v
            n += 1  # đếm cả 0 là ngày có dữ liệu
        if kind == "SL":
            totals.append(round(s, 3))
            avgs.append(round(s / n, 3) if n > 0 else "")
        else:
            totals.append(round(s, 0))
            avgs.append(round(s / n, 2) if n > 0 else "")
    df["Lũy kế"] = totals
    df["Bình quân ngày"] = avgs

def _update_one_sheet(ss, sheet_title: str, last_day: int, day_num: int, values_by_store: dict, kind: str):
    """
    Cập nhật 1 sheet (SL/DT) cho đúng ngày `day_num`.
    - Tạo header nếu sheet trống
    - Thêm dòng nếu gặp CHXD mới
    - Ghi giá trị cột `day_num` theo dict values_by_store
    - Tính lại Lũy kế, Bình quân ngày
    """
    ws = ensure_worksheet(ss, sheet_title, 2 + last_day + 2)
    df = read_sheet_as_df(ws, last_day, kind)

    # Đảm bảo đủ cột ngày
    for i in range(1, last_day + 1):
        col = str(i)
        if col not in df.columns:
            df[col] = ""
    # đảm bảo cột tổng/avg
    for c in ["Lũy kế", "Bình quân ngày"]:
        if c not in df.columns:
            df[c] = ""

    # Map tên CHXD -> index
    name_col = "Tên CHXD"
    existing_names = list(df[name_col]) if name_col in df.columns else []
    name_to_idx = {name: i for i, name in enumerate(existing_names)}

    # Danh sách tất cả cửa hàng = hiện có ∪ mới
    all_names = set(existing_names) | set(values_by_store.keys())
    # Sắp xếp để STT ổn định (alpha)
    sorted_names = sorted([n for n in all_names if n not in [None, ""]])

    # Xây df_new theo thứ tự sorted_names
    day_cols = [str(i) for i in range(1, last_day + 1)]
    cols = ["STT", name_col] + day_cols + ["Lũy kế", "Bình quân ngày"]
    df_new = pd.DataFrame(columns=cols)

    for idx, store in enumerate(sorted_names, start=1):
        # lấy hàng cũ nếu có
        if store in name_to_idx:
            row_old = df.loc[name_to_idx[store], :].to_dict()
        else:
            row_old = {c: "" for c in cols}
        row = {c: row_old.get(c, "") for c in cols}
        row["STT"] = idx
        row[name_col] = store

        # ghi giá trị ngày cần cập nhật
        v = values_by_store.get(store, None)
        if v is not None:
            if kind == "SL":
                row[str(day_num)] = round(float(v), 3)
            else:
                row[str(day_num)] = round(float(v), 0)
        df_new.loc[len(df_new)] = row

    # Tính lại tổng & bình quân
    _recalc_totals_and_avg(df_new, last_day, kind)

    # Ghi đè vào sheet
    write_df(ws, df_new)

def update_monthly_for_single_day(report_date: datetime):
    """
    API public: Cập nhật file 'Tổng hợp tháng m.yyyy' chỉ cho NGÀY report_date.
    - Đọc BCBH.<dd.mm.yyyy> -> lấy SL & DT theo CHXD
    - Tạo/mở file Tổng hợp tháng -> cập nhật cột ngày tương ứng trên 2 sheet
    """
    gclient, drive = get_clients()
    last_day = month_range(report_date.year, report_date.month)[1].day

    # 1) Tìm file BCBH của ngày
    file_id = find_bcbh_file_for_date(drive, report_date)
    if not file_id:
        print(f"[CẢNH BÁO] Không tìm thấy file BCBH cho ngày {report_date:%d/%m/%Y}. Bỏ qua cập nhật.")
        return

    # 2) Lấy dữ liệu ngày (bảo toàn thập phân)
    sl_by_store, dt_by_store = extract_daily_values_from_bcbh(gclient, file_id)

    # 3) Mở/Tạo file Tổng hợp tháng và cập nhật đúng ngày
    ss, _ = ensure_summary_spreadsheet(gclient, drive, report_date.year, report_date.month)

    title_sl = f"Sản lượng tháng {report_date.month}"
    title_dt = f"Doanh thu tháng {report_date.month}"

    _update_one_sheet(ss, title_sl, last_day, report_date.day, sl_by_store, kind="SL")
    time.sleep(SLEEP_BETWEEN_OPS)
    _update_one_sheet(ss, title_dt, last_day, report_date.day, dt_by_store, kind="DT")

    # 4) Xóa sheet mặc định trống nếu còn
    remove_empty_default_sheets(ss, {title_sl, title_dt})

    print(f"[OK] Đã cập nhật 'Tổng hợp tháng {report_date.month}.{report_date.year}' cho ngày {report_date:%d/%m/%Y}.")

# ============================ (Tùy chọn) Build cả tháng để debug ============================

def _build_month_all(year: int, month: int):
    """
    Hàm debug: dựng lại cả tháng (giữ từ bản trước).
    Dùng khi cần kiểm tra tổng thể. Không dùng trong luồng thường ngày.
    """
    gclient, drive = get_clients()
    _, end = month_range(year, month)
    last_day = end.day

    # gom theo tháng (ít dùng)
    from collections import defaultdict
    store_day_sl = defaultdict(dict)
    store_day_dt = defaultdict(dict)
    all_stores = set()

    for d in range(1, last_day + 1):
        fid = find_bcbh_file_for_date(drive, datetime(year, month, d))
        if not fid:
            continue
        sl_by_store, dt_by_store = extract_daily_values_from_bcbh(gclient, fid)
        for k, v in sl_by_store.items():
            store_day_sl[k][d] = v; all_stores.add(k)
        for k, v in dt_by_store.items():
            store_day_dt[k][d] = v; all_stores.add(k)

    # ghi ra file
    ss, _ = ensure_summary_spreadsheet(gclient, drive, year, month)
    title_sl = f"Sản lượng tháng {month}"
    title_dt = f"Doanh thu tháng {month}"

    # build df từ store_day_* (giữ nguyên format)
    day_cols = [str(i) for i in range(1, last_day + 1)]
    cols = ["STT", "Tên CHXD"] + day_cols + ["Lũy kế", "Bình quân ngày"]

    def build_df(kind, store_day):
        df = pd.DataFrame(columns=cols)
        for idx, store in enumerate(sorted(all_stores), start=1):
            row = {"STT": idx, "Tên CHXD": store}
            s = 0.0; n = 0
            for i in range(1, last_day + 1):
                v = store_day.get(store, {}).get(i, None)
                if v is not None:
                    row[str(i)] = round(float(v), 3) if kind=="SL" else round(float(v), 0)
                    s += float(v); n += 1
                else:
                    row[str(i)] = ""
            if kind=="SL":
                row["Lũy kế"] = round(s, 3)
                row["Bình quân ngày"] = round(s/n, 3) if n>0 else ""
            else:
                row["Lũy kế"] = round(s, 0)
                row["Bình quân ngày"] = round(s/n, 2) if n>0 else ""
            df.loc[len(df)] = row
        return df

    df_sl = build_df("SL", store_day_sl)
    df_dt = build_df("DT", store_day_dt)

    write_df(ensure_worksheet(ss, title_sl, len(cols)), df_sl)
    write_df(ensure_worksheet(ss, title_dt, len(cols)), df_dt)
    remove_empty_default_sheets(ss, {title_sl, title_dt})

# ============================ CLI (giữ cho debug) ============================

def main():
    ap = argparse.ArgumentParser(description="Tạo/Debug file Google Sheets 'Tổng hợp tháng m.yyyy' (Sản lượng & Doanh thu).")
    ap.add_argument("--year", type=int, required=True, help="Năm, ví dụ 2025")
    ap.add_argument("--month", type=int, required=True, help="Tháng 1..12")
    args = ap.parse_args()
    _build_month_all(args.year, args.month)

if __name__ == "__main__":
    main()
