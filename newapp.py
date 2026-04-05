import re
from io import StringIO
from typing import Dict, List

import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup

try:
    import accounts as account_module
except Exception:
    account_module = None

BASE_URL = "https://backend.lemonclean.com.tw"
LOGIN_URL = f"{BASE_URL}/login"
USER_ADD_URL = f"{BASE_URL}/user/add"
USER_LIST_URL = f"{BASE_URL}/user"

GOOGLE_SHEET_ID = "1hsmwhA36I0BPXQ8d6OYGGn8R_SETQe4vTR_FB5Sp8Uc"
GOOGLE_SHEET_NAME = "新人基本資料"
GOOGLE_SHEET_CSV_URL = (
    f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/gviz/tq"
    f"?tqx=out:csv&sheet={GOOGLE_SHEET_NAME}"
)

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": USER_ADD_URL,
}

# 固定值
FIXED_VALUES = {
    "使用者類型": "專員",
    "專員類型": "居家專員",
    "服務項目": "居家清潔",
    "意外險": "有",
    "良民證": "有",
    "角色": "專員管理",
    "狀態": "正常",
}

# 後台表單欄位
FORM_FIELD_MAP = {
    "使用者類型": "user_type_id",
    "專員類型": "coordinator_type",
    "使用者名稱": "name",
    "使用者密碼": "password",
    "email": "email",
    "生日": "birthday",
    "身分證字號": "id_number",
    "電話": "phone",
    "地址": "address",
    "緊急連絡人姓名": "urgent_name",
    "緊急連絡人關係": "urgent_relationship",
    "緊急連絡人電話": "urgent_phone",
    "到職日期": "date_arrival",
    "意外險": "pa",
    "良民證": "police_certificate",
    "總體表現": "score",
    "薪等": "wage_level",
    "時薪": "wage",
    "排班備註": "memoSchedule",
    "備註": "memo",
    "角色": "role_id[]",
    "狀態": "flag",
}

# 單選/文字值對應
VALUE_MAP = {
    "使用者類型": {"專員": "2", "內勤": "1", "客服": "1"},
    "專員類型": {"居家專員": "1", "家電／傢俱專員": "2", "收納專員": "3"},
    "意外險": {"有": "1", "無": "0"},
    "良民證": {"有": "1", "無": "0"},
    "狀態": {"正常": "1", "停用": "0"},
    "角色": {
        "專員管理": "1",   # 若實際不對，再改這裡
        "系統管理員": "2",
        "客服": "3",
        "外場主管": "4",
        "分店主管": "5",
        "行銷": "6",
    },
}

# 服務項目 checkbox 對應
COORDINATOR_ITEM_MAP = {
    "居家清潔": "1",
    "簡易收納": "2",
    "整理收納": "3",
    "裝潢清潔": "4",
    "空屋清潔": "5",
    "鐘點清潔": "6",
    "家電清潔": "7",
    "洗衣機清潔": "8",
    "冷氣清潔": "9",
    "床墊清潔": "10",
}

REQUIRED_SHEET_COLUMNS = [
    "使用者名稱", "使用者密碼", "email", "生日", "身分證字號", "電話", "地址",
    "緊急連絡人姓名", "緊急連絡人關係", "緊急連絡人電話", "到職日期",
    "總體表現", "薪等", "時薪"
]

# 只吃前面真正匯入用的欄位
SOURCE_COLUMN_MAP = {
    "使用者名稱": "使用者名稱",
    "使用者密碼": "使用者密碼",
    "email": "email",
    "生日": "生日",
    "身分證字號": "身分證字號",
    "電話": "電話",
    "地址": "地址",
    "緊急連絡人姓名": "緊急連絡人姓名",
    "緊急連絡人關係": "緊急連絡人關係",
    "緊急連絡人電話": "緊急連絡人電話",
    "到職日期": "到職日期",
    "意外險": "意外險",
    "良民證": "良民證",
    "總體表現": "總體表現",
    "薪等": "薪等",
    "時薪": "時薪",
    "角色": "角色",
    "狀態": "狀態",
    "服務項目": "服務項目",
    "排班備註": "排班備註",
    "備註": "備註",
}

def load_accounts() -> Dict[str, Dict[str, str]]:
    accounts: Dict[str, Dict[str, str]] = {}
    if account_module is None:
        return accounts

    if hasattr(account_module, "ACCOUNTS"):
        raw = getattr(account_module, "ACCOUNTS")
        if isinstance(raw, dict):
            for k, v in raw.items():
                if isinstance(v, dict):
                    email = v.get("email")
                    password = v.get("password")
                    if email and password:
                        accounts[str(k)] = {
                            "email": str(email),
                            "password": str(password),
                        }
    return accounts

def fetch_sheet() -> pd.DataFrame:
    resp = requests.get(GOOGLE_SHEET_CSV_URL, timeout=30)
    resp.raise_for_status()
    return pd.read_csv(StringIO(resp.text))

def get_login_token(session: requests.Session) -> str:
    r = session.get(LOGIN_URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    token_input = soup.find("input", {"name": "_token"})
    if not token_input or not token_input.get("value"):
        raise RuntimeError("找不到登入頁 _token")
    return token_input["value"]

def login_backend(email: str, password: str) -> requests.Session:
    session = requests.Session()
    token = get_login_token(session)
    payload = {
        "_token": token,
        "email": email,
        "password": password,
    }
    r = session.post(LOGIN_URL, data=payload, headers=HEADERS, timeout=30, allow_redirects=True)
    r.raise_for_status()
    if "login" in r.url.lower():
        raise RuntimeError("登入失敗，請確認 accounts.py 的帳密")
    return session

def inspect_user_add_form(session: requests.Session) -> Dict:
    r = session.get(USER_ADD_URL, headers=HEADERS, timeout=30, allow_redirects=True)
    r.raise_for_status()
    if "login" in r.url.lower():
        raise RuntimeError("登入狀態失效，已被導回登入頁")

    soup = BeautifulSoup(r.text, "html.parser")
    form = soup.find("form")
    if not form:
        raise RuntimeError("找不到新增使用者表單")

    token_input = form.find("input", {"name": "_token"})
    if not token_input or not token_input.get("value"):
        raise RuntimeError("找不到表單 _token")

    action = form.get("action") or "/user/add"
    method = (form.get("method") or "POST").upper()
    submit_url = action if action.startswith("http") else BASE_URL + action

    names = set()
    for tag in form.find_all(["input", "select", "textarea"]):
        name = tag.get("name")
        if name:
            names.add(name)

    return {
        "submit_url": submit_url,
        "method": method,
        "_token": token_input["value"],
        "field_names": sorted(names),
    }

def validate_sheet_columns(df: pd.DataFrame) -> List[str]:
    return [c for c in REQUIRED_SHEET_COLUMNS if c not in df.columns]

def normalize_row(row: Dict) -> Dict[str, str]:
    normalized = {}
    for target_key, source_col in SOURCE_COLUMN_MAP.items():
        value = row.get(source_col, "")
        if pd.isna(value):
            normalized[target_key] = ""
        else:
            normalized[target_key] = str(value).strip()
    return normalized

def map_single_value(zh_name: str, value: str) -> str:
    value = str(value or "").strip()
    mapper = VALUE_MAP.get(zh_name)
    return mapper.get(value, value) if mapper else value

def convert_roc_to_ad_if_needed(value: str) -> str:
    text = str(value or "").strip()
    m = re.match(r"^\s*(\d{2,3})[/-](\d{1,2})[/-](\d{1,2})\s*$", text)
    if not m:
        return text
    y = int(m.group(1)) + 1911
    mth = int(m.group(2))
    day = int(m.group(3))
    return f"{y:04d}-{mth:02d}-{day:02d}"

def build_payload(row: Dict[str, str], token: str, convert_birthday_to_ad: bool) -> Dict[str, object]:
    merged = {}
    merged.update(FIXED_VALUES)
    merged.update(row)

    payload: Dict[str, object] = {"_token": token}

    for zh_name, field_name in FORM_FIELD_MAP.items():
        value = merged.get(zh_name, "").strip()

        if zh_name == "生日":
            if convert_birthday_to_ad:
                value = convert_roc_to_ad_if_needed(value)
            payload[field_name] = value
            continue

        if zh_name == "角色":
            mapped = map_single_value(zh_name, value)
            payload[field_name] = [mapped] if mapped else []
            continue

        payload[field_name] = map_single_value(zh_name, value)

    service_value = merged.get("服務項目", "").strip()
    items = []
    raw_items = [x.strip() for x in re.split(r"[、,，/]+", service_value) if x.strip()]
    if not raw_items and service_value:
        raw_items = [service_value]
    for item in raw_items:
        mapped = COORDINATOR_ITEM_MAP.get(item)
        if mapped:
            items.append(mapped)
    payload["coordinator_item[]"] = items

    if payload.get("password") and "password_confirmation" not in payload:
        payload["password_confirmation"] = payload["password"]

    return payload

def submit_user(session: requests.Session, submit_url: str, payload: Dict[str, object]) -> requests.Response:
    return session.post(
        submit_url,
        data=payload,
        headers=HEADERS,
        timeout=30,
        allow_redirects=True,
    )

def extract_error_message(resp: requests.Response) -> str:
    soup = BeautifulSoup(resp.text, "html.parser")
    msgs = []

    selectors = [
        ".alert-danger",
        ".invalid-feedback",
        ".help-block",
        ".text-danger",
        ".error",
    ]

    for selector in selectors:
        for tag in soup.select(selector):
            txt = tag.get_text(" ", strip=True)
            if txt and txt not in msgs:
                msgs.append(txt)

    for li in soup.select("ul li"):
        txt = li.get_text(" ", strip=True)
        if txt and ("必填" in txt or "錯誤" in txt or "required" in txt.lower()):
            if txt not in msgs:
                msgs.append(txt)

    if msgs:
        return " | ".join(msgs[:10])

    title = soup.title.get_text(strip=True) if soup.title else ""
    return f"回傳表單頁，可能驗證失敗。URL={resp.url} TITLE={title}"

def is_success_response(resp: requests.Response) -> bool:
    if resp.status_code >= 400:
        return False

    # 常見成功情境：跳回 /user 列表頁
    normalized_url = resp.url.rstrip("/")
    if normalized_url == USER_LIST_URL:
        return True

    text = resp.text.lower()

    # 若還是新增頁表單，通常代表驗證失敗
    if "<form" in text and 'name="name"' in text and 'name="password"' in text and 'name="_token"' in text:
        return False

    return False

st.set_page_config(page_title="新人匯入工具", page_icon="🍋", layout="wide")
st.title("🍋 新人使用者匯入工具")
st.caption("登入後台 → 讀 Google Sheet → 選擇起訖列 → 匯入網站")

accounts = load_accounts()

if "session" not in st.session_state:
    st.session_state.session = None
if "logged_in_email" not in st.session_state:
    st.session_state.logged_in_email = ""
if "sheet_df" not in st.session_state:
    st.session_state.sheet_df = None
if "form_info" not in st.session_state:
    st.session_state.form_info = None

with st.expander("1. 後台登入", expanded=True):
    if accounts:
        account_name = st.selectbox("選擇 accounts.py 帳號", list(accounts.keys()))
        default_email = accounts[account_name]["email"]
        default_password = accounts[account_name]["password"]
    else:
        default_email = ""
        default_password = ""

    c1, c2, c3 = st.columns([2, 2, 1])
    with c1:
        email = st.text_input("Email", value=default_email)
    with c2:
        password = st.text_input("Password", value=default_password, type="password")
    with c3:
        st.write("")
        st.write("")
        login_clicked = st.button("登入後台", use_container_width=True)

    if login_clicked:
        try:
            session = login_backend(email, password)
            form_info = inspect_user_add_form(session)
            st.session_state.session = session
            st.session_state.logged_in_email = email
            st.session_state.form_info = form_info
            st.success(f"登入成功：{email}")
            st.info(f"submit={form_info['submit_url']}")
        except Exception as e:
            st.error(str(e))

    if st.session_state.session:
        st.success(f"目前登入中：{st.session_state.logged_in_email}")

with st.expander("2. 讀取 Google Sheet", expanded=True):
    st.write(f"Sheet ID：`{GOOGLE_SHEET_ID}`")
    st.write(f"工作表：`{GOOGLE_SHEET_NAME}`")

    if st.button("讀取 Google Sheet"):
        try:
            df = fetch_sheet()
            st.session_state.sheet_df = df
            st.success(f"已讀取 {len(df)} 筆資料")
        except Exception as e:
            st.error(f"讀取失敗：{e}")

    df = st.session_state.sheet_df
    if df is not None:
        missing = validate_sheet_columns(df)
        if missing:
            st.error("Google Sheet 缺少欄位：" + "、".join(missing))
        else:
            st.success("Google Sheet 欄位檢查通過")
        st.dataframe(df.head(10), use_container_width=True)

with st.expander("3. 選擇起訖列與預覽", expanded=True):
    df = st.session_state.sheet_df
    if df is None:
        st.info("請先讀取 Google Sheet")
    else:
        total_rows = len(df)
        st.write(f"資料總列數：{total_rows}（不含標題列）")

        start_row = st.number_input("起始列（Google Sheet 列號）", min_value=2, value=2, step=1, key="start_row")
        end_row = st.number_input("結束列（Google Sheet 列號）", min_value=2, value=min(5, total_rows + 1), step=1, key="end_row")

        if end_row < start_row:
            st.error("結束列不可小於起始列")
        else:
            selected_df = df.iloc[int(start_row) - 2:int(end_row) - 1].copy()
            preview_rows = []
            for _, row in selected_df.iterrows():
                row_dict = normalize_row(row.to_dict())
                preview_rows.append(row_dict)
            st.write(f"預覽列數：{len(preview_rows)}")
            st.dataframe(pd.DataFrame(preview_rows), use_container_width=True)

with st.expander("4. 表單欄位檢查", expanded=False):
    form_info = st.session_state.form_info
    if not form_info:
        st.info("請先登入後台")
    else:
        st.json(form_info["field_names"], expanded=False)
        st.write("固定 mapping：")
        st.json(FORM_FIELD_MAP, expanded=False)
        st.write("來源欄位 mapping：")
        st.json(SOURCE_COLUMN_MAP, expanded=False)

with st.expander("5. 開始匯入", expanded=True):
    dry_run = st.checkbox("測試模式（只組 payload，不真的送出）", value=False)
    convert_birthday_to_ad = st.checkbox("生日送出改西元格式（除錯用）", value=False)

    if st.button("開始匯入", type="primary"):
        df = st.session_state.sheet_df
        session = st.session_state.session
        form_info = st.session_state.form_info

        if df is None:
            st.error("請先讀取 Google Sheet")
        elif session is None or form_info is None:
            st.error("請先登入後台")
        else:
            missing = validate_sheet_columns(df)
            if missing:
                st.error("Google Sheet 缺少欄位：" + "、".join(missing))
            else:
                start_row = int(st.session_state.start_row)
                end_row = int(st.session_state.end_row)

                if end_row < start_row:
                    st.error("結束列不可小於起始列")
                else:
                    selected_df = df.iloc[start_row - 2:end_row - 1].copy()
                    results = []

                    progress = st.progress(0)
                    status_box = st.empty()

                    for idx, (_, row) in enumerate(selected_df.iterrows(), start=1):
                        sheet_row_no = start_row + idx - 1
                        row_dict = normalize_row(row.to_dict())
                        payload = build_payload(
                            row_dict,
                            form_info["_token"],
                            convert_birthday_to_ad=convert_birthday_to_ad,
                        )

                        with st.expander(f"第 {sheet_row_no} 列 payload 預覽", expanded=False):
                            st.json(payload)

                        if dry_run:
                            success = True
                            message = "測試模式，未送出"
                        else:
                            try:
                                resp = submit_user(session, form_info["submit_url"], payload)
                                success = is_success_response(resp)
                                if success:
                                    message = "成功"
                                else:
                                    message = f"失敗 HTTP {resp.status_code} / {extract_error_message(resp)}"
                            except Exception as e:
                                success = False
                                message = str(e)

                        results.append({
                            "Sheet列號": sheet_row_no,
                            "使用者名稱": row_dict.get("使用者名稱", ""),
                            "email": row_dict.get("email", ""),
                            "生日(民國)": row_dict.get("生日", ""),
                            "電話": row_dict.get("電話", ""),
                            "到職日期": row_dict.get("到職日期", ""),
                            "結果": "成功" if success else "失敗",
                            "訊息": message,
                        })

                        status_box.info(f"處理中：第 {sheet_row_no} 列 / {row_dict.get('使用者名稱', '')}")
                        progress.progress(idx / max(len(selected_df), 1))

                    result_df = pd.DataFrame(results)
                    st.success("處理完成")
                    st.dataframe(result_df, use_container_width=True)
                    st.write(f"成功：{(result_df['結果'] == '成功').sum()} 筆")
                    st.write(f"失敗：{(result_df['結果'] == '失敗').sum()} 筆")

                    csv_bytes = result_df.to_csv(index=False).encode("utf-8-sig")
                    st.download_button(
                        "下載結果 CSV",
                        data=csv_bytes,
                        file_name="import_result.csv",
                        mime="text/csv",
                    )
