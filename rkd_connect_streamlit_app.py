import streamlit as st
import pandas as pd
import re
import io
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

st.set_page_config(page_title="RKD Connect Processor", layout="wide")

# ─────────────────────────────────────────────
# Password gate
# ─────────────────────────────────────────────

def check_password():
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
    if st.session_state.authenticated:
        return True
    st.title("RKD Connect Processor")
    st.markdown("Please enter the password to continue.")
    pwd = st.text_input("Password", type="password", key="password_input")
    if st.button("Login"):
        if pwd == st.secrets["APP_PASSWORD"]:
            st.session_state.authenticated = True
            st.rerun()
        else:
            st.error("Incorrect password.")
    return False

if not check_password():
    st.stop()

# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def clean_phone(raw):
    if pd.isna(raw):
        return ""
    s = str(raw)
    s = re.split(r'(?i)\bx\w*|\bext[\s.]*\d+|\bex[\s.]*\d+', s)[0]
    digits = re.sub(r'\D', '', s)
    if len(digits) == 11 and digits.startswith('1'):
        digits = digits[1:]
    return digits if len(digits) == 10 else ""


def try_read_csv(uploaded):
    for enc in ['utf-8', 'cp1252', 'latin-1']:
        try:
            uploaded.seek(0)
            return pd.read_csv(uploaded, encoding=enc, dtype=str)
        except Exception:
            continue
    raise ValueError("Could not read CSV file.")


def prev_month_label():
    today = date.today()
    prev = today.replace(day=1) - relativedelta(months=1)
    return prev.strftime("%B %Y")


def fmt_date(val):
    if pd.isna(val) or str(val).strip() == "":
        return ""
    for fmt in [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%m/%d/%y",
        "%m-%d-%Y",
        "%d-%b-%Y %H:%M:%S",   # 19-Dec-2024 17:28:36
        "%d-%b-%Y",            # 19-Dec-2024
        "%d/%b/%Y",
        "%b %d, %Y",
        "%B %d, %Y",
    ]:
        try:
            return datetime.strptime(str(val).strip(), fmt).strftime("%m/%d/%Y")
        except Exception:
            continue
    return str(val).strip()


def parse_date(val):
    if pd.isna(val) or str(val).strip() == "":
        return None
    for fmt in [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%m/%d/%y",
        "%m-%d-%Y",
        "%d-%b-%Y %H:%M:%S",   # 19-Dec-2024 17:28:36
        "%d-%b-%Y",            # 19-Dec-2024
        "%d/%b/%Y",            # 19/Dec/2024
        "%b %d, %Y",           # Dec 19, 2024
        "%B %d, %Y",           # December 19, 2024
    ]:
        try:
            return datetime.strptime(str(val).strip(), fmt)
        except Exception:
            continue
    return None


def safe_str(val):
    s = str(val).strip()
    return "" if s.lower() == 'nan' else s


def df_to_csv_bytes(df):
    return df.to_csv(index=False).encode('utf-8')


# ─────────────────────────────────────────────
# Main App
# ─────────────────────────────────────────────

st.title("RKD Connect Processor")

st.info(
    "**File naming requirements:** Upload files whose names contain the following strings (not case-sensitive):\n"
    "- **appended phones** → RKD Appended Phones file\n"
    "- **data upload** → RKD Data Upload file\n"
    "- **re data** → Raiser's Edge Data file"
)

uploaded_files = st.file_uploader(
    "Upload your three files", accept_multiple_files=True, type=["csv", "xlsx"]
)

rkd_appended_phones = rkd_data_upload = raisers_edge_data = None

if uploaded_files:
    for f in uploaded_files:
        nl = f.name.lower()
        if "appended phones" in nl:
            rkd_appended_phones = f
        elif "data upload" in nl:
            rkd_data_upload = f
        elif "re data" in nl:
            raisers_edge_data = f

    c1, c2, c3 = st.columns(3)
    for col, label, f in [
        (c1, "appended phones", rkd_appended_phones),
        (c2, "data upload", rkd_data_upload),
        (c3, "re data", raisers_edge_data),
    ]:
        with col:
            if f:
                st.success(f"✅ `{f.name}`")
            else:
                st.warning(f"⚠️ Missing: *{label}*")

if st.button("Process Files", disabled=not all([rkd_appended_phones, rkd_data_upload, raisers_edge_data]), type="primary"):
    with st.spinner("Processing — please wait..."):
        try:
            def load(f):
                if f.name.lower().endswith('.xlsx'):
                    f.seek(0)
                    return pd.read_excel(f, dtype=str)
                return try_read_csv(f)

            df_phones = load(rkd_appended_phones)
            df_upload = load(rkd_data_upload)
            df_re     = load(raisers_edge_data)

            for df in [df_phones, df_upload, df_re]:
                df.columns = df.columns.str.strip()

            month_label = prev_month_label()

            # ── RE phone lookups ────────────────────────────────────────
            phone_num_cols  = sorted([c for c in df_re.columns if re.match(r'CnPh_1_\d{2}_Phone_number', c)])
            phone_type_cols = sorted([c for c in df_re.columns if re.match(r'CnPh_1_\d{2}_Phone_type',   c)])

            re_phones_map = {}  # CnBio_ID -> set of cleaned 10-digit phones
            re_types_map  = {}  # CnBio_ID -> list of phone types (non-null, ordered)

            for _, row in df_re.iterrows():
                bid = safe_str(row.get('CnBio_ID', ''))
                if not bid:
                    continue
                phones_set = set()
                types_list = []
                for nc, tc in zip(phone_num_cols, phone_type_cols):
                    cleaned = clean_phone(row.get(nc, ''))
                    if cleaned:
                        phones_set.add(cleaned)
                    ptype = safe_str(row.get(tc, ''))
                    if ptype:
                        types_list.append(ptype)
                re_phones_map[bid] = phones_set
                re_types_map[bid]  = types_list

            # ── Step 3: Appended phones vs RE ───────────────────────────
            phone_in_re_rows  = []
            phone_import_rows = []

            for _, row in df_phones.iterrows():
                const_id  = safe_str(row.get('Constituent ID', ''))
                phone_val = safe_str(row.get('Phone', ''))
                cleaned   = clean_phone(phone_val) or phone_val
                existing  = re_phones_map.get(const_id, set())
                if cleaned in existing or phone_val in existing:
                    phone_in_re_rows.append(row)
                else:
                    phone_import_rows.append(row)

            df_phone_in_re  = pd.DataFrame(phone_in_re_rows)
            df_phone_import = pd.DataFrame(phone_import_rows)

            # ── Step 4: Phone import processing ─────────────────────────
            output_phone_rows      = []
            phone_type_exists_rows = []

            for _, row in df_phone_import.iterrows():
                const_id       = safe_str(row.get('Constituent ID', ''))
                phone_val      = safe_str(row.get('Phone', ''))
                w_or_l         = safe_str(row.get('Phone Type', '')).upper()  # W=cell, L=landline
                date_val       = fmt_date(row.get('Date', ''))
                existing_types = re_types_map.get(const_id, [])

                if w_or_l == 'W':
                    if any(t.strip().lower() == 'cell' for t in existing_types):
                        phone_type_exists_rows.append(row)
                    else:
                        output_phone_rows.append({
                            'ConsID':        const_id,
                            'PhoneNum':      phone_val,
                            'PhoneType':     'Cell',
                            'PhoneComments': f'RKD Connect append cell {date_val}',
                        })
                else:
                    existing_lower = [t.strip().lower() for t in existing_types]
                    assigned_type  = None
                    for candidate in ['Primary Phone', 'Alt Phone', 'Alt 2 Phone', 'Alt 3 Phone']:
                        if candidate.lower() not in existing_lower:
                            assigned_type = candidate
                            break
                    if assigned_type is None:
                        phone_type_exists_rows.append(row)
                    else:
                        output_phone_rows.append({
                            'ConsID':        const_id,
                            'PhoneNum':      phone_val,
                            'PhoneType':     assigned_type,
                            'PhoneComments': f'RKD Connect append landline {date_val}',
                        })

            df_output_phones     = pd.DataFrame(output_phone_rows)
            df_phone_type_exists = pd.DataFrame(phone_type_exists_rows)

            # ── Step 5: RE action lookup ─────────────────────────────────

            # Build a case/space-insensitive column lookup for df_re
            re_col_map = {c.strip().lower(): c for c in df_re.columns}

            def re_col(name):
                """Return the actual df_re column name matching `name` (case+space insensitive)."""
                return re_col_map.get(name.strip().lower())

            # Find the actual CnBio_ID column name
            cnbio_col = re_col('CnBio_ID') or re_col('cnbio_id')

            act_groups = []
            for i in range(1, 6):
                n = f"{i:02d}"
                imp  = re_col(f"CnAct_1_{n}_Import_ID")
                adat = re_col(f"CnAct_1_{n}_Action_Date")
                nimp = re_col(f"CnAct_1_{n}_Note_1_01_Import_ID")
                desc = re_col(f"CnAct_1_{n}_Note_1_01_Description")
                if imp:  # column exists in file
                    act_groups.append((imp, adat, nimp, desc))

            re_actions_map = {}

            for _, row in df_re.iterrows():
                bid = safe_str(row.get(cnbio_col, '')) if cnbio_col else ''
                if not bid:
                    continue
                for imp, adat, nimp, desc in act_groups:
                    description = safe_str(row.get(desc, '')) if desc else ''
                    if 'receiving' not in description.lower():
                        continue
                    act_date = parse_date(row.get(adat, '')) if adat else None
                    if act_date is None:
                        continue
                    re_actions_map.setdefault(bid, []).append({
                        'import_id':      safe_str(row.get(imp, '')) if imp else '',
                        'action_date':    act_date,
                        'note_import_id': safe_str(row.get(nimp, '')) if nimp else '',
                        'description':    description,
                    })

            # ── Steps 6 & 7: Data upload processing
            no_matching_rows = []
            action_date_rows = []
            action_note_rows = []
            # debug_rows = []  # uncomment to re-enable debug panel

            # Normalize RE action map keys for comparison
            re_action_map_lower = {k.lstrip('0') or '0': v for k, v in re_actions_map.items()}

            for _, row in df_upload.iterrows():
                const_id       = safe_str(row.get('Constituent ID', ''))
                time_raw       = safe_str(row.get('Time', ''))
                result_val     = safe_str(row.get('Result', ''))
                call_notes_val = safe_str(row.get('Call Notes', ''))

                upload_date = parse_date(time_raw)

                # Try both original and stripped-leading-zero versions of const_id
                const_id_stripped = const_id.lstrip('0') or '0'
                candidates = (
                    re_actions_map.get(const_id)
                    or re_actions_map.get(const_id_stripped)
                    or re_action_map_lower.get(const_id_stripped)
                    or []
                )

                # debug_info = { ... }  # uncomment debug_rows lines to re-enable

                if upload_date is None:
                    no_matching_rows.append(row)
                    continue

                matching = []
                for a in candidates:
                    delta = (upload_date - a['action_date']).days
                    if 0 <= delta <= 45:
                        matching.append(a)

                if not matching:
                    no_matching_rows.append(row)
                    continue

                fmt_time  = fmt_date(time_raw)
                note_text = f"RKD Connect thank you call result - {result_val}"
                if call_notes_val:
                    note_text += f"; {call_notes_val}"

                for a in matching:
                    action_date_rows.append({
                        'ConsID':         const_id,
                        'ACImpID':        a['import_id'],
                        'ACDate':         fmt_time,
                        'ACComplete':     'TRUE',
                        'ACCompleteDate': fmt_time,
                    })
                    action_note_rows.append({
                        'CALink':      a['import_id'],
                        'CANoteImpID': a['note_import_id'],
                        'CANoteDate':  fmt_time,
                        'CANoteType':  'Phone Call',
                        'CANoteNotes': note_text,
                        'CANoteDesc':  re.sub(r'(?i)\\bReceiving\\b', 'Received', a['description']),
                    })

            df_no_matching = pd.DataFrame(no_matching_rows)
            df_action_date = pd.DataFrame(action_date_rows)
            df_action_note = pd.DataFrame(action_note_rows)
            # df_debug     = pd.DataFrame(debug_rows)  # uncomment to re-enable debug panel


            # ── Exceptions workbook ──────────────────────────────────────
            exc_buf     = io.BytesIO()
            placeholder = pd.DataFrame(columns=['(no records)'])
            with pd.ExcelWriter(exc_buf, engine='openpyxl') as writer:
                (df_phone_in_re       if not df_phone_in_re.empty       else placeholder).to_excel(writer, sheet_name='phone_in_re',        index=False)
                (df_phone_type_exists if not df_phone_type_exists.empty else placeholder).to_excel(writer, sheet_name='phone_type_exists',  index=False)
                (df_no_matching       if not df_no_matching.empty       else placeholder).to_excel(writer, sheet_name='no_matching_actions', index=False)
            exc_buf.seek(0)

            phone_csv       = df_to_csv_bytes(df_output_phones)
            action_date_csv = df_to_csv_bytes(df_action_date)
            action_note_csv = df_to_csv_bytes(df_action_note)

            # ── Results summary ──────────────────────────────────────────
            st.success("✅ Processing complete!")
            cols = st.columns(6)
            metrics = [
                ("Phones already in RE",  len(df_phone_in_re)),
                ("Phone type conflicts",  len(df_phone_type_exists)),
                ("Phones to import",      len(df_output_phones)),
                ("No matching actions",   len(df_no_matching)),
                ("Action date rows",      len(df_action_date)),
                ("Action note rows",      len(df_action_note)),
            ]
            for col, (label, val) in zip(cols, metrics):
                col.metric(label, val)

            # ── Pack all four outputs into a zip for single download ─────
            import zipfile
            zip_buf = io.BytesIO()
            with zipfile.ZipFile(zip_buf, 'w', zipfile.ZIP_DEFLATED) as zf:
                zf.writestr(f"{month_label} RKD Connect Exceptions.xlsx",      exc_buf.getvalue())
                zf.writestr(f"{month_label} RKD Connect Phone Import.csv",      phone_csv)
                zf.writestr(f"{month_label} RKD Connect Action Date Import.csv", action_date_csv)
                zf.writestr(f"{month_label} RKD Connect Action Note Import.csv", action_note_csv)
            zip_buf.seek(0)

            st.divider()
            _, center, _ = st.columns([1, 2, 1])
            with center:
                st.download_button(
                    "📦 Download All Outputs (.zip)",
                    data=zip_buf,
                    file_name=f"{month_label} RKD Connect Outputs.zip",
                    mime="application/zip",
                    use_container_width=True,
                    type="primary",
                )

            st.divider()
            st.subheader("Data Previews")
            tab_labels = [
                "Phone Import", "Action Date Import", "Action Note Import",
                "Exc: phone_in_re", "Exc: phone_type_exists", "Exc: no_matching_actions",
            ]
            tab_dfs = [df_output_phones, df_action_date, df_action_note,
                       df_phone_in_re, df_phone_type_exists, df_no_matching]

            for tab, df in zip(st.tabs(tab_labels), tab_dfs):
                with tab:
                    if df.empty:
                        st.info("No records in this output.")
                    else:
                        st.dataframe(df, use_container_width=True)

            # ── Debug section (commented out) ────────────────────────────
            # with st.expander("🔍 Action Matching Debug (expand to diagnose issues)"):
            #     if df_debug.empty:
            #         st.info("No upload rows processed.")
            #     else:
            #         st.caption("Shows every row from the data upload file and why it matched or failed.")
            #         st.dataframe(df_debug, use_container_width=True)
            #         st.caption(f"RE action map contains **{len(re_actions_map)}** unique IDs. Sample keys: `{list(re_actions_map.keys())[:10]}`")
            #         st.caption(f"CnBio_ID column resolved to: `{cnbio_col}`")
            #         st.caption(f"Action groups found: {len(act_groups)} — columns: {[g[0] for g in act_groups]}")
            #         st.caption(f"df_re columns (first 10): {list(df_re.columns[:10])}")
            #         st.caption(f"df_upload 'Constituent ID' sample values: {list(df_upload['Constituent ID'].dropna().head(5))}")

        except Exception as e:
            st.error(f"An error occurred: {e}")
            st.exception(e)
