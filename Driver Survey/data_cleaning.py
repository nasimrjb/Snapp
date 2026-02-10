import pandas as pd

# ============================================================
# Paths
# ============================================================

INPUT_RAW_DATA_PATH = r"D:\OneDrive\Work\Driver Survey\DataSources\Raw_Data.xlsx"
INPUT_COLUMN_RENAME_PATH = r"D:\OneDrive\Work\Driver Survey\DataSources\column_rename.xlsx"

OUTPUT_CLEAN_DATA_PATH = r"D:\OneDrive\Work\Driver Survey\Outputs\cleaned_survey.xlsx"
OUTPUT_ERROR_REPORT_PATH = r"D:\OneDrive\Work\Driver Survey\Outputs\survey_errors.xlsx"
OUTPUT_CODEBOOK_PATH = r"D:\OneDrive\Work\Driver Survey\Outputs\codebook.xlsx"

SHEET_QUESTIONS = "questions"
SHEET_REPLACED_ANSWERS = "replaced_answers"

# ============================================================
# Helpers
# ============================================================

PERSIAN_DIGITS = str.maketrans("۰۱۲۳۴۵۶۷۸۹", "0123456789")
MULTI_SELECT_DELIMITERS = [";", "؛"]


def normalize_text(val) -> str:
    if pd.isna(val):
        return ""
    text = str(val).translate(PERSIAN_DIGITS)
    text = text.replace("\r\n", " ").replace("\n", " ").replace("\r", " ")
    return " ".join(text.split()).strip()


def split_multi_select(value: str):
    if not value.strip():
        return []
    parts, buf, stack = [], "", 0
    for ch in value:
        if ch in "([{":
            stack += 1
        elif ch in ")]}" and stack > 0:
            stack -= 1
        if ch in MULTI_SELECT_DELIMITERS and stack == 0:
            parts.append(buf.strip())
            buf = ""
        else:
            buf += ch
    if buf.strip():
        parts.append(buf.strip())
    return parts


def smart_numeric_cast(series: pd.Series) -> pd.Series:
    normalized = series.apply(
        lambda x: normalize_text(x) if isinstance(x, str) else x)
    numeric = pd.to_numeric(normalized, errors="coerce")
    non_empty = normalized != ""
    if non_empty.any() and numeric[non_empty].notna().all():
        return numeric
    return series

# ============================================================
# Load data (preserve datetime as-is)
# ============================================================


raw_df = pd.read_excel(INPUT_RAW_DATA_PATH, dtype=str)
questions_df = pd.read_excel(
    INPUT_COLUMN_RENAME_PATH,
    sheet_name=SHEET_QUESTIONS,
    header=None,
    dtype=str
).fillna("")
replaced_answers_df = pd.read_excel(
    INPUT_COLUMN_RENAME_PATH,
    sheet_name=SHEET_REPLACED_ANSWERS,
    header=None,
    dtype=str
).fillna("")

raw_df = raw_df.fillna("")

error_rows = []
codebook_rows = []

# ============================================================
# Header validation
# ============================================================

raw_headers = raw_df.columns.tolist()
expected_headers = questions_df.iloc[0].tolist()

if len(raw_headers) != len(expected_headers):
    raise ValueError(
        "❌ Column count mismatch between raw data and questions sheet")

for i, (raw_h, exp_h) in enumerate(zip(raw_headers, expected_headers)):
    if normalize_text(raw_h) != normalize_text(exp_h):
        error_rows.append({
            "error_type": "header_mismatch",
            "column_index": i + 1,
            "raw_header": raw_h,
            "expected_header": exp_h
        })

# ============================================================
# Rename columns
# ============================================================

raw_df.columns = questions_df.iloc[1].tolist()

# ============================================================
# Column processing
# ============================================================

for col_idx, col_name in enumerate(raw_df.columns):

    # ---------- DATETIME: keep exactly as-is ----------
    if col_name.lower() == "datetime":
        codebook_rows.append({
            "column_name": col_name,
            "question_text": normalize_text(expected_headers[col_idx]),
            "allowed_answers": "datetime",
            "replaced_answers": "datetime"
        })
        continue  # Skip all further processing for datetime

    rule = normalize_text(questions_df.iloc[2, col_idx]).lower()

    # ---------- IGNORE ----------
    if col_name.lower().startswith("ignore"):
        raw_df[col_name] = ""
        continue

    # ---------- CUSTOMIZED ANSWERS ----------
    if rule == "customized_answer":
        codebook_rows.append({
            "column_name": col_name,
            "question_text": normalize_text(expected_headers[col_idx]),
            "allowed_answers": "customized",
            "replaced_answers": "customized"
        })
        continue

    # ---------- NORMAL QUESTIONS ----------
    allowed_raw = [
        normalize_text(x)
        for x in questions_df.iloc[2:, col_idx]
        if normalize_text(x)
    ]

    replaced_vals = [
        normalize_text(x)
        for x in replaced_answers_df.iloc[2:, col_idx]
        if normalize_text(x)
    ]

    replace_map = dict(zip(allowed_raw, replaced_vals))

    cleaned_col = []

    for row_idx, raw_val in enumerate(raw_df[col_name]):
        raw_val = normalize_text(raw_val)
        if not raw_val:
            cleaned_col.append("")
            continue

        parts = split_multi_select(raw_val)
        replaced_parts = []

        for part in parts:
            if part not in replace_map:
                error_rows.append({
                    "error_type": "invalid_answer",
                    "column": col_name,
                    "row": row_idx + 2,
                    "raw_value": part,
                    "allowed": ",,".join(allowed_raw)
                })
                replaced_parts.append(part)
            else:
                replaced_parts.append(replace_map[part])

        cleaned_col.append(";".join(replaced_parts))

    # Update column values
    raw_df[col_name] = cleaned_col

    # Numeric conversion (except datetime, already skipped)
    raw_df[col_name] = smart_numeric_cast(raw_df[col_name])

    # Codebook
    codebook_rows.append({
        "column_name": col_name,
        "question_text": normalize_text(expected_headers[col_idx]),
        "allowed_answers": ",,".join(allowed_raw),
        "replaced_answers": ",,".join(replaced_vals)
    })

# ============================================================
# Final output
# ============================================================

final_df = raw_df[[
    c for c in raw_df.columns if not c.lower().startswith("ignore")]]

final_df.to_excel(OUTPUT_CLEAN_DATA_PATH, index=False)
pd.DataFrame(codebook_rows).to_excel(OUTPUT_CODEBOOK_PATH, index=False)

if error_rows:
    pd.DataFrame(error_rows).to_excel(OUTPUT_ERROR_REPORT_PATH, index=False)

print("✅ Survey cleaning completed — datetime preserved, numbers numeric, customized answers safe")
