import pandas as pd

# ============================================================
# Paths
# ============================================================

INPUT_RAW_DATA_PATH = r"D:\OneDrive\Work\Driver Survey\DataSources\Data Raw Driver 2608.xlsx"
INPUT_COLUMN_RENAME_PATH = r"D:\OneDrive\Work\Driver Survey\DataSources\column_rename.xlsx"

OUTPUT_CLEAN_DATA_PATH = r"D:\OneDrive\Work\Driver Survey\Outputs\cleaned_survey.xlsx"
OUTPUT_ERROR_REPORT_PATH = r"D:\OneDrive\Work\Driver Survey\Outputs\survey_errors.xlsx"
OUTPUT_CODEBOOK_PATH = r"D:\OneDrive\Work\Driver Survey\Outputs\codebook.xlsx"
OUTPUT_MISSING_REPORT_PATH = r"D:\OneDrive\Work\Driver Survey\Outputs\unmapped_raw_columns.xlsx"

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
        lambda x: normalize_text(x) if isinstance(x, str) else x
    )
    numeric = pd.to_numeric(normalized, errors="coerce")
    non_empty = normalized != ""
    if non_empty.any() and numeric[non_empty].notna().all():
        return numeric
    return series


# ============================================================
# Load data
# ============================================================

raw_df = pd.read_excel(INPUT_RAW_DATA_PATH, dtype=str).fillna("")

# Data is now in columns instead of rows — transpose after loading
questions_df = pd.read_excel(
    INPUT_COLUMN_RENAME_PATH,
    sheet_name=SHEET_QUESTIONS,
    header=None,
    dtype=str
).fillna("").T.reset_index(drop=True)

replaced_answers_df = pd.read_excel(
    INPUT_COLUMN_RENAME_PATH,
    sheet_name=SHEET_REPLACED_ANSWERS,
    header=None,
    dtype=str
).fillna("").T.reset_index(drop=True)

error_rows = []
codebook_rows = []

# ============================================================
# Build Mapping (MATCH BY TEXT)
# ============================================================

expected_headers = questions_df.iloc[0].tolist()
rename_row = questions_df.iloc[1].tolist()

normalized_expected = {
    normalize_text(q): idx
    for idx, q in enumerate(expected_headers)
    if normalize_text(q)
}

raw_headers = raw_df.columns.tolist()

normalized_raw = {
    col: normalize_text(col)
    for col in raw_headers
}

# 🚨 Only validate raw columns that do NOT exist in mapping
unmapped_raw_columns = [
    col for col, norm in normalized_raw.items()
    if norm not in normalized_expected
]

if unmapped_raw_columns:
    pd.DataFrame({
        "unmapped_raw_columns": unmapped_raw_columns
    }).to_excel(OUTPUT_MISSING_REPORT_PATH, index=False)

    print("❌ Raw data contains columns not defined in column_rename.xlsx")
    print(f"📄 File saved to: {OUTPUT_MISSING_REPORT_PATH}")
    raise SystemExit

# ============================================================
# Rename Using Mapping
# ============================================================

new_column_names = {}

for raw_col in raw_headers:
    norm = normalized_raw[raw_col]
    idx = normalized_expected[norm]
    new_column_names[raw_col] = rename_row[idx]

raw_df = raw_df.rename(columns=new_column_names).copy()

# ============================================================
# Process Columns
# ============================================================

for raw_original_col in raw_headers:

    norm = normalized_raw[raw_original_col]
    col_idx = normalized_expected[norm]
    col_name = rename_row[col_idx]

    if col_name.lower() == "datetime":
        codebook_rows.append({
            "column_name": col_name,
            "question_text": normalize_text(expected_headers[col_idx]),
            "allowed_answers": "datetime",
            "replaced_answers": "datetime"
        })
        continue

    rule = normalize_text(questions_df.iloc[2, col_idx]).lower()

    if col_name.lower().startswith("ignore"):
        raw_df[col_name] = ""
        continue

    if rule == "customized_answer":
        codebook_rows.append({
            "column_name": col_name,
            "question_text": normalize_text(expected_headers[col_idx]),
            "allowed_answers": "customized",
            "replaced_answers": "customized"
        })
        continue

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

    raw_df[col_name] = smart_numeric_cast(pd.Series(cleaned_col))

    codebook_rows.append({
        "column_name": col_name,
        "question_text": normalize_text(expected_headers[col_idx]),
        "allowed_answers": ",,".join(allowed_raw),
        "replaced_answers": ",,".join(replaced_vals)
    })

# ============================================================
# Final Output
# ============================================================

final_df = raw_df[
    [c for c in raw_df.columns if not c.lower().startswith("ignore")]
]

final_df.to_excel(OUTPUT_CLEAN_DATA_PATH, index=False)
pd.DataFrame(codebook_rows).to_excel(OUTPUT_CODEBOOK_PATH, index=False)

if error_rows:
    pd.DataFrame(error_rows).to_excel(OUTPUT_ERROR_REPORT_PATH, index=False)

print("✅ Survey cleaning completed — raw columns validated only")
