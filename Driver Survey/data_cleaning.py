import pandas as pd

# ============================================================
# Paths
# ============================================================

INPUT_RAW_DATA_PATH = r"D:\OneDrive\Work\Driver Survey\DataSources\combined_raw.xlsx"
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


def is_checkbox_column(allowed_raw: list, col_name: str) -> bool:
    """
    Detect checkbox-style columns where the survey tool writes the column name
    (or question label) as the cell value when the box is ticked, and empty
    when unticked.  We treat a column as a checkbox when:
      - there are no allowed answers defined at all, OR
      - the only allowed answer defined is the column name itself.
    """
    return len(allowed_raw) == 0 or (
        len(allowed_raw) == 1 and allowed_raw[0] == col_name
    )


# ============================================================
# Load data
# ============================================================

raw_df = pd.read_excel(INPUT_RAW_DATA_PATH, dtype=str).fillna("")

# Data is stored column-wise in the rename file — transpose after loading
questions_df = pd.read_excel(
    INPUT_COLUMN_RENAME_PATH,
    sheet_name=SHEET_QUESTIONS,
    header=None,
    dtype=str,
).fillna("").T.reset_index(drop=True)

replaced_answers_df = pd.read_excel(
    INPUT_COLUMN_RENAME_PATH,
    sheet_name=SHEET_REPLACED_ANSWERS,
    header=None,
    dtype=str,
).fillna("").T.reset_index(drop=True)

error_rows = []
codebook_rows = []

# ============================================================
# Build Mapping (match by normalized text)
# ============================================================

expected_headers = questions_df.iloc[0].tolist()
rename_row = questions_df.iloc[1].tolist()

normalized_expected = {
    normalize_text(q): idx
    for idx, q in enumerate(expected_headers)
    if normalize_text(q)
}

raw_headers = raw_df.columns.tolist()

normalized_raw = {col: normalize_text(col) for col in raw_headers}

# Only validate raw columns that do NOT exist in mapping
unmapped_raw_columns = [
    col for col, norm in normalized_raw.items()
    if norm not in normalized_expected
]

if unmapped_raw_columns:
    pd.DataFrame({"unmapped_raw_columns": unmapped_raw_columns}).to_excel(
        OUTPUT_MISSING_REPORT_PATH, index=False
    )
    print("❌ Raw data contains columns not defined in column_rename.xlsx")
    print(f"📄 Unmapped columns saved to: {OUTPUT_MISSING_REPORT_PATH}")
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

    # ----------------------------------------------------------
    # Datetime columns — pass through as-is
    # ----------------------------------------------------------
    if col_name.lower() == "datetime":
        codebook_rows.append({
            "column_name": col_name,
            "question_text": normalize_text(expected_headers[col_idx]),
            "allowed_answers": "datetime",
            "replaced_answers": "datetime",
        })
        continue

    rule = normalize_text(questions_df.iloc[2, col_idx]).lower()

    # ----------------------------------------------------------
    # Ignored columns — blank out and skip
    # ----------------------------------------------------------
    if col_name.lower().startswith("ignore"):
        raw_df[col_name] = ""
        continue

    # ----------------------------------------------------------
    # Free-text / customized columns — no validation needed
    # ----------------------------------------------------------
    if rule == "customized_answer":
        codebook_rows.append({
            "column_name": col_name,
            "question_text": normalize_text(expected_headers[col_idx]),
            "allowed_answers": "customized",
            "replaced_answers": "customized",
        })
        continue

    # ----------------------------------------------------------
    # Build allowed answers and replacement map
    # ----------------------------------------------------------
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

    # Deduplicate to avoid zip misalignment (Bug 2 fix)
    seen = {}
    for raw_ans, rep_ans in zip(allowed_raw, replaced_vals):
        if raw_ans not in seen:
            seen[raw_ans] = rep_ans
        elif seen[raw_ans] != rep_ans:
            print(
                f"⚠️  Duplicate answer '{raw_ans}' in column '{col_name}' "
                f"with conflicting replacements (keeping first). "
                f"Please fix column_rename.xlsx."
            )
    replace_map = seen

    # ----------------------------------------------------------
    # Checkbox column detection (Bug 1 fix)
    # Checkbox columns: the survey tool writes the column name
    # as the cell value when ticked, and empty when unticked.
    # We convert ticked → "1" and unticked → "0".
    # ----------------------------------------------------------
    checkbox_col = is_checkbox_column(allowed_raw, col_name)

    cleaned_col = []

    for row_idx, raw_val in enumerate(raw_df[col_name]):
        raw_val = normalize_text(raw_val)

        # Empty cell
        if not raw_val:
            cleaned_col.append("0" if checkbox_col else "")
            continue

        # --- Checkbox column ---
        if checkbox_col:
            # Any non-empty value (typically the column name itself) = ticked
            cleaned_col.append("1")
            continue

        # --- Regular single/multi-select column ---
        parts = split_multi_select(raw_val)
        replaced_parts = []

        for part in parts:
            if part not in replace_map:
                error_rows.append({
                    "error_type": "invalid_answer",
                    "column": col_name,
                    "row": row_idx + 2,
                    "raw_value": part,
                    "allowed": ",,".join(allowed_raw),
                })
                replaced_parts.append(part)
            else:
                replaced_parts.append(replace_map[part])

        cleaned_col.append(";".join(replaced_parts))

    raw_df[col_name] = smart_numeric_cast(pd.Series(cleaned_col))

    codebook_rows.append({
        "column_name": col_name,
        "question_text": normalize_text(expected_headers[col_idx]),
        "allowed_answers": "checkbox (0/1)" if checkbox_col else ",,".join(allowed_raw),
        "replaced_answers": "checkbox (0/1)" if checkbox_col else ",,".join(replaced_vals),
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
    print(
        f"⚠️  {len(error_rows)} validation error(s) written to: {OUTPUT_ERROR_REPORT_PATH}")
else:
    print("✅ No validation errors found.")

print("✅ Survey cleaning completed.")
