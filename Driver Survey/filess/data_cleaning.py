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

ZWNJ = "\u200c"
ZWJ = "\u200d"
ZWSP = "\u200b"
BOM = "\ufeff"
ARABIC_KAF = "\u0643"
PERSIAN_KAF = "\u06a9"
ARABIC_YEH = "\u064a"
PERSIAN_YEH = "\u06cc"


def normalize_text(val) -> str:
    if pd.isna(val):
        return ""
    text = str(val).translate(PERSIAN_DIGITS)
    text = text.replace("\r\n", " ").replace("\n", " ").replace("\r", " ")
    text = text.replace(ZWNJ, "").replace(
        ZWJ, "").replace(ZWSP, "").replace(BOM, "")
    text = text.replace(ARABIC_KAF, PERSIAN_KAF).replace(
        ARABIC_YEH, PERSIAN_YEH)
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


def build_allowed_raw(questions_df, col_idx, col_name):
    """
    Build a deduplicated, order-preserved list of allowed answers for a column.
    Strips any entry that equals the column name itself.
    """
    seen = {}
    for x in questions_df.iloc[2:, col_idx]:
        norm_x = normalize_text(x)
        if norm_x and norm_x not in seen and norm_x != col_name:
            seen[norm_x] = True
    return list(seen.keys())


def is_checkbox_column(
    allowed_raw: list,
    col_name: str,
    original_question: str = "",
    row2_text: str = "",
) -> bool:
    """
    A column is a checkbox (0/1) when — after stripping the col_name —
    the remaining allowed answers contain only "echo" values that the survey
    tool writes automatically:
      • nothing at all (empty allowed list), OR
      • the only entry is the full Persian question text (row 0), OR
      • the only entry is the Persian sub-question fragment (row 2 text),
        i.e. the text that the survey tool copies into the cell when ticked.

    This covers the common pattern where column_rename.xlsx row 2 holds the
    Persian answer label rather than being empty, causing is_checkbox_column
    to incorrectly return False with the old single-condition check.
    """
    # Collect all values that are *not* just echoing structural text
    meaningful = [
        a for a in allowed_raw
        if a != col_name
        and a != original_question
        and a != row2_text          # <-- KEY FIX: ignore the row-2 echo text
    ]
    return len(meaningful) == 0


# ============================================================
# Load data
# ============================================================

raw_df = pd.read_excel(INPUT_RAW_DATA_PATH, dtype=str).fillna("")

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
# Build replaced_answers vlookup index (by normalized question header)
# ============================================================

replaced_headers = replaced_answers_df.iloc[0].tolist()
normalized_replaced_headers = {
    normalize_text(h): idx
    for idx, h in enumerate(replaced_headers)
    if normalize_text(h)
}

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
    # Build allowed answers: deduplicated + col_name entries stripped
    # ----------------------------------------------------------
    allowed_raw = build_allowed_raw(questions_df, col_idx, col_name)

    # ----------------------------------------------------------
    # Build replaced_vals via vlookup-style match on question header
    # ----------------------------------------------------------
    question_key = normalize_text(expected_headers[col_idx])
    rep_col_idx = normalized_replaced_headers.get(question_key)

    if rep_col_idx is not None:
        replaced_vals = [
            normalize_text(x)
            for x in replaced_answers_df.iloc[2:, rep_col_idx]
            if normalize_text(x) and normalize_text(x) != col_name
        ]
        # Deduplicate replaced_vals in the same order
        seen_rep = {}
        for v in replaced_vals:
            if v not in seen_rep:
                seen_rep[v] = True
        replaced_vals = list(seen_rep.keys())
    else:
        replaced_vals = allowed_raw
        print(
            f"⚠️  Column '{col_name}' not found in replaced_answers sheet — using raw values.")

    # ----------------------------------------------------------
    # Build replace_map
    # ----------------------------------------------------------
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
    # Checkbox column detection
    # ----------------------------------------------------------
    original_question = normalize_text(expected_headers[col_idx])
    # row2_text is the Persian sub-question fragment the survey tool echoes
    # into the cell when a checkbox is ticked — must be excluded from the
    # "meaningful answers" count when deciding if a column is a checkbox.
    row2_text = normalize_text(questions_df.iloc[2, col_idx])

    checkbox_col = is_checkbox_column(
        allowed_raw, col_name, original_question, row2_text
    )

    cleaned_col = []

    for row_idx, raw_val in enumerate(raw_df[col_name]):
        raw_val = normalize_text(raw_val)

        if not raw_val:
            cleaned_col.append("0" if checkbox_col else "")
            continue

        if checkbox_col:
            cleaned_col.append("1")
            continue

        parts = split_multi_select(raw_val)
        replaced_parts = []

        for part in parts:
            part = normalize_text(part)
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
