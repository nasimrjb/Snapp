"""
Driver Survey Data Processing Pipeline
=======================================
Reads raw weekly survey Excel files, validates columns against a JSON mapping,
renames headers, recodes answers, and produces:
  - short_survey.csv  → meta + single-choice questions
  - wide_survey.csv   → meta + multi-choice questions (binary 0/1 columns)
  - long_survey.csv   → meta + multi-choice questions (melted/stacked rows)

If unmapped columns are found, outputs unmapped_columns.csv and exits early.
If unmapped answers are found in single/multi questions, outputs
unmapped_answers.csv and exits early.

Column exclusion rules:
  - "customized_answer": "customized_answer" → excluded from all outputs.
  - type == "other" → excluded from all outputs (free-text "other" fields).

A computed "weeknumber" column is added based on the datetime column:
  If weekday is Saturday (dayofweek==5), weeknumber = ISO week + 1,
  else weeknumber = ISO week.

Usage:
    python process_surveys.py
"""

import os
import sys
import json
import glob
import re
import unicodedata
import pandas as pd
from collections import defaultdict

# ============================================================
# CONFIGURATION
# ============================================================
RAW_DIR = r"D:\OneDrive\Work\Driver Survey\raw"
MAPPING_PATH = r"D:\OneDrive\Work\Driver Survey\DataSources\column_rename_mapping.json"
OUTPUT_DIR = r"D:\OneDrive\Work\Driver Survey\processed"


# ============================================================
# Helpers
# ============================================================

def normalize(text):
    if not isinstance(text, str):
        return str(text)
    return unicodedata.normalize("NFKC", text).strip()


# Digit translators: Persian ۰-۹ and Arabic ٠-٩ → ASCII 0-9
_PERSIAN_TO_ASCII = str.maketrans("۰۱۲۳۴۵۶۷۸۹", "0123456789")
_ARABIC_TO_ASCII = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")


def fuzzy_normalize(text):
    """
    Aggressive normalization for answer comparison.
    Handles: NFKC, Persian/Arabic digits → ASCII, zero-width chars (ZWNJ etc.)
    replaced with space, diacritics stripped, punctuation removed, whitespace
    collapsed, lowercased.
    """
    if not isinstance(text, str):
        text = str(text)
    text = unicodedata.normalize("NFKC", text)
    # Persian / Arabic digits → ASCII
    text = text.translate(_PERSIAN_TO_ASCII)
    text = text.translate(_ARABIC_TO_ASCII)
    # Replace zero-width / formatting chars (Cf) and diacritics (Mn) with space
    text = "".join(
        c if unicodedata.category(c) not in ("Cf", "Mn") else " "
        for c in text
    )
    # Remove punctuation and symbols (keep letters, digits, spaces)
    text = re.sub(r"[^\w\s]", "", text, flags=re.UNICODE)
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text.lower()


def load_mapping(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def is_customized_answer(meta):
    """Return True if this mapping entry has customized_answer set."""
    answers = meta.get("answers")
    if isinstance(answers, dict):
        return (answers.get("customized_answer") == "customized_answer")
    return False


def build_raw_to_key(mapping):
    raw_to_key = {}
    for key, meta in mapping.items():
        for raw_header in meta.get("raw", []):
            raw_to_key[normalize(raw_header)] = key
    return raw_to_key


def compute_weeknumber(dt_series):
    """
    Compute week number from a datetime series.
    Replicates the Excel formula:
        IF(WEEKDAY(date)=7, WEEKNUM(date)+1, WEEKNUM(date))

    Excel WEEKDAY (type 1): Sunday=1, Monday=2, ... Saturday=7
    Excel WEEKNUM (type 1): Week containing Jan 1 is week 1, weeks start Sunday.

    pandas dayofweek: Monday=0 ... Sunday=6  →  Saturday=5
    """
    dt = pd.to_datetime(dt_series, errors="coerce")

    # Day of year (1-based)
    doy = dt.dt.dayofyear

    # What weekday is Jan 1 of each year? (Sunday=0 basis)
    jan1 = pd.to_datetime(dt.dt.year.astype(str) + "-01-01", errors="coerce")
    # pandas dayofweek: Mon=0..Sun=6 → convert to Sun=0..Sat=6
    jan1_wday_sun = (jan1.dt.dayofweek + 1) % 7

    # Excel WEEKNUM (type 1, Sunday start): week containing Jan 1 is week 1
    weeknum = (doy + jan1_wday_sun - 1) // 7 + 1

    # Saturday (dayofweek=5) or Sunday (dayofweek=6) → week + 1
    is_weekend = dt.dt.dayofweek.isin([5, 6])

    weeknumber = weeknum.where(~is_weekend, weeknum + 1)

    return weeknumber.astype("Int64")


def load_all_raw_files(raw_dir):
    patterns = ["*.xlsx", "*.xls", "*.csv"]
    files = []
    for pat in patterns:
        files.extend(glob.glob(os.path.join(raw_dir, pat)))
    files = sorted(set(files))

    if not files:
        print(f"ERROR: No data files found in {raw_dir}")
        sys.exit(1)

    print(f"Found {len(files)} raw file(s)")

    frames = []
    col_unique_vals = defaultdict(set)
    col_file_count = defaultdict(int)

    for fpath in files:
        fname = os.path.basename(fpath)
        print(f"  Reading: {fname}")

        if fpath.endswith(".csv"):
            df = pd.read_csv(fpath, dtype=str)
        else:
            df = pd.read_excel(fpath, dtype=str)

        norm_cols = [normalize(c) for c in df.columns]
        df.columns = norm_cols
        df["_source_file"] = fname

        for col in norm_cols:
            col_file_count[col] += 1
            col_unique_vals[col].update(df[col].dropna().unique())

        frames.append(df)

    combined = pd.concat(frames, ignore_index=True)
    print(f"Total rows: {len(combined)}")

    return combined, col_unique_vals, col_file_count


# ============================================================
# Validation
# ============================================================

def find_unmapped_answers(col_unique_vals, mapping, raw_to_key):
    """
    For single and multi-choice questions, check that every unique raw
    answer found in the data has a corresponding entry in the mapping's
    answers dict.  Returns a list of dicts describing each mismatch.
    """
    unmapped = []

    for norm_col, unique_vals in col_unique_vals.items():
        if norm_col not in raw_to_key:
            continue
        key = raw_to_key[norm_col]
        meta = mapping.get(key)
        if not meta:
            continue

        qtype = meta.get("type")
        if qtype not in ("single", "multi"):
            continue

        # Skip columns that would be excluded during processing
        if is_customized_answer(meta):
            continue
        if meta.get("type") == "other":
            continue

        answers = meta.get("answers")
        if not answers:
            continue

        # Fuzzy-normalize the expected answer keys for comparison
        fuzzy_expected = {fuzzy_normalize(k) for k in answers.keys()}

        for raw_val in sorted(unique_vals):
            if fuzzy_normalize(raw_val) not in fuzzy_expected:
                unmapped.append({
                    "mapping_key": key,
                    "question_type": qtype,
                    "long_name": meta.get("long", ""),
                    "unmapped_raw_answer": raw_val,
                })

    return unmapped


def find_unmapped_columns(col_unique_vals, col_file_count, raw_to_key):
    unmapped = []

    for col in sorted(col_unique_vals.keys()):
        if col == "_source_file":
            continue
        if col not in raw_to_key:
            answers = col_unique_vals[col]
            unmapped.append({
                "raw_column_header": col,
                "unique_answers_sample": " | ".join(sorted(answers)[:30]),
                "num_unique_answers": len(answers),
                "num_files_appeared": col_file_count[col],
            })

    return unmapped


# ============================================================
# Processing
# ============================================================

def process_data(combined, mapping, raw_to_key):

    # Build set of keys to skip:
    #   - customized_answer columns
    #   - "other" type columns
    skip_keys = set()
    for key, meta in mapping.items():
        if is_customized_answer(meta):
            skip_keys.add(key)
        if meta.get("type") == "other":
            skip_keys.add(key)

    if skip_keys:
        print(f"\nSkipping {len(skip_keys)} column(s) "
              f"(customized_answer + other type)")

    # Identify present mapping keys, excluding skipped ones
    present_keys = {}
    for col in combined.columns:
        if col in raw_to_key:
            key = raw_to_key[col]
            if key not in skip_keys:
                present_keys[key] = col

    # Classify by type
    meta_keys = []
    single_keys = []
    multi_keys = []

    for key, col in present_keys.items():
        qtype = mapping[key]["type"]
        if qtype == "meta":
            if not key.startswith("ignore"):
                meta_keys.append(key)
        elif qtype == "single":
            single_keys.append(key)
        elif qtype == "multi":
            multi_keys.append(key)

    print("\nColumn classification (after exclusions):")
    print(f"  meta:   {len(meta_keys)}")
    print(f"  single: {len(single_keys)}")
    print(f"  multi:  {len(multi_keys)}")
    print(f"  other:  (excluded)")

    # ============================================================
    # BUILD META COLUMNS (shared across all outputs)
    # ============================================================

    meta_dict = {}
    for key in meta_keys:
        meta_dict[key] = combined[present_keys[key]].copy()
    meta_dict["_source_file"] = combined["_source_file"]

    # Add computed weeknumber from datetime
    if "datetime" in meta_dict:
        meta_dict["weeknumber"] = compute_weeknumber(meta_dict["datetime"])
    else:
        print("WARNING: 'datetime' column not found — weeknumber not computed")

    meta_df = pd.DataFrame(meta_dict)

    # ============================================================
    # SHORT SURVEY → meta + single-choice (recoded)
    # ============================================================

    single_dict = {}
    for key in single_keys:
        col = combined[present_keys[key]].copy()
        answers = mapping[key].get("answers")

        if answers:
            fuzzy_answers = {fuzzy_normalize(k): v for k, v in answers.items()}
            col = col.map(lambda x: fuzzy_answers.get(
                fuzzy_normalize(x), x) if pd.notna(x) else x)

        single_dict[key] = col

    short_df = pd.concat([meta_df, pd.DataFrame(single_dict)], axis=1)

    # ============================================================
    # DATA VALIDATION — drop invalid rows
    # ============================================================
    # Drop rows where tapsi_age == 'Not Registered' but
    # tapsi_trip_count != '0' (contradictory response)

    n_before = len(short_df)
    valid_mask = pd.Series(True, index=short_df.index)

    if "tapsi_age" in short_df.columns and "tapsi_trip_count" in short_df.columns:
        invalid = (
            (short_df["tapsi_age"] == "Not Registered") &
            (short_df["tapsi_trip_count"] != "0")
        )
        valid_mask &= ~invalid

    combined = combined.loc[valid_mask].reset_index(drop=True)
    meta_df = meta_df.loc[valid_mask].reset_index(drop=True)
    short_df = short_df.loc[valid_mask].reset_index(drop=True)

    n_dropped = n_before - len(short_df)
    if n_dropped:
        print(f"\nDropped {n_dropped} invalid row(s) "
              f"(tapsi_age='Not Registered' with tapsi_trip_count != '0')")

    print("\nShort shape (meta + single):", short_df.shape)

    # ============================================================
    # MULTI GROUPING
    # ============================================================

    multi_groups = defaultdict(list)

    for key in multi_keys:
        long_title = mapping[key]["long"]
        answers = mapping[key].get("answers", {})
        answer_value = list(answers.values())[0] if answers else key
        multi_groups[long_title].append((key, present_keys[key], answer_value))

    # ============================================================
    # WIDE SURVEY → meta + multi-choice (binary columns)
    # ============================================================

    multi_columns = {}

    for long_title, options in multi_groups.items():
        for key, norm_col, answer_value in options:
            col_name = f"{long_title}__{answer_value}"
            multi_columns[col_name] = (
                combined[norm_col]
                .notna()
                .astype(int)
            )

    if multi_columns:
        wide_df = pd.concat(
            [meta_df, pd.DataFrame(multi_columns)], axis=1)
    else:
        wide_df = meta_df.copy()

    print("Wide shape (meta + multi binary):", wide_df.shape)

    # ============================================================
    # LONG SURVEY → meta + multi-choice (melted rows)
    # ============================================================

    long_rows = []

    for idx in combined.index:
        meta_row = meta_df.loc[idx].to_dict()

        for long_title, options in multi_groups.items():
            for key, norm_col, answer_value in options:
                if pd.notna(combined.at[idx, norm_col]):
                    row = meta_row.copy()
                    row["question"] = long_title
                    row["answer"] = answer_value
                    row["question_type"] = "multi_choice"
                    long_rows.append(row)

    long_df = pd.DataFrame(long_rows)
    print("Long shape (meta + multi melted):", long_df.shape)

    return short_df, wide_df, long_df


# ============================================================
# MAIN
# ============================================================

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("Loading mapping...")
    mapping = load_mapping(MAPPING_PATH)
    raw_to_key = build_raw_to_key(mapping)

    print("Loading raw files...")
    combined, col_unique_vals, col_file_count = load_all_raw_files(RAW_DIR)

    print("Checking unmapped columns...")
    unmapped = find_unmapped_columns(
        col_unique_vals, col_file_count, raw_to_key)

    if unmapped:
        unmapped_df = pd.DataFrame(unmapped)
        out_path = os.path.join(OUTPUT_DIR, "unmapped_columns.csv")
        unmapped_df.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"Unmapped columns found. Saved to {out_path}")
        return

    print("Checking for unmapped answers...")
    unmapped_ans = find_unmapped_answers(col_unique_vals, mapping, raw_to_key)

    if unmapped_ans:
        unmapped_ans_df = pd.DataFrame(unmapped_ans)
        out_path = os.path.join(OUTPUT_DIR, "unmapped_answers.csv")
        unmapped_ans_df.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"\nERROR: {len(unmapped_ans)} unmapped answer(s) found across "
              f"{unmapped_ans_df['mapping_key'].nunique()} question(s).")
        print(f"Saved to {out_path}")
        print("Please update column_rename_mapping.json and re-run.")
        return

    print("All columns and answers mapped. Processing...")

    short_df, wide_df, long_df = process_data(combined, mapping, raw_to_key)

    short_df.to_csv(os.path.join(OUTPUT_DIR, "short_survey.csv"),
                    index=False, encoding="utf-8-sig")

    wide_df.to_csv(os.path.join(OUTPUT_DIR, "wide_survey.csv"),
                   index=False, encoding="utf-8-sig")

    long_df.to_csv(os.path.join(OUTPUT_DIR, "long_survey.csv"),
                   index=False, encoding="utf-8-sig")

    print("\nDone.")
    print("  short_survey.csv → meta + single-choice questions")
    print("  wide_survey.csv  → meta + multi-choice (binary columns)")
    print("  long_survey.csv  → meta + multi-choice (melted rows)")


if __name__ == "__main__":
    main()
