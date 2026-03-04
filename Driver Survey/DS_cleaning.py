"""
Driver Survey Data Processing Pipeline
=======================================
Reads raw weekly survey Excel files, validates columns against a JSON mapping,
renames headers, recodes answers, and produces:
  - short_survey.csv  → meta + single-choice questions + computed columns
  - wide_survey.csv   → meta + multi-choice questions (binary 0/1 columns) + computed columns
  - long_survey.csv   → meta + multi-choice questions (melted/stacked rows) + computed columns

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
import numpy as np
import pandas as pd
from collections import defaultdict

# ============================================================
# CONFIGURATION
# ============================================================
RAW_DIR = r"D:\Work\Driver Survey\raw"
MAPPING_PATH = r"D:\Work\Driver Survey\DataSources\column_rename_mapping.json"
OUTPUT_DIR = r"D:\Work\Driver Survey\processed"


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
# Computed columns
# ============================================================

def build_incentive_category(wide_df, platform):
    """
    Classify incentive into Money, Free-Commission, or both.

    Uses wide_df binary columns which follow the naming pattern:
        '{Platform} Incentive Type__{Answer Value}'
    """
    platform_title = platform.capitalize()  # "snapp" → "Snapp"

    money_cols = [
        f"{platform_title} Incentive Type__Pay After Ride",
        f"{platform_title} Incentive Type__Income Guarantee",
    ]
    commfree_cols = [
        f"{platform_title} Incentive Type__Ride-Based Commission-free",
        f"{platform_title} Incentive Type__Earning-based Commission-free",
    ]

    # Only use columns that actually exist in wide_df
    money_cols = [c for c in money_cols if c in wide_df.columns]
    commfree_cols = [c for c in commfree_cols if c in wide_df.columns]

    if money_cols:
        money_used = wide_df[money_cols].astype(int).any(axis=1)
    else:
        money_used = pd.Series(False, index=wide_df.index)

    if commfree_cols:
        commfree_used = wide_df[commfree_cols].astype(int).any(axis=1)
    else:
        commfree_used = pd.Series(False, index=wide_df.index)

    return np.select(
        [
            money_used & commfree_used,
            money_used,
            commfree_used,
        ],
        [
            "Money & Free-commission",
            "Money",
            "Free-Commission",
        ],
        default=""
    )


def add_computed_columns(short_df, wide_df):
    """
    Add all computed/derived columns to short_df.
    Uses recoded values from short_df for single-choice lookups,
    and wide_df for multi-choice (incentive_type) binary columns.

    Returns the modified short_df with new columns appended.
    """

    # ---- BASIC FLAGS ----
    if "tapsi_age" in short_df.columns:
        short_df["joint_by_signup"] = np.where(
            short_df["tapsi_age"] == "Not Registered", 0, 1
        )

    if "tapsi_age" in short_df.columns and "tapsi_trip_count" in short_df.columns:
        short_df["active_joint"] = np.where(
            (short_df["tapsi_age"] == "Not Registered") |
            (short_df["tapsi_trip_count"] == "0"),
            0, 1
        )

    # ---- RIDE COUNT MAPPING ----
    ride_map = {
        "<5": 2.5,
        "5_10": 7.5,
        "11_20": 15,
        "21_30": 25,
        "31_40": 35,
        "41_50": 45,
        "51_60": 55,
        "61_70": 65,
        "71_80": 75,
        ">80": 80,
        "0": 0,     # tapsi_trip_count has "0" for drivers with no trips
    }

    if "snapp_trip_count" in short_df.columns:
        short_df["snapp_ride"] = short_df["snapp_trip_count"].map(ride_map)
    if "tapsi_trip_count" in short_df.columns:
        short_df["tapsi_ride"] = short_df["tapsi_trip_count"].map(ride_map)

    # ---- COMMISSION-FREE RIDE MAPPING ----
    commfree_map = {
        "<5": 2.5,
        "5_10": 7.5,
        "11_20": 15,
        "21_30": 25,
        "31_40": 35,
        "41_50": 45,
        "51_60": 55,
        "61_70": 65,
        "71_80": 75,
        ">80": 80,
    }

    if "tapsi_trip_count_commfree_discount" in short_df.columns:
        short_df["tapsi_commfree_disc_ride"] = short_df[
            "tapsi_trip_count_commfree_discount"
        ].map(commfree_map)
    if "snapp_trip_count_commfree_discount" in short_df.columns:
        short_df["snapp_commfree_disc_ride"] = short_df[
            "snapp_trip_count_commfree_discount"
        ].map(commfree_map)

    # ---- DIFFERENCES ----
    if "snapp_ride" in short_df.columns and "snapp_commfree_disc_ride" in short_df.columns:
        short_df["snapp_diff_commfree"] = (
            short_df["snapp_ride"] - short_df["snapp_commfree_disc_ride"]
        )
    if "tapsi_ride" in short_df.columns and "tapsi_commfree_disc_ride" in short_df.columns:
        short_df["tapsi_diff_commfree"] = (
            short_df["tapsi_ride"] - short_df["tapsi_commfree_disc_ride"]
        )

    # ---- FINAL COMMISSION-FREE VALUE ----
    if "snapp_diff_commfree" in short_df.columns:
        short_df["snapp_commfree"] = np.where(
            short_df["snapp_diff_commfree"] < 0,
            short_df["snapp_ride"],
            short_df["snapp_commfree_disc_ride"],
        )
    if "tapsi_diff_commfree" in short_df.columns:
        short_df["tapsi_commfree"] = np.where(
            short_df["tapsi_diff_commfree"] < 0,
            short_df["tapsi_ride"],
            short_df["tapsi_commfree_disc_ride"],
        )

    # ---- INCENTIVE (RIAL) MAPPING ----
    # NOTE: The JSON recodes to "< 100k" (with space) for the older ranges,
    #       and to "<50k", "50_100k", ">1m", "50_250k" for newer survey waves.
    incentive_map = {
        "< 100k": 500_000,
        "<100k": 500_000,       # alias (no space) just in case
        "<50k": 250_000,
        "50_100k": 750_000,
        "50_250k": 1_500_000,
        "100_200k": 1_500_000,
        "100_250k": 1_750_000,
        "200_400k": 3_000_000,
        "250_500k": 3_750_000,
        "400_600k": 5_000_000,
        "500_750k": 6_250_000,
        "600_800k": 7_000_000,
        "750k_1m": 8_750_000,
        "800k_1m": 9_000_000,
        "1m_1.25m": 11_250_000,
        "1.25m_1.5m": 13_750_000,
        ">1m": 12_500_000,
        ">1.5m": 17_500_000,
    }

    if "snapp_incentive_rial_details" in short_df.columns:
        short_df["snapp_incentive"] = short_df[
            "snapp_incentive_rial_details"
        ].map(incentive_map)
    if "tapsi_incentive_rial_details" in short_df.columns:
        short_df["tapsi_incentive"] = short_df[
            "tapsi_incentive_rial_details"
        ].map(incentive_map)

    # ---- WHEEL (TAPSI MAGICAL WINDOW INCOME) ----
    wheel_map = {
        "<20k": 150_000,
        "20_40k": 300_000,
        "40_60k": 500_000,
        "60_80k": 700_000,
        "80_100k": 900_000,
        "100_150k": 1_250_000,
        "150_200k": 1_750_000,
        ">200k": 2_000_000,
    }

    if "tapsi_magical_window_income" in short_df.columns:
        short_df["wheel"] = short_df["tapsi_magical_window_income"].map(
            wheel_map
        )

    # ---- COOPERATION TYPE ----
    coop_map = {
        "few hours/month": "Part-Time",
        "<20hour/mo": "Part-Time",
        "5_20hour/week": "Part-Time",
        "20_40h/week": "Part-Time",
        ">40h/week": "Full-Time",
        "8_12hour/day": "Full-Time",
        ">12h/day": "Full-Time",
    }

    if "active_time" in short_df.columns:
        short_df["cooperation_type"] = short_df["active_time"].map(coop_map)

    # ---- LOC (LENGTH OF COOPERATION) ----
    loc_map = {
        "Not Registered": 0,
        "less_than_1_month": 0.5,
        "1_to_3_months": 2,
        "less_than_3_months": 2,
        "less_than_5_trips": 2.5,
        "3_to_6_months": 4.5,
        "5_and_10_trips": 7.5,
        "6_to_12_months": 9,
        "6_months_to_1_year": 9,
        "10_and_20_trips": 15,
        "1_to_2_years": 18,
        "1_to_3_years": 24,
        "20_and_30_trips": 25,
        "2_to_3_years": 30,
        "30_and_40_trips": 35,
        "3_to_4_years": 42,
        "40_and_50_trips": 45,
        "3_to_5_years": 48,
        "more_than_4_years": 54,
        "50_and_60_trips": 55,
        "60_and_70_trips": 65,
        "5_to_7_years": 72,
        "70_and_80_trips": 75,
        "more_than_80_trips": 80,
        "more_than_7_years": 96,
    }

    if "snapp_age" in short_df.columns:
        short_df["snapp_LOC"] = short_df["snapp_age"].map(loc_map)
    if "tapsi_age" in short_df.columns:
        short_df["tapsi_LOC"] = short_df["tapsi_age"].map(loc_map)

    # ---- AGE GROUP ----
    age_group_map = {
        "<18": "18_to_35",
        "18_25": "18_to_35",
        "26_35": "18_to_35",
        "36_45": "more_than_35",
        "46_55": "more_than_35",
        "56_65": "more_than_35",
        ">65": "more_than_35",
    }

    if "age" in short_df.columns:
        short_df["age_group"] = short_df["age"].map(age_group_map)

    # ---- EDUCATION ----
    edu_map = {
        "HighSchool_Diploma": 0,
        "College Degree": 1,
        "Bachelors": 1,
        "Masters": 1,
        "MD/PhD": 1,
    }

    if "education" in short_df.columns:
        short_df["edu"] = short_df["education"].map(edu_map)

    # ---- MARITAL STATUS ----
    marr_map = {
        "Single": 0,
        "Married": 1,
    }

    if "marital_status" in short_df.columns:
        short_df["marr_stat"] = short_df["marital_status"].map(marr_map)

    # ---- INCENTIVE CATEGORY (uses wide_df multi-choice binary columns) ----
    short_df["snapp_incentive_category"] = build_incentive_category(
        wide_df, "snapp"
    )
    short_df["tapsi_incentive_category"] = build_incentive_category(
        wide_df, "tapsi"
    )

    return short_df


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
    # COMPUTED COLUMNS — add to short_df (uses recoded values)
    # ============================================================

    short_df = add_computed_columns(short_df, wide_df)

    # Identify which computed columns were added
    computed_cols = [
        c for c in short_df.columns
        if c not in meta_df.columns
        and c not in single_dict
    ]
    print(f"\nComputed columns added: {len(computed_cols)}")

    # Also add computed columns to wide_df
    for col in computed_cols:
        wide_df[col] = short_df[col].values

    print("Wide shape (after computed columns):", wide_df.shape)

    # ============================================================
    # LONG SURVEY → meta + multi-choice (melted rows)
    # ============================================================

    # Build a per-row computed dict for efficient long-format generation
    computed_df = short_df[computed_cols] if computed_cols else pd.DataFrame(
        index=short_df.index
    )

    long_rows = []

    for idx in combined.index:
        meta_row = meta_df.loc[idx].to_dict()
        # Add computed columns to each long row
        if not computed_df.empty:
            comp_row = computed_df.loc[idx].to_dict()
            meta_row.update(comp_row)

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
    print("  short_survey.csv → meta + single-choice + computed columns")
    print("  wide_survey.csv  → meta + multi-choice (binary) + computed columns")
    print("  long_survey.csv  → meta + multi-choice (melted) + computed columns")


if __name__ == "__main__":
    main()
