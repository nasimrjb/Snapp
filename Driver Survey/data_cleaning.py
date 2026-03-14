"""
Driver Survey Data Processing Pipeline  v2
==========================================
STEP 2 OF 3 in the ETL pipeline:
    generate_mapping.py  -->  [data_cleaning.py]  -->  survey_analysis_v6.py

What this script does:
    1. Reads raw weekly survey Excel/CSV files from the raw/ folder.
    2. Validates every column header and every answer value against a
       JSON mapping file (column_rename_mapping.json) produced by Step 1.
    3. Renames (recodes) Persian/Arabic survey answers into clean English
       labels using that mapping.
    4. Splits questions into "main" (asked every week) vs "rare" (asked
       occasionally) based on the "freq" field in the mapping JSON.
    5. Builds computed/derived columns (ride counts, incentive amounts,
       demographic flags, etc.) from the recoded answers.
    6. Outputs six CSV files into processed/:
         - short_survey_main.csv  -- one row per respondent, single-choice
                                     questions asked always/often + computed cols
         - wide_survey_main.csv   -- one row per respondent, multi-choice
                                     questions (binary 0/1 columns) + computed cols
         - long_survey_main.csv   -- one row per (respondent x selected answer),
                                     multi-choice questions melted into long format
         - short_survey_rare.csv  -- same as short_main but for rare questions
         - wide_survey_rare.csv   -- same as wide_main but for rare questions
         - long_survey_rare.csv   -- same as long_main but for rare questions

    If unmapped columns or answers are found, the script writes a diagnostic
    CSV and exits early so you can update the mapping before proceeding.

Fix vs v1:
    v1 had a bug where wide_rare (which lacks Incentive Type binary columns)
    was passed to add_computed_columns() for rare outputs, causing
    snapp_incentive_category / tapsi_incentive_category to silently return
    empty strings.  v2 passes wide_main instead, which contains those columns.

Usage:
    python data_cleaning.py
"""

import os
import sys
import json
import glob
import re
import unicodedata
import numpy as np
import pandas as pd
from collections import defaultdict  # defaultdict: a dict that auto-creates missing keys with a default value

# ============================================================
# CONFIGURATION — paths to raw data, mapping file, and output folder
# ============================================================
RAW_DIR = r"D:\Work\Driver Survey\raw"
MAPPING_PATH = r"D:\Work\Driver Survey\DataSources\column_rename_mapping.json"
OUTPUT_DIR = r"D:\Work\Driver Survey\processed"


# ============================================================
# Helpers — text normalization, file loading, date parsing
# ============================================================

def normalize(text):
    """
    Light normalization: apply Unicode NFKC normalization and strip whitespace.

    NFKC normalization converts visually similar Unicode characters into a
    single canonical form (e.g., full-width Latin letters become ASCII).
    This ensures column headers match even if copied from different sources.

    Parameters:
        text: a string (or non-string, which gets str()-ified)

    Returns:
        The normalized, stripped string.
    """
    if not isinstance(text, str):
        return str(text)
    return unicodedata.normalize("NFKC", text).strip()


# Digit translators: convert Persian digits (۰-۹) and Arabic digits (٠-٩) to ASCII 0-9.
# str.maketrans() creates a translation table that str.translate() can use for fast
# character-by-character replacement.  Survey data often has digits in these scripts.
_PERSIAN_TO_ASCII = str.maketrans("۰۱۲۳۴۵۶۷۸۹", "0123456789")
_ARABIC_TO_ASCII = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")


def fuzzy_normalize(text):
    """
    Aggressive normalization for comparing survey answers.

    Why this exists:
        Survey answers are typed by different people across weeks, so the same
        logical answer might appear as "کمتر از ۵" in one file and "كمتر از 5"
        in another (different digit scripts, different Unicode forms of Arabic
        letters, extra zero-width characters, etc.).  This function strips all
        those differences so we can match answers reliably.

    Steps applied:
        1. NFKC Unicode normalization (canonical decomposition + composition)
        2. Persian / Arabic digits → ASCII digits
        3. Zero-width / formatting characters (Unicode category "Cf") and
           diacritics (category "Mn") replaced with spaces
        4. Punctuation and symbols removed (keep only letters, digits, spaces)
        5. Whitespace collapsed to single spaces
        6. Lowercased

    Parameters:
        text: a raw answer string from the survey data

    Returns:
        A cleaned, lowercase string suitable for dictionary lookup.
    """
    if not isinstance(text, str):
        text = str(text)
    text = unicodedata.normalize("NFKC", text)
    # Persian / Arabic digits → ASCII
    text = text.translate(_PERSIAN_TO_ASCII)
    text = text.translate(_ARABIC_TO_ASCII)
    # Replace zero-width / formatting chars (Cf) and diacritics (Mn) with space.
    # unicodedata.category(c) returns a 2-letter code for each character's Unicode
    # category (e.g., "Cf" = format char like ZWNJ, "Mn" = combining accent mark).
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
    """
    Load the column rename mapping JSON file (produced by generate_mapping.py).

    The mapping is a dict where each key is the cleaned column name and the
    value is a dict with metadata: "raw" (original header variants), "type"
    (meta/single/multi/other), "freq" (always/often/rare), "answers" (answer
    recoding dict), and "long" (human-readable question title).

    Parameters:
        path: absolute path to column_rename_mapping.json

    Returns:
        The parsed dict.
    """
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def is_customized_answer(meta):
    """
    Return True if this mapping entry has customized_answer set.

    "customized_answer" columns are free-text fields where the respondent
    typed their own answer (as opposed to selecting from a predefined list).
    These are excluded from all outputs because they contain unstructured text
    that cannot be recoded into standardized categories.

    Parameters:
        meta: the mapping dict for a single column (e.g., mapping["some_key"])

    Returns:
        True if the column should be excluded, False otherwise.
    """
    answers = meta.get("answers")
    if isinstance(answers, dict):
        return (answers.get("customized_answer") == "customized_answer")
    return False


def build_raw_to_key(mapping):
    """
    Build a lookup dict from normalized raw column headers → mapping keys.

    Each mapping key can have multiple "raw" header variants (because the
    same question may appear with slightly different headers across weekly
    survey files).  This function creates a reverse index so we can look up
    any raw header and find its canonical key.

    Parameters:
        mapping: the full mapping dict loaded from JSON

    Returns:
        dict of {normalized_raw_header: mapping_key}
    """
    raw_to_key = {}
    for key, meta in mapping.items():
        for raw_header in meta.get("raw", []):
            raw_to_key[normalize(raw_header)] = key
    return raw_to_key


def parse_datetime_column(series):
    """
    Parse a datetime column that may contain mixed formats.

    Why this exists:
        Different weekly survey files may store the submission timestamp in
        different formats: some as normal date strings ('2025/01/15'), some
        as Excel serial date numbers stored as text ('45771.864...'), and
        some as empty/NaN.  This function handles all three cases.

    How Excel serial dates work:
        Excel stores dates as the number of days since 1899-12-30.  So the
        float 45771.864 means 45771 days + 0.864 of a day after that epoch.

    Parameters:
        series: a pandas Series of raw datetime values (strings or NaN)

    Returns:
        A pandas Series of proper Timestamp objects (or NaT for unparseable).
    """
    def parse_one(val):
        if pd.isna(val):
            return pd.NaT
        if isinstance(val, str):
            val = val.strip()
            if not val:
                return pd.NaT
            # Try normal datetime parse first (handles '2025-01-15', '2025/01/15 10:30' etc.)
            ts = pd.to_datetime(val, errors="coerce")
            if ts is not pd.NaT:
                return ts
            # Try as Excel serial number (float stored as string)
            try:
                numeric = float(val)
                # Sanity check: serial dates for years ~1903-2173 fall in range 1000-100000
                if 1000 < numeric < 100000:
                    return pd.Timestamp("1899-12-30") + pd.Timedelta(days=numeric)
            except (ValueError, TypeError):
                pass
        return pd.NaT

    # .apply() calls parse_one on each element of the Series individually
    return series.apply(parse_one)


def compute_weeknumber(dt_series):
    """
    Compute a custom week number from an already-parsed datetime series.

    Business reason:
        The survey team uses an Excel-style week numbering system where weeks
        start on Sunday.  In Iran's calendar, the weekend is Thursday-Friday,
        and Saturday is the first day of the work week.  Surveys completed on
        weekends (Saturday/Sunday) should be counted as belonging to the
        NEXT week, because they were completed after the work week ended.

    Replicates the Excel formula:
        IF(WEEKDAY(date)=7, WEEKNUM(date)+1, WEEKNUM(date))

    Excel conventions:
        WEEKDAY (type 1): Sunday=1, Monday=2, ... Saturday=7
        WEEKNUM (type 1): Week containing Jan 1 is week 1, weeks start Sunday.

    pandas conventions:
        dayofweek: Monday=0, Tuesday=1, ... Saturday=5, Sunday=6

    So "Saturday" = dayofweek 5 and "Sunday" = dayofweek 6 in pandas.
    Both get weeknum + 1 to shift them into the following week.

    Parameters:
        dt_series: a pandas Series of Timestamp values (may contain NaT)

    Returns:
        A pandas Series of Int64 week numbers (nullable integer to handle NaT).
    """
    dt = dt_series
    valid = dt.notna()

    # Start with all NA values; Int64 is pandas' nullable integer type
    # (regular int64 cannot hold NaN, but Int64 can)
    result = pd.Series(pd.NA, index=dt.index, dtype="Int64")

    if valid.sum() == 0:
        return result

    # Only compute on non-NaT rows to avoid NaN poisoning
    dv = dt[valid]

    # Day of year (1-based): Jan 1 = 1, Jan 2 = 2, etc.
    doy = dv.dt.dayofyear

    # What weekday is Jan 1 of each year?  We need this to align the week
    # boundaries.  Convert pandas convention (Mon=0..Sun=6) to Sun=0..Sat=6.
    jan1 = pd.to_datetime(
        dv.dt.year.astype(int).astype(str) + "-01-01",
        format="%Y-%m-%d"
    )
    # (dayofweek + 1) % 7 converts: Mon(0)→1, Tue(1)→2, ... Sat(5)→6, Sun(6)→0
    jan1_wday_sun = (jan1.dt.dayofweek + 1) % 7

    # Excel WEEKNUM formula (type 1, Sunday start):
    # The week containing Jan 1 is always week 1.
    # weeknum = floor((dayOfYear + jan1Weekday - 1) / 7) + 1
    weeknum = (doy + jan1_wday_sun - 1) // 7 + 1

    # Weekend adjustment: Saturday (dayofweek=5) or Sunday (dayofweek=6)
    # get bumped to the next week, because the survey for that work week
    # has already closed.
    is_weekend = dv.dt.dayofweek.isin([5, 6])

    # .where(~is_weekend, weeknum + 1) means:
    #   keep weeknum where NOT weekend, else use weeknum + 1
    result[valid] = weeknum.where(~is_weekend, weeknum + 1).astype("Int64")

    return result


def load_all_raw_files(raw_dir):
    """
    Load all raw survey files (Excel and CSV) from the raw directory,
    normalize their column headers, and combine them into one DataFrame.

    Also collects per-column statistics (unique values and file counts)
    needed for validation.

    Parameters:
        raw_dir: path to the folder containing raw survey files

    Returns:
        A tuple of:
        - combined: a single DataFrame with all rows from all files, plus a
                    '_source_file' column tracking which file each row came from
        - col_unique_vals: defaultdict(set) mapping each normalized column
                           header to all unique non-null values seen across files
        - col_file_count: defaultdict(int) mapping each normalized column
                          header to the number of files it appeared in
    """
    patterns = ["*.xlsx", "*.xls", "*.csv"]
    files = []
    for pat in patterns:
        files.extend(glob.glob(os.path.join(raw_dir, pat)))
    # sorted(set(...)) removes duplicates (in case a file matches multiple patterns)
    files = sorted(set(files))

    if not files:
        print(f"ERROR: No data files found in {raw_dir}")
        sys.exit(1)

    print(f"Found {len(files)} raw file(s)")

    frames = []
    # defaultdict(set): accessing a missing key auto-creates an empty set
    # defaultdict(int): accessing a missing key auto-creates 0
    # This avoids "if key not in dict: dict[key] = ..." boilerplate.
    col_unique_vals = defaultdict(set)
    col_file_count = defaultdict(int)

    for fpath in files:
        fname = os.path.basename(fpath)
        print(f"  Reading: {fname}")

        # Read everything as strings (dtype=str) to avoid pandas guessing types.
        # We'll convert to proper types later during recoding.
        if fpath.endswith(".csv"):
            df = pd.read_csv(fpath, dtype=str)
        else:
            df = pd.read_excel(fpath, dtype=str)

        # Normalize column headers (NFKC + strip whitespace)
        norm_cols = [normalize(c) for c in df.columns]
        df.columns = norm_cols
        # Track which file each row came from (useful for debugging)
        df["_source_file"] = fname

        # Collect unique values and file counts for each column
        # (used later to detect unmapped columns and answers)
        for col in norm_cols:
            col_file_count[col] += 1
            col_unique_vals[col].update(df[col].dropna().unique())

        frames.append(df)

    # pd.concat stacks all DataFrames vertically; ignore_index resets row numbers
    combined = pd.concat(frames, ignore_index=True)
    print(f"Total rows: {len(combined)}")

    return combined, col_unique_vals, col_file_count


# ============================================================
# Validation — check that all columns and answers are in the mapping
# ============================================================

def find_unmapped_answers(col_unique_vals, mapping, raw_to_key):
    """
    For single and multi-choice questions, check that every unique raw answer
    found in the actual survey data has a corresponding entry in the mapping's
    answers dict.

    Why this exists:
        If a new answer option is added to the survey (e.g., a new age range),
        it won't be in the mapping JSON and would silently pass through without
        recoding.  This function catches those mismatches early so you can
        update the mapping before producing outputs with uncoded values.

    Parameters:
        col_unique_vals: dict of {normalized_column: set of unique raw values}
        mapping:         the full mapping dict from JSON
        raw_to_key:      dict of {normalized_raw_header: mapping_key}

    Returns:
        A list of dicts, each describing one unmapped answer:
        {"mapping_key", "question_type", "long_name", "unmapped_raw_answer"}
        Empty list means all answers are mapped.
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
        # Only check single-choice and multi-choice questions (not meta/other)
        if qtype not in ("single", "multi"):
            continue

        # Skip columns that would be excluded during processing anyway
        if is_customized_answer(meta):
            continue
        if meta.get("type") == "other":
            continue

        answers = meta.get("answers")
        if not answers:
            continue

        # Build a set of fuzzy-normalized expected answers for comparison
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
    """
    Find columns present in the raw data that have no entry in the mapping.

    Why this exists:
        When new questions are added to the survey, their column headers won't
        be in the mapping JSON.  This function detects them so you can add them
        to the mapping (via generate_mapping.py) before running the pipeline.

    Parameters:
        col_unique_vals: dict of {normalized_column: set of unique raw values}
        col_file_count:  dict of {normalized_column: number of files it appeared in}
        raw_to_key:      dict of {normalized_raw_header: mapping_key}

    Returns:
        A list of dicts describing each unmapped column:
        {"raw_column_header", "unique_answers_sample", "num_unique_answers",
         "num_files_appeared"}
        Empty list means all columns are mapped.
    """
    unmapped = []

    for col in sorted(col_unique_vals.keys()):
        # Skip the internal tracking column we added during loading
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
# Computed columns — derived metrics added to the output
# ============================================================

def build_incentive_category(wide_df, platform):
    """
    Classify each driver's incentive usage into one of three categories:
    "Money", "Free-Commission", or "Money & Free-commission".

    Business context:
        Snapp and Tapsi offer two broad types of driver incentives:
        - Money-based: "Pay After Ride" (bonus per trip) and "Income Guarantee"
          (minimum earnings guarantee)
        - Commission-free: "Ride-Based Commission-free" (X rides without
          commission) and "Earning-based Commission-free" (earnings up to Y
          are commission-free)
        A driver may use one or both types.  This column lets analysts segment
        drivers by incentive strategy.

    How it works:
        Uses the binary (0/1) columns in wide_df.  These columns follow the
        naming pattern: '{Platform} Incentive Type__{Answer Value}'.
        If ANY money column is 1 → money_used = True.
        If ANY commission-free column is 1 → commfree_used = True.

    Parameters:
        wide_df:  the wide-format DataFrame containing multi-choice binary columns
        platform: "snapp" or "tapsi" (lowercase)

    Returns:
        A numpy array of category strings (one per row), with "" for drivers
        who didn't select any incentive type.
    """
    platform_title = platform.capitalize()  # "snapp" → "Snapp"

    # Money-based incentive columns
    money_cols = [
        f"{platform_title} Incentive Type__Pay After Ride",
        f"{platform_title} Incentive Type__Income Guarantee",
    ]
    # Commission-free incentive columns
    commfree_cols = [
        f"{platform_title} Incentive Type__Ride-Based Commission-free",
        f"{platform_title} Incentive Type__Earning-based Commission-free",
    ]

    # Only use columns that actually exist in wide_df (some survey waves
    # may not include all incentive types)
    money_cols = [c for c in money_cols if c in wide_df.columns]
    commfree_cols = [c for c in commfree_cols if c in wide_df.columns]

    # .any(axis=1) checks if ANY column in the row is True (i.e., at least
    # one money-based incentive was selected by this driver)
    if money_cols:
        money_used = wide_df[money_cols].astype(int).any(axis=1)
    else:
        money_used = pd.Series(False, index=wide_df.index)

    if commfree_cols:
        commfree_used = wide_df[commfree_cols].astype(int).any(axis=1)
    else:
        commfree_used = pd.Series(False, index=wide_df.index)

    # np.select() is like a vectorized if/elif/else chain:
    #   - First condition matched wins
    #   - "default" is the fallback if no condition matches
    return np.select(
        [
            money_used & commfree_used,   # Both types used → first priority
            money_used,                    # Only money-based
            commfree_used,                 # Only commission-free
        ],
        [
            "Money & Free-commission",
            "Money",
            "Free-Commission",
        ],
        default=""  # Driver didn't select any incentive type
    )


def add_computed_columns(short_df, wide_df):
    """
    Add all computed/derived columns to short_df.

    These columns are business metrics calculated from the recoded survey
    answers.  They don't exist in the raw data — they are derived here to
    save the analyst from repeating the same calculations.

    Uses:
        - short_df for single-choice lookups (trip counts, age, etc.)
        - wide_df for multi-choice binary columns (incentive type)

    Parameters:
        short_df: the short-format DataFrame (meta + single-choice columns)
        wide_df:  the wide-format DataFrame (meta + multi-choice binary columns)

    Returns:
        The modified short_df with new computed columns appended.
    """

    # ---- BASIC FLAGS ----
    # joint_by_signup: 1 if the driver is registered on Tapsi (i.e., drives for
    # both platforms), 0 if "Not Registered" on Tapsi.  "Joint" = both Snapp+Tapsi.
    if "tapsi_age" in short_df.columns:
        # np.where(condition, value_if_true, value_if_false) — vectorized if/else
        short_df["joint_by_signup"] = np.where(
            short_df["tapsi_age"] == "Not Registered", 0, 1
        )

    # active_joint: 1 if the driver is both registered on Tapsi AND has completed
    # at least one Tapsi trip.  A driver who signed up but never drove (0 trips)
    # is not considered an "active" joint driver.
    if "tapsi_age" in short_df.columns and "tapsi_trip_count" in short_df.columns:
        short_df["active_joint"] = np.where(
            (short_df["tapsi_age"] == "Not Registered") |
            (short_df["tapsi_trip_count"] == "0"),
            0, 1
        )

    # ---- RIDE COUNT MAPPING ----
    # The survey asks "How many rides did you complete last week?" with bucketed
    # answers like "<5", "5_10", "11_20", etc.  We map each bucket to its
    # midpoint so we can compute numerical averages and differences.
    # Example: "5_10" → 7.5 (midpoint of 5 and 10)
    ride_map = {
        "<5": 2.5,       # Midpoint of 0-5
        "5_10": 7.5,     # Midpoint of 5-10
        "11_20": 15,     # Midpoint of 11-20
        "21_30": 25,     # Midpoint of 21-30
        "31_40": 35,     # Midpoint of 31-40
        "41_50": 45,     # Midpoint of 41-50
        "51_60": 55,     # Midpoint of 51-60
        "61_70": 65,     # Midpoint of 61-70
        "71_80": 75,     # Midpoint of 71-80
        ">80": 80,       # Conservative estimate for 80+ (no upper bound)
        "0": 0,          # tapsi_trip_count has "0" for non-Tapsi drivers
    }

    # .map(dict) replaces each value using the dict as a lookup table.
    # Values not found in the dict become NaN.
    if "snapp_trip_count" in short_df.columns:
        short_df["snapp_ride"] = short_df["snapp_trip_count"].map(ride_map)
    if "tapsi_trip_count" in short_df.columns:
        short_df["tapsi_ride"] = short_df["tapsi_trip_count"].map(ride_map)

    # ---- COMMISSION-FREE RIDE MAPPING ----
    # Same idea as ride_map above, but for the question: "How many of your rides
    # last week used a commission-free discount?"  Maps bucketed answers to
    # midpoint values for numerical analysis.
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
    # Calculate the difference between total rides and commission-free rides.
    # A negative value would mean the driver reported MORE commission-free rides
    # than total rides — which is logically impossible and likely a data error.
    if "snapp_ride" in short_df.columns and "snapp_commfree_disc_ride" in short_df.columns:
        short_df["snapp_diff_commfree"] = (
            short_df["snapp_ride"] - short_df["snapp_commfree_disc_ride"]
        )
    if "tapsi_ride" in short_df.columns and "tapsi_commfree_disc_ride" in short_df.columns:
        short_df["tapsi_diff_commfree"] = (
            short_df["tapsi_ride"] - short_df["tapsi_commfree_disc_ride"]
        )

    # ---- FINAL COMMISSION-FREE VALUE ----
    # If the difference is negative (impossible: more commission-free rides than
    # total rides), assume the driver misunderstood the question and use total
    # rides as the commission-free count instead.  Otherwise, use the reported
    # commission-free discount ride count as-is.
    if "snapp_diff_commfree" in short_df.columns:
        short_df["snapp_commfree"] = np.where(
            short_df["snapp_diff_commfree"] < 0,
            short_df["snapp_ride"],            # Fallback: use total rides
            short_df["snapp_commfree_disc_ride"],  # Normal: use reported value
        )
    if "tapsi_diff_commfree" in short_df.columns:
        short_df["tapsi_commfree"] = np.where(
            short_df["tapsi_diff_commfree"] < 0,
            short_df["tapsi_ride"],
            short_df["tapsi_commfree_disc_ride"],
        )

    # ---- INCENTIVE (RIAL) MAPPING ----
    # Maps the survey's bucketed incentive amount answers to midpoint values
    # in Rials (Iran's currency).  The values are in Rials, not Tomans
    # (1 Toman = 10 Rials).
    #
    # The survey question asks "How much incentive money did you earn last week?"
    # with answers like "<100k" (less than 100,000 Tomans), "100_200k" (100k-200k
    # Tomans), etc.  The mapped values are the midpoints in Rials.
    #
    # NOTE: The answer labels changed across survey waves (e.g., older waves use
    # "< 100k" with a space; newer waves use "<50k", "50_100k").  Both variants
    # are included here.
    incentive_map = {
        "< 100k": 500_000,       # <100k Tomans → 50k Tomans midpoint = 500,000 Rials
        "<100k": 500_000,        # Alias without space, just in case
        "<50k": 250_000,         # <50k Tomans → 25k Tomans midpoint = 250,000 Rials
        "50_100k": 750_000,      # 50-100k Tomans → 75k midpoint = 750,000 Rials
        "50_250k": 1_500_000,    # 50-250k Tomans → 150k midpoint
        "100_200k": 1_500_000,   # 100-200k Tomans → 150k midpoint
        "100_250k": 1_750_000,   # 100-250k Tomans → 175k midpoint
        "200_400k": 3_000_000,   # 200-400k Tomans → 300k midpoint
        "250_500k": 3_750_000,   # 250-500k Tomans → 375k midpoint
        "400_600k": 5_000_000,   # 400-600k Tomans → 500k midpoint
        "500_750k": 6_250_000,   # 500-750k Tomans → 625k midpoint
        "600_800k": 7_000_000,   # 600-800k Tomans → 700k midpoint
        "750k_1m": 8_750_000,    # 750k-1m Tomans → 875k midpoint
        "800k_1m": 9_000_000,    # 800k-1m Tomans → 900k midpoint
        "1m_1.25m": 11_250_000,  # 1-1.25m Tomans → 1.125m midpoint
        "1.25m_1.5m": 13_750_000, # 1.25-1.5m Tomans → 1.375m midpoint
        ">1m": 12_500_000,       # >1m Tomans → 1.25m estimate
        ">1.5m": 17_500_000,     # >1.5m Tomans → 1.75m estimate
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
    # "Magical Window" (Panjere-ye Jadooyi) is a Tapsi gamification feature
    # where drivers spin a wheel/lottery after completing rides and earn bonus
    # income.  This maps the bucketed answer to midpoint Rials.
    wheel_map = {
        "<20k": 150_000,         # <20k Tomans → 15k midpoint = 150,000 Rials
        "20_40k": 300_000,       # 20-40k Tomans → 30k midpoint
        "40_60k": 500_000,       # 40-60k Tomans → 50k midpoint
        "60_80k": 700_000,       # 60-80k Tomans → 70k midpoint
        "80_100k": 900_000,      # 80-100k Tomans → 90k midpoint
        "100_150k": 1_250_000,   # 100-150k Tomans → 125k midpoint
        "150_200k": 1_750_000,   # 150-200k Tomans → 175k midpoint
        ">200k": 2_000_000,     # >200k Tomans → 200k estimate
    }

    if "tapsi_magical_window_income" in short_df.columns:
        short_df["wheel"] = short_df["tapsi_magical_window_income"].map(
            wheel_map
        )

    # ---- COOPERATION TYPE ----
    # Maps the driver's reported weekly active hours into a binary classification:
    # "Part-Time" (less than 40h/week) vs "Full-Time" (40+ hours/week).
    # This is a key segmentation variable for the analysis team.
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
    # Maps the driver's tenure with each platform into a numeric value (months).
    # "Not Registered" → 0, "less_than_1_month" → 0.5, etc.
    # Some survey waves ask tenure as months, others as trip counts — both
    # variants are mapped here.  Trip-count-based values are approximate
    # conversions assuming a typical ride frequency.
    loc_map = {
        "Not Registered": 0,
        "less_than_1_month": 0.5,
        "1_to_3_months": 2,
        "less_than_3_months": 2,
        "less_than_5_trips": 2.5,        # Trip-based proxy for tenure
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
    # Collapses the 7 age brackets into two groups for simplified analysis:
    # "18_to_35" (younger drivers) vs "more_than_35" (older drivers).
    age_group_map = {
        "<18": "18_to_35",          # Under 18 grouped with younger
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
    # Binary flag: 0 = high school diploma or below, 1 = any college/university degree.
    # Used for demographic segmentation in the analysis report.
    edu_map = {
        "HighSchool_Diploma": 0,   # No college degree
        "College Degree": 1,       # 2-year associate degree
        "Bachelors": 1,            # 4-year bachelor's degree
        "Masters": 1,              # Master's degree
        "MD/PhD": 1,               # Doctoral degree
    }

    if "education" in short_df.columns:
        short_df["edu"] = short_df["education"].map(edu_map)

    # ---- MARITAL STATUS ----
    # Binary flag: 0 = Single, 1 = Married
    marr_map = {
        "Single": 0,
        "Married": 1,
    }

    if "marital_status" in short_df.columns:
        short_df["marr_stat"] = short_df["marital_status"].map(marr_map)

    # ---- INCENTIVE CATEGORY (uses wide_df multi-choice binary columns) ----
    # Classify each driver's incentive usage strategy.
    # NOTE: wide_df must contain the "Incentive Type" binary columns for this
    # to work.  In v1 this was buggy because wide_rare was passed for rare
    # outputs (and wide_rare doesn't have incentive columns).  v2 fixes this
    # by always passing wide_main.
    short_df["snapp_incentive_category"] = build_incentive_category(
        wide_df, "snapp"
    )
    short_df["tapsi_incentive_category"] = build_incentive_category(
        wide_df, "tapsi"
    )

    return short_df


# ============================================================
# Processing — the main data transformation pipeline
# ============================================================

def process_data(combined, mapping, raw_to_key):
    """
    Core processing function.  Takes the combined raw DataFrame and mapping,
    and produces six output DataFrames (short/wide/long x main/rare).

    High-level flow:
        1. Identify which columns to skip (customized_answer, other type)
        2. Classify present columns by type (meta/single/multi) and frequency
           (main vs rare)
        3. Build meta columns (shared by all outputs)
        4. Recode all single-choice answers using the mapping
        5. Validate data (drop contradictory rows)
        6. Build wide DataFrames (multi-choice → binary 0/1 columns)
        7. Add computed columns to short DataFrames
        8. Build long DataFrames (one row per selected multi-choice answer)

    Parameters:
        combined:   the raw DataFrame with all rows from all files
        mapping:    the full mapping dict from JSON
        raw_to_key: dict of {normalized_raw_header: mapping_key}

    Returns:
        Tuple of (short_main, wide_main, long_main,
                  short_rare, wide_rare, long_rare)
    """

    # Build set of keys to skip:
    #   - customized_answer columns (free-text fields — not useful for analysis)
    #   - "other" type columns (the "Other: ___" free-text option in multi-choice)
    skip_keys = set()
    for key, meta in mapping.items():
        if is_customized_answer(meta):
            skip_keys.add(key)
        if meta.get("type") == "other":
            skip_keys.add(key)

    if skip_keys:
        print(f"\nSkipping {len(skip_keys)} column(s) "
              f"(customized_answer + other type)")

    # Identify which mapping keys are actually present in the combined data,
    # excluding the skipped ones.  present_keys maps key → normalized_raw_col.
    present_keys = {}
    for col in combined.columns:
        if col in raw_to_key:
            key = raw_to_key[col]
            if key not in skip_keys:
                present_keys[key] = col

    # Classify each present key by its question type AND frequency.
    # "main" = always/often (asked every week or most weeks)
    # "rare" = asked only occasionally (e.g., quarterly satisfaction questions)
    meta_keys = []           # Metadata columns (city, phone, datetime, etc.)
    main_single_keys = []    # Single-choice questions asked always/often
    rare_single_keys = []    # Single-choice questions asked rarely
    main_multi_keys = []     # Multi-choice questions asked always/often
    rare_multi_keys = []     # Multi-choice questions asked rarely

    for key, col in present_keys.items():
        qtype = mapping[key]["type"]
        freq = mapping[key].get("freq")
        if qtype == "meta":
            # Skip columns whose key starts with "ignore" (columns explicitly
            # marked to be excluded from outputs, like internal IDs)
            if not key.startswith("ignore"):
                meta_keys.append(key)
        elif qtype == "single":
            if freq == "rare":
                rare_single_keys.append(key)
            else:
                main_single_keys.append(key)
        elif qtype == "multi":
            if freq == "rare":
                rare_multi_keys.append(key)
            else:
                main_multi_keys.append(key)

    print("\nColumn classification (after exclusions):")
    print(f"  meta:        {len(meta_keys)}")
    print(f"  single main: {len(main_single_keys)}")
    print(f"  single rare: {len(rare_single_keys)}")
    print(f"  multi  main: {len(main_multi_keys)}")
    print(f"  multi  rare: {len(rare_multi_keys)}")
    print(f"  other:       (excluded)")

    # ============================================================
    # BUILD META COLUMNS (shared by all six output files)
    # ============================================================
    # Meta columns are things like city, phone number, datetime, etc.
    # They appear in every output file so all rows can be traced back
    # to a specific respondent and survey submission.

    meta_dict = {}
    for key in meta_keys:
        meta_dict[key] = combined[present_keys[key]].copy()
    # Include source file tracking in meta
    meta_dict["_source_file"] = combined["_source_file"]

    # Parse datetime and compute weeknumber if the datetime column exists
    if "datetime" in meta_dict:
        meta_dict["datetime"] = parse_datetime_column(meta_dict["datetime"])
        meta_dict["weeknumber"] = compute_weeknumber(meta_dict["datetime"])
        n_parsed = meta_dict["datetime"].notna().sum()
        n_total = len(meta_dict["datetime"])
        print(f"\nDatetime parsing: {n_parsed:,} of {n_total:,} parsed "
              f"({n_total - n_parsed:,} unparseable)")
        print(f"Weeknumber computed: {meta_dict['weeknumber'].notna().sum():,} "
              f"valid values")
    else:
        print("WARNING: 'datetime' column not found — weeknumber not computed")

    meta_df = pd.DataFrame(meta_dict)

    # ============================================================
    # RECODE ALL SINGLE-CHOICE ANSWERS
    # ============================================================
    # Recode both main AND rare single-choice columns together, because
    # some columns (like tapsi_age, tapsi_trip_count) are needed by the
    # data validation step regardless of their freq classification.

    def _recode_single(keys):
        """
        For each key in keys, recode the raw survey answers into clean
        English labels using the mapping's "answers" dict.

        How it works:
            1. Get the raw column from combined
            2. Build a fuzzy-normalized version of the answers dict
            3. For each cell value, fuzzy-normalize it and look it up
            4. If found, replace with the mapped value; if not found, keep as-is

        The lambda function captures 'fa' (fuzzy answers dict) via a default
        argument trick: "lambda x, fa=fuzzy_ans: ..." binds fuzzy_ans at
        definition time, not at call time.  This is needed because the loop
        variable fuzzy_ans changes on each iteration.

        Parameters:
            keys: list of mapping keys to recode

        Returns:
            dict of {key: recoded Series}
        """
        d = {}
        for key in keys:
            col = combined[present_keys[key]].copy()
            answers = mapping[key].get("answers")
            if answers:
                # Build a fuzzy-normalized lookup: {fuzzy_key: recoded_value}
                fuzzy_ans = {fuzzy_normalize(k): v for k, v in answers.items()}
                # .map() with a lambda: for each non-null value, look up its
                # fuzzy-normalized form in fuzzy_ans; return the mapped value
                # if found, otherwise keep the original value
                col = col.map(
                    lambda x, fa=fuzzy_ans: fa.get(fuzzy_normalize(x), x)
                    if pd.notna(x) else x
                )
            d[key] = col
        return d

    # Recode ALL single-choice columns (both main and rare) in one pass
    all_single_dict = _recode_single(main_single_keys + rare_single_keys)

    # ============================================================
    # DATA VALIDATION — drop invalid/contradictory rows
    # ============================================================
    # Drop rows where a driver claims to be "Not Registered" on Tapsi but
    # has a non-zero trip count on Tapsi.  This is a contradictory response
    # that indicates either a misunderstanding or data entry error.
    # Applied to all rows before splitting into main/rare so both versions
    # share the same clean row set.

    _tmp = pd.concat([meta_df, pd.DataFrame(all_single_dict)], axis=1)
    n_before = len(_tmp)
    valid_mask = pd.Series(True, index=_tmp.index)

    if "tapsi_age" in _tmp.columns and "tapsi_trip_count" in _tmp.columns:
        # BUG FIX: The old condition `tapsi_trip_count != "0"` also matched
        # NaN values (because NaN != anything is True in pandas), which
        # accidentally dropped ~140k exclusive-Snapp drivers whose trip count
        # was blank (they never registered on Tapsi, so the question was
        # skipped).  The fix adds a notna() check so only rows with an
        # *explicit* non-zero trip count are considered contradictory.
        invalid = (
            (_tmp["tapsi_age"] == "Not Registered") &
            (_tmp["tapsi_trip_count"].notna()) &
            (_tmp["tapsi_trip_count"] != "0")
        )
        valid_mask &= ~invalid  # Keep rows that are NOT invalid

    # Apply the valid_mask to all DataFrames/dicts, dropping invalid rows
    # and resetting the index so row numbers are contiguous again
    combined = combined.loc[valid_mask].reset_index(drop=True)
    meta_df = meta_df.loc[valid_mask].reset_index(drop=True)
    for key in all_single_dict:
        all_single_dict[key] = (
            all_single_dict[key].loc[valid_mask].reset_index(drop=True)
        )

    n_dropped = n_before - len(combined)
    if n_dropped:
        print(f"\nDropped {n_dropped} invalid row(s) "
              f"(tapsi_age='Not Registered' with tapsi_trip_count != '0')")

    # ============================================================
    # HELPER FUNCTIONS for building wide and long DataFrames
    # ============================================================

    def _build_multi_groups(keys):
        """
        Group multi-choice columns by their parent question (long title).

        Multi-choice questions are stored in the raw data as multiple columns,
        one per answer option.  For example, the question "What incentive types
        do you use?" might have columns for "Pay After Ride", "Income Guarantee",
        etc.  Each column is non-null if the respondent selected that option.

        This function groups them by their parent question's "long" title so
        we can process all options for one question together.

        Parameters:
            keys: list of mapping keys for multi-choice columns

        Returns:
            defaultdict(list) mapping long_title → [(key, norm_col, answer_value), ...]
            where answer_value is the recoded label for this option.
        """
        groups = defaultdict(list)
        for key in keys:
            long_title = mapping[key]["long"]
            answers = mapping[key].get("answers", {})
            # Each multi-choice column has exactly one answer value
            answer_value = list(answers.values())[0] if answers else key
            groups[long_title].append((key, present_keys[key], answer_value))
        return groups

    def _build_wide(multi_groups_dict):
        """
        Build a wide-format DataFrame from multi-choice question groups.

        In the wide format, each answer option becomes its own binary (0/1)
        column.  The column name is "{Question Title}__{Answer Value}".
        A value of 1 means the respondent selected that option; 0 means
        they did not.

        The binary encoding comes from checking if the raw cell is non-null:
        .notna().astype(int).  In the raw data, a selected multi-choice option
        has the option text as the cell value; an unselected option is NaN.

        Parameters:
            multi_groups_dict: output of _build_multi_groups()

        Returns:
            DataFrame with meta columns + binary multi-choice columns.
        """
        cols = {}
        for long_title, options in multi_groups_dict.items():
            for key, norm_col, answer_value in options:
                col_name = f"{long_title}__{answer_value}"
                # notna() → True where respondent selected this option → 1
                # isna()  → True where not selected → 0
                cols[col_name] = combined[norm_col].notna().astype(int)
        if cols:
            return pd.concat([meta_df, pd.DataFrame(cols)], axis=1)
        return meta_df.copy()

    # ============================================================
    # MAIN VERSION  (freq: always + often)
    # ============================================================

    # Build short_main: meta + recoded single-choice columns for main questions
    main_single_dict = {k: all_single_dict[k] for k in main_single_keys}
    short_main = pd.concat([meta_df, pd.DataFrame(main_single_dict)], axis=1)
    print(f"\nMain short shape (meta + single): {short_main.shape}")

    # Build wide_main: meta + binary multi-choice columns for main questions
    main_multi_groups = _build_multi_groups(main_multi_keys)
    wide_main = _build_wide(main_multi_groups)
    print(f"Main wide shape (meta + multi binary): {wide_main.shape}")

    # Add computed columns to short_main (uses wide_main for incentive binary cols)
    short_main = add_computed_columns(short_main, wide_main)
    # Identify which columns are newly computed (not in meta or single-choice)
    main_computed_cols = [
        c for c in short_main.columns
        if c not in meta_df.columns and c not in main_single_dict
    ]
    print(f"Main computed columns added: {len(main_computed_cols)}")
    # Copy computed columns into wide_main so all outputs have them
    for col in main_computed_cols:
        wide_main[col] = short_main[col].values
    print(f"Main wide shape (after computed columns): {wide_main.shape}")

    # ============================================================
    # RARE VERSION  (freq: rare)
    # ============================================================

    # Build short_rare: meta + recoded single-choice columns for rare questions
    rare_single_dict = {k: all_single_dict[k] for k in rare_single_keys}
    short_rare = pd.concat([meta_df, pd.DataFrame(rare_single_dict)], axis=1)
    print(f"\nRare short shape (meta + single): {short_rare.shape}")

    # Build wide_rare: meta + binary multi-choice columns for rare questions
    rare_multi_groups = _build_multi_groups(rare_multi_keys)
    wide_rare = _build_wide(rare_multi_groups)
    print(f"Rare wide shape (meta + multi binary): {wide_rare.shape}")

    # FIX (v2): pass wide_main (NOT wide_rare) so that the Incentive Type
    # binary columns are available for build_incentive_category().
    # wide_rare does NOT contain those columns, so in v1 the incentive
    # category was always empty string for rare outputs.
    short_rare = add_computed_columns(short_rare, wide_main)
    rare_computed_cols = [
        c for c in short_rare.columns
        if c not in meta_df.columns and c not in rare_single_dict
    ]
    print(f"Rare computed columns added: {len(rare_computed_cols)}")
    # Copy computed columns into wide_rare
    for col in rare_computed_cols:
        wide_rare[col] = short_rare[col].values
    print(f"Rare wide shape (after computed columns): {wide_rare.shape}")

    # ============================================================
    # LONG SURVEYS — melt multi-choice from wide to long format
    # ============================================================
    # Long format has one row per (respondent x selected answer).
    # For example, if a driver selected 3 out of 5 incentive types,
    # they get 3 rows in the long DataFrame (one for each selection).
    # This format is useful for counting answer frequencies and plotting.

    # Grab just the computed columns to include in each long row
    main_comp_df = (short_main[main_computed_cols]
                    if main_computed_cols
                    else pd.DataFrame(index=short_main.index))
    rare_comp_df = (short_rare[rare_computed_cols]
                    if rare_computed_cols
                    else pd.DataFrame(index=short_rare.index))

    long_rows_main = []
    long_rows_rare = []

    # Iterate through every respondent row.  For each multi-choice question,
    # if the respondent selected that option (cell is not NaN), create a
    # long-format row with meta + computed columns + question/answer info.
    for idx in combined.index:
        meta_row = meta_df.loc[idx].to_dict()

        # Build main long rows
        base_main = {**meta_row,
                     **(main_comp_df.loc[idx].to_dict()
                        if not main_comp_df.empty else {})}
        for long_title, options in main_multi_groups.items():
            for key, norm_col, answer_value in options:
                # Only create a row if the respondent selected this option
                if pd.notna(combined.at[idx, norm_col]):
                    row = base_main.copy()
                    row["question"] = long_title
                    row["answer"] = answer_value
                    row["question_type"] = "multi_choice"
                    long_rows_main.append(row)

        # Build rare long rows (same logic, different question set)
        base_rare = {**meta_row,
                     **(rare_comp_df.loc[idx].to_dict()
                        if not rare_comp_df.empty else {})}
        for long_title, options in rare_multi_groups.items():
            for key, norm_col, answer_value in options:
                if pd.notna(combined.at[idx, norm_col]):
                    row = base_rare.copy()
                    row["question"] = long_title
                    row["answer"] = answer_value
                    row["question_type"] = "multi_choice"
                    long_rows_rare.append(row)

    long_main = pd.DataFrame(long_rows_main)
    long_rare = pd.DataFrame(long_rows_rare)
    print(f"\nMain long shape: {long_main.shape}")
    print(f"Rare  long shape: {long_rare.shape}")

    return short_main, wide_main, long_main, short_rare, wide_rare, long_rare


# ============================================================
# MAIN — orchestrates the full pipeline
# ============================================================

def main():
    """
    Entry point: load mapping, load raw files, validate, process, and save.

    The pipeline has built-in "guard rails":
        1. If any column headers are unmapped → save diagnostic CSV and stop
        2. If any answer values are unmapped → save diagnostic CSV and stop
        3. Only if everything is mapped → proceed to process and output

    This prevents silently producing outputs with uncoded values.
    """
    # Create the output directory if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Step 1: Load the column mapping (produced by generate_mapping.py)
    print("Loading mapping...")
    mapping = load_mapping(MAPPING_PATH)
    raw_to_key = build_raw_to_key(mapping)

    # Step 2: Load and combine all raw survey files
    print("Loading raw files...")
    combined, col_unique_vals, col_file_count = load_all_raw_files(RAW_DIR)

    # Step 3: Check for unmapped columns (new questions not yet in the mapping)
    print("Checking unmapped columns...")
    unmapped = find_unmapped_columns(
        col_unique_vals, col_file_count, raw_to_key)

    if unmapped:
        # Save the unmapped columns to a CSV for the user to review
        unmapped_df = pd.DataFrame(unmapped)
        out_path = os.path.join(OUTPUT_DIR, "unmapped_columns.csv")
        # utf-8-sig encoding adds a BOM (Byte Order Mark) so Excel opens it correctly
        unmapped_df.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"Unmapped columns found. Saved to {out_path}")
        return  # Exit early — user must update mapping before proceeding

    # Step 4: Check for unmapped answers (new answer options not yet in the mapping)
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
        return  # Exit early — user must update mapping before proceeding

    # Step 5: All validations passed — run the main processing pipeline
    print("All columns and answers mapped. Processing...")

    short_main, wide_main, long_main, short_rare, wide_rare, long_rare = \
        process_data(combined, mapping, raw_to_key)

    # Step 6: Save all six output CSVs
    # utf-8-sig encoding ensures Persian/Arabic text displays correctly in Excel
    short_main.to_csv(os.path.join(OUTPUT_DIR, "short_survey_main.csv"),
                      index=False, encoding="utf-8-sig")
    wide_main.to_csv(os.path.join(OUTPUT_DIR, "wide_survey_main.csv"),
                     index=False, encoding="utf-8-sig")
    long_main.to_csv(os.path.join(OUTPUT_DIR, "long_survey_main.csv"),
                     index=False, encoding="utf-8-sig")

    short_rare.to_csv(os.path.join(OUTPUT_DIR, "short_survey_rare.csv"),
                      index=False, encoding="utf-8-sig")
    wide_rare.to_csv(os.path.join(OUTPUT_DIR, "wide_survey_rare.csv"),
                     index=False, encoding="utf-8-sig")
    long_rare.to_csv(os.path.join(OUTPUT_DIR, "long_survey_rare.csv"),
                     index=False, encoding="utf-8-sig")

    print("\nDone.")
    print("  short_survey_main.csv -> meta + always/often single-choice + computed")
    print("  wide_survey_main.csv  -> meta + always/often multi-choice (binary) + computed")
    print("  long_survey_main.csv  -> meta + always/often multi-choice (melted) + computed")
    print("  short_survey_rare.csv -> meta + rare single-choice")
    print("  wide_survey_rare.csv  -> meta + rare multi-choice (binary)")
    print("  long_survey_rare.csv  -> meta + rare multi-choice (melted)")


if __name__ == "__main__":
    main()
