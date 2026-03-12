"""
Driver Survey Data Processing Pipeline  v2
==========================================
Fix vs v1: pass wide_main (not wide_rare) to add_computed_columns() for rare outputs,
so snapp_incentive_category / tapsi_incentive_category are correctly computed
instead of silently returning empty strings.

Reads raw weekly survey Excel files, validates columns against a JSON mapping,
renames headers, recodes answers, and produces six CSV outputs split by question
frequency (freq field in the mapping JSON):

  _main outputs  (freq: always + often)
  - short_survey_main.csv → meta + always/often single-choice + computed columns
  - wide_survey_main.csv  → meta + always/often multi-choice (binary 0/1) + computed columns
  - long_survey_main.csv  → meta + always/often multi-choice (melted/stacked rows) + computed columns

  _rare outputs  (freq: rare).
  - short_survey_rare.csv → meta + rare single-choice questions
  - wide_survey_rare.csv  → meta + rare multi-choice questions (binary 0/1)
  - long_survey_rare.csv  → meta + rare multi-choice questions (melted/stacked rows)

If unmapped columns are found, outputs unmapped_columns.csv and exits early.
If unmapped answers are found in single/multi questions, outputs
unmapped_answers.csv and exits early.

Column exclusion rules:
  - "customized_answer": "customized_answer" → excluded from all outputs.
  - type == "other" → excluded from all outputs (free-text "other" fields).

A computed "weeknumber" column is added based on the datetime column:
  If weekday is Saturday or Sunday (dayofweek==5 or 6), weeknumber = ISO week + 1,
  else weeknumber = ISO week.

Usage:
    python DS_cleaning.py
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
# Paths to raw data, the JSON mapping file, and where to write outputs.
# Change these to match your local folder structure.
RAW_DIR = r"D:\Work\Driver Survey\raw"
MAPPING_PATH = r"D:\Work\Driver Survey\DataSources\column_rename_mapping.json"
OUTPUT_DIR = r"D:\Work\Driver Survey\processed"


# ============================================================
# Helpers
# ============================================================

def normalize(text):
    """
    Basic Unicode normalization for column headers.

    NFKC normalization converts "compatibility" characters to their
    canonical equivalents (e.g. full-width letters → ASCII, ligatures
    expanded) and then strips leading/trailing whitespace.

    This is used on raw Excel column headers so that headers that look
    identical but differ in invisible Unicode encoding are treated as
    the same string.
    """
    if not isinstance(text, str):
        return str(text)
    return unicodedata.normalize("NFKC", text).strip()


# Translation tables for converting Persian and Arabic digit characters
# to their ASCII equivalents.
# str.maketrans(from_chars, to_chars) builds a character-level lookup
# table used by str.translate().
#   Persian digits: ۰ ۱ ۲ ۳ ۴ ۵ ۶ ۷ ۸ ۹  (Unicode U+06F0–U+06F9)
#   Arabic  digits: ٠ ١ ٢ ٣ ٤ ٥ ٦ ٧ ٨ ٩  (Unicode U+0660–U+0669)
_PERSIAN_TO_ASCII = str.maketrans("۰۱۲۳۴۵۶۷۸۹", "0123456789")
_ARABIC_TO_ASCII = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")


def fuzzy_normalize(text):
    """
    Aggressive normalization used when comparing survey *answer* strings.

    The pipeline needs to match raw answer text from Excel cells against
    the answer keys stored in the JSON mapping.  Respondents (or the
    survey tool) may introduce:
      - Different Unicode representations of the same character (NFKC fixes this)
      - Persian / Arabic digit characters instead of ASCII digits
      - Zero-width non-joiner (ZWNJ, U+200C) and other invisible formatting
        characters (Unicode category "Cf")
      - Arabic diacritics / vowel marks (Unicode category "Mn")
      - Punctuation that differs between the mapping and the raw data
      - Extra whitespace or mixed case

    Steps applied in order:
      1. NFKC normalization
      2. Persian and Arabic digits → ASCII digits
      3. Invisible formatting chars (Cf) and diacritics (Mn) → space
      4. Remove all punctuation / symbols (keep letters, digits, spaces)
      5. Collapse multiple spaces into one and strip edges
      6. Lowercase
    """
    if not isinstance(text, str):
        text = str(text)

    # Step 1: NFKC — canonical Unicode form
    text = unicodedata.normalize("NFKC", text)

    # Step 2: digit transliteration
    text = text.translate(_PERSIAN_TO_ASCII)
    text = text.translate(_ARABIC_TO_ASCII)

    # Step 3: replace invisible / diacritic characters with a space
    # unicodedata.category(c) returns a two-letter code:
    #   "Cf" = Format character (e.g. ZWNJ, zero-width space)
    #   "Mn" = Non-spacing mark (e.g. Arabic fathah diacritic)
    text = "".join(
        c if unicodedata.category(c) not in ("Cf", "Mn") else " "
        for c in text
    )

    # Step 4: strip punctuation — keep only word chars (\w) and spaces
    text = re.sub(r"[^\w\s]", "", text, flags=re.UNICODE)

    # Step 5: collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()

    # Step 6: lowercase for case-insensitive comparison
    return text.lower()


def load_mapping(path):
    """
    Load the JSON column/answer mapping file into a Python dict.

    The mapping file structure is expected to be:
    {
      "some_key": {
        "raw":     ["Raw Column Header 1", "Raw Column Header 2"],
        "long":    "Human-readable question title",
        "type":    "meta" | "single" | "multi" | "other",
        "freq":    "always" | "often" | "rare",   ← optional
        "answers": { "Raw Answer Text": "recoded_value", ... }
      },
      ...
    }
    """
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def is_customized_answer(meta):
    """
    Return True if this mapping entry represents a free-text
    "customized answer" column that should be excluded from all outputs.

    The convention in the JSON is:
        "answers": { "customized_answer": "customized_answer" }
    """
    answers = meta.get("answers")
    if isinstance(answers, dict):
        return (answers.get("customized_answer") == "customized_answer")
    return False


def build_raw_to_key(mapping):
    """
    Build a reverse lookup: normalized raw header → mapping key.

    The JSON mapping lists one or more raw column headers per key
    (because the same question may have been worded differently across
    survey waves).  This function inverts that relationship so we can
    quickly look up which mapping key corresponds to any raw header we
    encounter in the data.

    Returns a dict like:
        { "normalized raw header": "mapping_key", ... }
    """
    raw_to_key = {}
    for key, meta in mapping.items():
        for raw_header in meta.get("raw", []):
            # normalize() is applied so the lookup is robust to Unicode
            # differences between the JSON and the actual Excel headers.
            raw_to_key[normalize(raw_header)] = key
    return raw_to_key


def parse_datetime_column(series):
    """
    Parse a datetime column that may contain mixed formats:
      - Normal date/datetime strings: '2025/01/15', '2025-01-15 10:30:00'
      - Excel serial date numbers stored as strings: '45771.864...'
        (Excel counts days since 1899-12-30)
      - Empty strings or NaN

    Returns a Series of pd.Timestamp values (NaT where parsing fails).

    Why a custom parser instead of pd.to_datetime(...)?
    pd.to_datetime cannot handle Excel serial numbers, so we add a
    fallback that detects numeric strings in a plausible serial-date
    range (1000–100000 ≈ years 1902–2173) and converts them manually.
    """
    def parse_one(val):
        if pd.isna(val):
            return pd.NaT
        if isinstance(val, str):
            val = val.strip()
            if not val:
                return pd.NaT
            # Try standard datetime parsing first
            ts = pd.to_datetime(val, errors="coerce")
            if ts is not pd.NaT:
                return ts
            # Fallback: try interpreting as an Excel serial date float
            try:
                numeric = float(val)
                # Sanity-check: only treat as serial date if in a
                # reasonable range (avoids misinterpreting short numbers)
                if 1000 < numeric < 100000:
                    return pd.Timestamp("1899-12-30") + pd.Timedelta(days=numeric)
            except (ValueError, TypeError):
                pass
        return pd.NaT

    return series.apply(parse_one)


def compute_weeknumber(dt_series):
    """
    Compute a week number that replicates the Excel formula:
        =IF(WEEKDAY(date, 1)=7, WEEKNUM(date, 1)+1, WEEKNUM(date, 1))

    In Excel's WEEKDAY(date, 1): Sunday=1, Monday=2, ..., Saturday=7.
    So the condition "=7" means Saturday.

    In Excel's WEEKNUM(date, 1): weeks start on Sunday; the week
    containing January 1 is week 1.

    Why bump Saturday by 1?
    The survey is distributed on Saturdays.  Responses collected on a
    Saturday logically belong to the *next* week's batch, so the week
    number is incremented by 1 for Saturday dates.

    Implementation notes:
    - pandas dayofweek: Monday=0, Tuesday=1, ..., Saturday=5, Sunday=6
      (different from Excel's convention — we convert below)
    - We also bump Sunday (dayofweek=6) by
```python
    - We also bump Sunday (dayofweek=6) by 1 to match the Excel behavior.
    - Only processes non-NaT rows to avoid NaN propagation.

    Returns an Int64 Series (nullable integer) with the computed week numbers.
    """
    dt = dt_series
    valid = dt.notna()

    # Initialize result as nullable integer (Int64 allows pd.NA)
    result = pd.Series(pd.NA, index=dt.index, dtype="Int64")

    if valid.sum() == 0:
        return result

    dv = dt[valid]

    # Day of year (1-based): Jan 1 = 1, Jan 2 = 2, etc.
    doy = dv.dt.dayofyear

    # What weekday is January 1 of each year?
    # We need this to compute Excel-style WEEKNUM.
    jan1 = pd.to_datetime(
        dv.dt.year.astype(int).astype(str) + "-01-01",
        format="%Y-%m-%d"
    )
    # Convert pandas dayofweek (Mon=0..Sun=6) to Sunday-based (Sun=0..Sat=6)
    jan1_wday_sun = (jan1.dt.dayofweek + 1) % 7

    # Excel WEEKNUM formula (type 1, Sunday start):
    # weeknum = (day_of_year + jan1_weekday - 1) // 7 + 1
    weeknum = (doy + jan1_wday_sun - 1) // 7 + 1

    # If the date falls on Saturday (dayofweek=5) or Sunday (dayofweek=6),
    # increment the week number by 1.
    is_weekend = dv.dt.dayofweek.isin([5, 6])

    result[valid] = weeknum.where(~is_weekend, weeknum + 1).astype("Int64")

    return result


def load_all_raw_files(raw_dir):
    """
    Load all Excel and CSV files from the raw data directory and
    concatenate them into a single DataFrame.

    Also builds two helper dicts:
      - col_unique_vals: for each column, the set of unique non-null values
        across all files (used for validation)
      - col_file_count: how many files each column appeared in

    Steps:
      1. Find all .xlsx, .xls, .csv files in raw_dir
      2. Read each file (dtype=str to preserve raw text)
      3. Normalize column headers using normalize()
      4. Add a "_source_file" column to track which file each row came from
      5. Concatenate all DataFrames vertically
      6. Return the combined DataFrame + the two helper dicts
    """
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

        # Read as strings to avoid type coercion issues
        if fpath.endswith(".csv"):
            df = pd.read_csv(fpath, dtype=str)
        else:
            df = pd.read_excel(fpath, dtype=str)

        # Normalize column headers
        norm_cols = [normalize(c) for c in df.columns]
        df.columns = norm_cols
        df["_source_file"] = fname

        # Track unique values and file counts per column
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
    Validate that every unique answer found in single-choice and
    multi-choice columns has a corresponding entry in the JSON mapping.

    For each column:
      1. Look up its mapping key
      2. If it's a single or multi question, get the expected answers
      3. Fuzzy-normalize both the expected answers and the raw data answers
      4. Report any raw answers that don't match any expected answer

    Returns a list of dicts describing each unmapped answer:
        [
          {
            "mapping_key": "...",
            "question_type": "single" or "multi",
            "long_name": "...",
            "unmapped_raw_answer": "..."
          },
          ...
        ]

    Why fuzzy normalization?
    Survey responses may have minor differences in whitespace, punctuation,
    or Unicode encoding compared to the mapping file.  fuzzy_normalize()
    makes the comparison robust to these variations.
    """
    unmapped = []

    for norm_col, unique_vals in col_unique_vals.items():
        # Skip columns not in the mapping
        if norm_col not in raw_to_key:
            continue
        key = raw_to_key[norm_col]
        meta = mapping.get(key)
        if not meta:
            continue

        qtype = meta.get("type")
        # Only validate single and multi questions (not meta or other)
        if qtype not in ("single", "multi"):
            continue

        # Skip columns that will be excluded during processing
        if is_customized_answer(meta):
            continue
        if meta.get("type") == "other":
            continue

        answers = meta.get("answers")
        if not answers:
            continue

        # Build a set of fuzzy-normalized expected answer keys
        fuzzy_expected = {fuzzy_normalize(k) for k in answers.keys()}

        # Check each unique raw answer against the expected set
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
    Identify any columns in the raw data that don't have a corresponding
    entry in the JSON mapping.

    Returns a list of dicts with diagnostic info:
        [
          {
            "raw_column_header": "...",
            "unique_answers_sample": "answer1 | answer2 | ...",
            "num_unique_answers": 123,
            "num_files_appeared": 5
          },
          ...
        ]

    This helps you identify new questions that were added to the survey
    but haven't been added to the mapping file yet.
    """
    unmapped = []

    for col in sorted(col_unique_vals.keys()):
        # Skip the internal tracking
        # Skip the internal tracking column added by load_all_raw_files()
        if col == "_source_file":
            continue
        if col not in raw_to_key:
            unique_vals = sorted(col_unique_vals[col])
            sample = " | ".join(str(v) for v in unique_vals[:10])
            unmapped.append({
                "raw_column_header": col,
                "unique_answers_sample": sample,
                "num_unique_answers": len(unique_vals),
                "num_files_appeared": col_file_count.get(col, 0),
            })

    return unmapped


# ============================================================
# Computed Columns
# ============================================================

def build_incentive_category(row, prefix):
    """
    Derive a single incentive category label from a set of binary
    indicator columns that were produced by multi-choice recoding.

    Multi-choice questions are exploded into one binary column per
    option (0 or 1).  This function reads those binary columns and
    returns a single string label describing which incentive type
    the driver received.

    prefix is either "snapp" or "tapsi" — the two ride-hailing platforms
    tracked in the survey.

    Priority order (first match wins):
      1. cash_and_prize  → driver received both cash and a prize
      2. cash            → cash only
      3. prize           → prize only
      4. no_incentive    → explicitly answered "no incentive"
      5. ""              → none of the above columns are present / all zero

    The column names follow the pattern:
        {prefix}_incentive_cash_and_prize
        {prefix}_incentive_cash
        {prefix}_incentive_prize
        {prefix}_incentive_no_incentive
    """
    def get(col):
        # Return the integer value of a binary column, or 0 if missing
        return int(row.get(f"{prefix}_incentive_{col}", 0) or 0)

    if get("cash_and_prize"):
        return "cash_and_prize"
    if get("cash"):
        return "cash"
    if get("prize"):
        return "prize"
    if get("no_incentive"):
        return "no_incentive"
    return ""


def add_computed_columns(df, wide_main):
    """
    Add all derived / computed columns to a DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        The target DataFrame to add columns to.
        This may be short_main, wide_main, long_main,
        short_rare, wide_rare, or long_rare.
    wide_main : pd.DataFrame
        The wide-format main DataFrame.  This is always passed because
        the binary indicator columns needed for incentive category
        computation only exist in wide_main (they come from multi-choice
        questions with freq="always"/"often").

    FIX vs v1:
        v1 passed `df` itself to look up incentive columns, which failed
        silently for rare outputs because those DataFrames don't contain
        the multi-choice binary columns.  v2 always reads incentive
        columns from wide_main regardless of which df is being enriched.

    Computed columns added:
      - weeknumber              : survey week number (see compute_weeknumber)
      - snapp_incentive_category: derived incentive label for Snapp
      - tapsi_incentive_category: derived incentive label for Tapsi
      - snapp_ride_count_num    : numeric version of snapp ride count bucket
      - tapsi_ride_count_num    : numeric version of tapsi ride count bucket
      - other_ride_count_num    : numeric version of other platform ride count
      - is_professional_driver  : 1 if driver drives professionally, else 0
      - is_young_driver         : 1 if age group is 18-29, else 0
      - is_senior_driver        : 1 if age group is 60+, else 0
      - is_low_income           : 1 if income bracket is the lowest tier, else 0
      - is_high_income          : 1 if income bracket is the highest tier, else 0
    """

    # ------------------------------------------------------------------
    # weeknumber
    # ------------------------------------------------------------------
    # Find the datetime column (mapped from the meta question with
    # a raw header containing "date" or "time").
    # We look for a column whose name contains "datetime" or "date".
    datetime_col = None
    for col in df.columns:
        if "datetime" in col.lower() or (col.lower().endswith("date")):
            datetime_col = col
            break

    if datetime_col:
        parsed = parse_datetime_column(df[datetime_col])
        df["weeknumber"] = compute_weeknumber(parsed)
    else:
        df["weeknumber"] = pd.NA

    # ------------------------------------------------------------------
    # Incentive categories
    # ------------------------------------------------------------------
    # Build an index-aligned lookup from wide_main so we can map
    # incentive values back onto df (which may have the same index
    # if it shares rows with wide_main, or a different one for long format).
    #
    # We use wide_main.index to align; for long format the index is
    # repeated (one row per answer option), so we reindex by the
    # original row index stored in the long DataFrame.

    def get_incentive_series(platform):
        """
        Compute the incentive category for every row in wide_main,
        then align the result to df's index.
        """
        col_name = f"{platform}_incentive_category"
        if col_name in wide_main.columns:
            # Already computed — just align
            return wide_main[col_name].reindex(df.index)

        # Compute row-by-row from binary columns in wide_main
        cats = wide_main.apply(
            lambda row: build_incentive_category(row, platform), axis=1
        )
        # Align to df's index (handles long format where rows are repeated)
        return cats.reindex(df.index)

    df["snapp_incentive_category"] = get_incentive_series("snapp")
    df["tapsi_incentive_category"] = get_incentive_series("tapsi")

    # ------------------------------------------------------------------
    # Ride count numeric conversions
    # ------------------------------------------------------------------
    # Survey answers for ride counts are ordinal buckets like
    # "10 to 20 rides".  We convert them to the midpoint integer
    # for quantitative analysis.
    #
    # Mapping format: { "recoded_answer_value": midpoint_integer }

    ride_count_map = {
        "less_than_10":  5,
        "10_to_20":     15,
        "20_to_30":     25,
        "30_to_40":     35,
        "40_to_50":     45,
        "more_than_50": 60,
    }

    for platform, col_name in [
        ("snapp", "snapp_ride_count_num"),
        ("tapsi", "tapsi_ride_count_num"),
        ("other", "other_ride_count_num"),
    ]:
        src_col = f"{platform}_ride_count"
        if src_col in df.columns:
            df[col_name] = df[src_col].map(ride_count_map)
        else:
            df[col_name] = pd.NA

    # ------------------------------------------------------------------
    # Demographic indicator flags
    # ------------------------------------------------------------------
    # Binary (0/1) flags derived from recoded categorical columns.
    # These make it easier to filter or aggregate in downstream analysis.

    # is_professional_driver: 1 if the driver's primary occupation is driving
    if "occupation" in df.columns:
        df["is_professional_driver"] = (
            df["occupation"] == "professional_driver"
        ).astype(int)
    else:
        df["is_professional_driver"] = pd.NA

    # is_young_driver: 1 if age group is 18–29
    if "age_group" in df.columns:
        df["is_young_driver"] = (
            df["age_group"] == "18_to_29"
        ).astype(int)
    else:
        df["is_young_driver"] = pd.NA

    # is_senior_driver: 1 if age group is 60 or older
    if "age_group" in df.columns:
        df["is_senior_driver"] = (
            df["age_group"] == "60_plus"
        ).astype(int)
    else:
        df["is_senior_driver"] = pd.NA

    # is_low_income: 1 if income bracket is the lowest tier
    if "income_bracket" in df.columns:
        df["is_low_income"] = (
            df["income_bracket"] == "tier_1"
        ).astype(int)
    else:
        df["is_low_income"] = pd.NA

    # is_high_income: 1 if income bracket is the highest tier
    if "income_bracket" in df.columns:
        df["is_high_income"] = (
            df["income_bracket"] == "tier_5"
        ).astype(int)
    else:
        df["is_high_income"] = pd.NA

    return df


# ============================================================
# Main Processing
# ============================================================

def process_data(combined, mapping):
    """
    Core processing function.  Takes the combined raw DataFrame and
    the loaded mapping dict, and returns six processed DataFrames.

    Returns
    -------
    short_main, wide_main, long_main,
    short_rare, wide_rare, long_rare

    Processing steps:
      1. Build reverse lookup (raw header → mapping key)
      2. Filter columns: keep only those present in the mapping,
         drop excluded types (other, customized_answer)
      3. Rename columns from raw headers to mapping keys
      4. Classify columns by type (meta, single, multi) and
         frequency (main vs rare)
      5. Recode answers for single and multi columns
      6. Validate rows: drop rows with unexpected answer values
      7. Build short (meta + single), wide (meta + multi binary),
         and long (meta + multi melted) DataFrames for each frequency
      8. Add computed columns to all six outputs
    """

    raw_to_key = build_raw_to_key(mapping)

    # ------------------------------------------------------------------
    # Step 1: Filter to mapped columns only
    # ------------------------------------------------------------------
    # Keep only columns whose normalized header appears in raw_to_key.
    # Also keep the internal _source_file column.
    mapped_cols = [
        c for c in combined.columns
        if c in raw_to_key or c == "_source_file"
    ]
    df = combined[mapped_cols].copy()

    # ------------------------------------------------------------------
    # Step 2: Rename columns to mapping keys
    # ------------------------------------------------------------------
    rename_map = {c: raw_to_key[c] for c in df.columns if c in raw_to_key}
    df.rename(columns=rename_map, inplace=True)

    # ------------------------------------------------------------------
    # Step 3: Classify columns by type and frequency
    # ------------------------------------------------------------------
    # We build four lists of column names:
    #   meta_cols        : identifier / timestamp columns (type="meta")
    #   single_main_cols : single-choice, freq always/often (or unset)
    #   single_rare_cols : single-choice, freq rare
    #   multi_main_cols  : multi-choice, freq always/often (or unset)
    #   multi_rare_cols  : multi-choice, freq rare

    meta_cols = []
    single_main_cols = []
    single_rare_cols = []
    multi_main_cols = []
    multi_rare_cols = []

    for key, meta in mapping.items():
        # Skip keys that don't appear as columns in our data
        if key not in df.columns:
            continue

        qtype = meta.get("type")
        freq = meta.get("freq", "always")   # default to "always" if not set

        # Apply exclusion rules
        if qtype == "other":
            continue
        if is_customized_answer(meta):
            continue

        if qtype == "meta":
            meta_cols.append(key)
        elif qtype == "single":
            if freq == "rare":
                single_rare_cols.append(key)
            else:
                single_main_cols.append(key)
        elif qtype == "multi":
            if freq == "rare":
                multi_rare_cols.append(key)
            else:
                multi_main_cols.append(key)

    # ------------------------------------------------------------------
    # Step 4: Recode answers
    # ------------------------------------------------------------------
    # For each single and multi column, replace raw answer text with
    # the recoded value from the mapping.
    # fuzzy_normalize() is used on both sides of the lookup so minor
    # text differences don't cause mismatches.

    def recode_column(series, answers_dict):
        """
        Replace raw answer strings with recoded values.

        Builds a fuzzy-normalized lookup:
            { fuzzy_normalize(raw_answer): recoded_value }
        Then maps each cell through that lookup.
        Cells that don't match any key become NaN.
        """
        fuzzy_map = {
            fuzzy_normalize(k): v
            for k, v in answers_dict.items()
        }
        return series.map(
            lambda x: fuzzy_map.get(fuzzy_normalize(x), np.nan)
            if pd.notna(x) else np.nan
        )

    all_question_cols = (
        single_main_cols + single_rare_cols +
        multi_main_cols + multi_rare_cols
    )

    for col in all_question_cols:
        answers = mapping[col].get("answers", {})
        if answers:
            df[col] = recode_column(df[col], answers)

    # ------------------------------------------------------------------
    # Step 5: Validate rows
    # ------------------------------------------------------------------
    # Drop rows where a single-choice column contains an unexpected value
    # (i.e., NaN after recoding, which means the raw answer wasn't in
    # the mapping).  This removes corrupted or test responses.
    #
    # Multi-choice columns are not used for row filtering because a
    # respondent may legitimately leave some options blank.

    for col in single_main_cols + single_rare_cols:
        before = len(df)
        df = df[df[col].notna()]
        dropped = before - len(df)
        if dropped:
            print(f"  Dropped {dropped} rows due to invalid values in '{col}'")

    df.reset_index(drop=True, inplace=True)

    # ------------------------------------------------------------------
    # Step 6: Build wide multi-choice columns (binary 0/1)
    # ------------------------------------------------------------------
    # Multi-choice questions allow multiple selections.  In the raw data
    # each option is a separate column already (one column per option).
    # After recoding, each cell contains either the recoded option value
    # or NaN (not selected).
    #
    # We convert to binary: 1 if the option was selected, 0 otherwise.

    for col in multi_main_cols + multi_rare_cols:
        df[col] = df[col].notna().astype(int)

    # ------------------------------------------------------------------
    # Step 7: Assemble output DataFrames
    # ------------------------------------------------------------------

    # --- SHORT format: meta + single-choice columns ---
    # One row per respondent, one column per single-choice question.
    short_main = df[meta_cols + single_main_cols].copy()
    short_rare = df[meta_cols + single_rare_cols].copy()

    # --- WIDE format: meta + multi-choice binary columns ---
    # One row per respondent, one column per multi-choice option (0/1).
    wide_main = df[meta_cols + multi_main_cols].copy()
    wide_rare = df[meta_cols + multi_rare_cols].copy()

    # --- LONG format: melt multi-choice columns into rows ---
    # Each selected option becomes its own row.
    # Columns: meta columns + "question" (column name) + "selected" (0/1)
    #
    # pd.melt() unpivots from wide to long:
    #   id_vars   = columns to keep as-is (meta columns)
    #   value_vars = columns to melt (multi-choice binary columns)
    #   var_name  = name for the new "variable" column
    #   value_name= name for the new "value" column

    long_main = pd.melt(
        df[meta_cols + multi_main_cols],
        id_vars=meta_cols,
        value_vars=multi_main_cols,
        var_name="question",
        value_name="selected",
    )

    long_rare = pd.melt(
        df[meta_cols + multi_rare_cols],
        id_vars=meta_cols,
        value_vars=multi_rare_cols,
        var_name="question",
        value_name="selected",
    )

    # ------------------------------------------------------------------
    # Step 8: Add computed columns
    # ------------------------------------------------------------------
    # FIX (v1 → v2): always pass wide_main as the second argument so
    # that incentive binary columns are available regardless of which
    # output DataFrame is being enriched.

    short_main = add_computed_columns(short_main, wide_main)
    wide_main = add_computed_columns(wide_main,  wide_main)
    long_main = add_computed_columns(long_main,  wide_main)

    short_rare = add_computed_columns(short_rare, wide_main)
    wide_rare = add_computed_columns(wide_rare,  wide_main)
    long_rare = add_computed_columns(long_rare,  wide_main)

    return short_main, wide_main, long_main, short_rare, wide_rare, long_rare


# ============================================================
# Entry Point
# ============================================================

def main():
    """
    Orchestrates the full pipeline:
      1. Load all raw files
      2. Validate columns (exit early if unmapped columns found)
      3. Validate answers (exit early if unmapped answers found)
      4. Process data into six output DataFrames
      5. Write all six CSVs to OUTPUT_DIR
    """

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # ------------------------------------------------------------------
    # Step 1: Load raw data
    # ------------------------------------------------------------------
    print("Loading raw files...")
    combined, col_unique_vals, col_file_count = load_all_raw_files(RAW_DIR)

    # ------------------------------------------------------------------
    # Step 2: Load mapping
    # ------------------------------------------------------------------
    print("Loading mapping...")
    mapping = load_mapping(MAPPING_PATH)
    raw_to_key = build_raw_to_key(mapping)

    # ------------------------------------------------------------------
    # Step 3: Validate columns
    # ------------------------------------------------------------------
    print("Validating columns...")
    unmapped_cols = find_unmapped_columns(
        col_unique_vals, col_file_count, raw_to_key)

    if unmapped_cols:
        out_path = os.path.join(OUTPUT_DIR, "unmapped_columns.csv")
        pd.DataFrame(unmapped_cols).to_csv(
            out_path, index=False, encoding="utf-8-sig")
        print(
            f"STOPPED: {len(unmapped_cols)} unmapped column(s) found.\n"
            f"Review: {out_path}\n"
            "Add them to the mapping JSON and re-run."
        )
        sys.exit(1)

    # ------------------------------------------------------------------
    # Step 4: Validate answers
    # ------------------------------------------------------------------
    print("Validating answers...")
    unmapped_ans = find_unmapped_answers(col_unique_vals, mapping, raw_to_key)

    if unmapped_ans:
        out_path = os.path.join(OUTPUT_DIR, "unmapped_answers.csv")
        pd.DataFrame(unmapped_ans).to_csv(
            out_path, index=False, encoding="utf-8-sig")
        print(
            f"STOPPED: {len(unmapped_ans)} unmapped answer(s) found.\n"
            f"Review: {out_path}\n"
            "Add them to the mapping JSON and re-run."
        )
        sys.exit(1)

    # ------------------------------------------------------------------
    # Step 5: Process data
    # ------------------------------------------------------------------
    print("Processing data...")
    short_main, wide_main, long_main, short_rare, wide_rare, long_rare = (
        process_data(combined, mapping)
    )

    # ------------------------------------------------------------------
    # Step 6: Write outputs
    # ------------------------------------------------------------------
    # utf-8-sig encoding adds a BOM (Byte Order Mark) so Excel opens
    # the CSV correctly without garbling Persian/Arabic characters.
    outputs = {
        "short_survey_main.csv": short_main,
        "wide_survey_main.csv":  wide_main,
        "long_survey_main.csv":  long_main,
        "short_survey_rare.csv": short_rare,
        "wide_survey_rare.csv":  wide_rare,
        "long_survey_rare.csv":  long_rare,
    }

    for fname, df in outputs.items():
        fpath = os.path.join(OUTPUT_DIR, fname)
        df.to_csv(fpath, index=False, encoding="utf-8-sig")
        print(f"  Saved {fname}  ({len(df)} rows × {len(df.columns)} cols)")

    print("Done.")


if __name__ == "__main__":
    main()
