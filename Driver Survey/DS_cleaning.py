"""
Snapp Driver Survey Cleaner (Stable v6 - Smart Other + Protected Metadata)
============================================================================
python DS_cleaning.py --explore
python DS_cleaning.py --clean
python DS_cleaning.py --export
python DS_cleaning.py --all
"""

import os
import re
import sys
import traceback
from pathlib import Path
from collections import defaultdict

import pandas as pd

# =============================================================================
# PATHS
# =============================================================================

RAW_DIR = r"D:\OneDrive\Work\Driver Survey\raw"
OUTPUT_DIR = r"D:\OneDrive\Work\Driver Survey\cleaned6"

Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

# =============================================================================
# FIXED METADATA MAP
# =============================================================================

FIXED_COLUMNS = {
    "شناسه پاسخ": "record_id",
    "آدرس آی پی": "ip_address",
    "مهر زمان (mm/dd/yyyy)": "survey_datetime",
    "زمان لازم برای تکمیل (ثانیه)": "completion_seconds",
    "کد کشور": "country_code",
    "منطقه": "region",
}

# These columns must be preserved and treated differently
PROTECTED_CUSTOM_COLUMNS = {
    "شناسه پاسخ",
    "آدرس آی پی",
    "مهر زمان (mm/dd/yyyy)",
    "زمان لازم برای تکمیل (ثانیه)",
    "کد کشور",
    "منطقه",
}

DROP_PREFIXES = [
    "کاربر گرامی سلام",
    "از وقتی که در اختیار ما قرار دادید",
]

# Columns containing these phrases become OTHER type
OTHER_TRIGGERS = [
    "اگر پاسخ",
    "بنویسید",
    "شرح دهید",
]

# =============================================================================
# TEXT NORMALIZATION
# =============================================================================

_AR_TO_FA = str.maketrans({
    "\u0643": "\u06a9",
    "\u064a": "\u06cc",
    "\u0629": "\u0647",
    "\u0624": "\u0648",
    "\u0625": "\u0627",
    "\u0623": "\u0627",
    "\u0671": "\u0627",
    "\u200c": " ",
    "\u200b": "",
    "\u200f": "",
    "\u200e": "",
    "\ufeff": "",
})

_DIACRITICS = re.compile(
    "[\u064b-\u065f\u0610-\u061a\u06d6-\u06dc\u06df-\u06e4\u06e7\u06e8\u06ea-\u06ed]"
)


def fa_norm(text):
    text = str(text)
    text = text.translate(_AR_TO_FA)
    text = _DIACRITICS.sub("", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def safe_name(s):
    s = fa_norm(s)
    s = re.sub(r"[^\w\u0600-\u06ff]", "_", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("_").lower()


def norm_key(col):
    s = fa_norm(col)
    s = re.sub(r"[^\w\u0600-\u06ff\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip().lower()


_FIXED_LOOKUP = {norm_key(k): v for k, v in FIXED_COLUMNS.items()}
_DROP_PREFIXES_NORM = [norm_key(p) for p in DROP_PREFIXES]


def should_drop(key):
    return any(key.startswith(p) for p in _DROP_PREFIXES_NORM)


def is_other_question(text):
    return any(trigger in text for trigger in OTHER_TRIGGERS)

# =============================================================================
# MULTI CHOICE SPLITTER (STRICT: ؟ + DASH ONLY)
# =============================================================================


_OPTION_LINE = re.compile(r"[-–—]\s*(.+)")


def split_stem_option(col):
    norm = fa_norm(col)

    if "؟" not in norm:
        return norm, None

    q_index = norm.rfind("؟")
    stem = norm[: q_index + 1].strip()

    remainder = norm[q_index + 1:]
    match = _OPTION_LINE.search(remainder)

    if match:
        option = match.group(1).strip()
        return stem, option

    return stem, None

# =============================================================================
# DEDUPLICATION
# =============================================================================


def deduplicate_columns(cols):
    seen = {}
    new_cols = []
    for col in cols:
        if col in seen:
            seen[col] += 1
            new_cols.append(f"{col}__dup{seen[col]}")
        else:
            seen[col] = 0
            new_cols.append(col)
    return new_cols

# =============================================================================
# EXPLORE
# =============================================================================


def explore_data():
    files = sorted(Path(RAW_DIR).glob("*.xls*"))
    if not files:
        print("No Excel files found.")
        return

    registry = {}
    total = len(files)

    for f in files:
        try:
            df = pd.read_excel(f, dtype=str)

            for col in df.columns:
                full_q = col
                key = norm_key(col)

                if full_q not in registry:

                    if col in PROTECTED_CUSTOM_COLUMNS:
                        q_type = "protected_meta"
                    elif is_other_question(col):
                        q_type = "other"
                    else:
                        stem, option = split_stem_option(col)
                        q_type = "multi_choice" if option else "single_choice"

                    registry[full_q] = {
                        "norm_key": key,
                        "question_type": q_type,
                        "files": 0,
                        "unique_values": set(),
                    }

                registry[full_q]["files"] += 1

                # track unique only for SINGLE CHOICE
                if registry[full_q]["question_type"] == "single_choice":
                    vals = (
                        df[col]
                        .dropna()
                        .astype(str)
                        .str.strip()
                        .loc[lambda s: s != ""]
                        .unique()
                    )
                    registry[full_q]["unique_values"].update(vals)

        except Exception:
            print(f"Skipped {f.name}")

    rows = []
    for q, v in registry.items():
        rows.append({
            "full_question_text": q,
            "question_type": v["question_type"],
            "appears_in": v["files"],
            "out_of": total,
            "unique_values": "|".join(sorted(v["unique_values"]))[:5000],
        })

    pd.DataFrame(rows).to_csv(
        os.path.join(OUTPUT_DIR, "column_report.csv"),
        index=False,
        encoding="utf-8-sig"
    )

    print("Column report generated.")

# =============================================================================
# CLEAN
# =============================================================================


def clean_and_merge():
    files = sorted(Path(RAW_DIR).glob("*.xls*"))
    if not files:
        print("No Excel files found.")
        return

    all_mc_stems = set()

    for f in files:
        df = pd.read_excel(f, nrows=1)
        for col in df.columns:
            if col in PROTECTED_CUSTOM_COLUMNS:
                continue
            if is_other_question(col):
                continue
            stem, option = split_stem_option(col)
            if option:
                all_mc_stems.add(stem)

    frames = []

    for f in files:
        try:
            df = pd.read_excel(f, dtype=str)
            df = clean_file(df, f.name, all_mc_stems)
            frames.append(df)
            print(f"Loaded {f.name} ({len(df)} rows)")
        except Exception:
            traceback.print_exc()

    if not frames:
        return

    merged = pd.concat(frames, ignore_index=True, sort=False)
    merged = collapse_multi_choice(merged)

    merged.to_csv(
        os.path.join(OUTPUT_DIR, "merged_wide.csv"),
        index=False,
        encoding="utf-8-sig"
    )

    print("Clean complete.")


def clean_file(df, filename, all_mc_stems):

    df = df.copy()
    df.columns = deduplicate_columns(df.columns)

    df.insert(0, "_source_file", filename)
    df.insert(1, "_week_label", Path(filename).stem)

    rename_map = {}

    for col in df.columns:
        if col.startswith("_"):
            continue

        if col in PROTECTED_CUSTOM_COLUMNS:
            rename_map[col] = _FIXED_LOOKUP.get(norm_key(col), safe_name(col))
            continue

        if is_other_question(col):
            rename_map[col] = safe_name(col)
            continue

        stem, option = split_stem_option(col)

        if option and stem in all_mc_stems:
            rename_map[col] = f"_mc__{safe_name(stem)}__{safe_name(option)}"
        else:
            rename_map[col] = safe_name(stem)

    df.rename(columns=rename_map, inplace=True)
    df.columns = deduplicate_columns(df.columns)

    for col in [c for c in df.columns if c.startswith("_mc__")]:
        s = df[col]
        df[col] = ~(s.isna() | (
            s.astype(str).str.strip().isin(["", "nan", "NaN", "None"])
        ))

    return df


def collapse_multi_choice(df):

    groups = defaultdict(list)

    for col in df.columns:
        if col.startswith("_mc__"):
            stem = col.split("__")[1]
            groups[stem].append(col)

    if not groups:
        return df

    # Build all new columns first (avoid fragmentation)
    new_columns = {}

    for stem, cols in groups.items():

        # TRUE if cell has any value (cleaner & no downcast warning)
        bool_df = df[cols].notna() & (
            df[cols].astype(str).apply(lambda s: s.str.strip() != "")
        )

        selected_series = bool_df.apply(
            lambda row: "|".join(
                [c.split("__")[2] for c, val in zip(cols, row) if val]
            ) or None,
            axis=1
        )

        new_columns[stem + "_selected"] = selected_series

    # Drop all MC columns at once
    all_mc_cols = [c for cols in groups.values() for c in cols]
    df = df.drop(columns=all_mc_cols)

    # Concatenate once (no fragmentation)
    df = pd.concat([df, pd.DataFrame(new_columns)], axis=1)

    # Optional but recommended: defragment memory
    df = df.copy()

    return df

# =============================================================================
# MAIN
# =============================================================================


if __name__ == "__main__":

    mode = sys.argv[1] if len(sys.argv) > 1 else "--help"

    if mode == "--explore":
        explore_data()
    elif mode == "--clean":
        clean_and_merge()
    elif mode == "--all":
        explore_data()
        clean_and_merge()
    else:
        print("Usage: python DS_cleaning.py --explore | --clean | --all")
