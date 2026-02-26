"""
Snapp Driver Survey Cleaner (Stable v2)
=======================================

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
OUTPUT_DIR = r"D:\OneDrive\Work\Driver Survey\cleaned2"

Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)


# =============================================================================
# FIXED METADATA MAP
# =============================================================================

FIXED_COLUMNS = {
    "شناسه پاسخ": "record_id",
    "وضعیت پاسخ": "response_status",
    "آدرس آی پی": "ip_address",
    "مهر زمان (mm/dd/yyyy)": "survey_datetime",
    "زمان لازم برای تکمیل (ثانیه)": "completion_seconds",
    "ثانیه عدد": "completion_seconds_num",
    "مرجع خارجی": "external_ref",
    "متغیر سفارشی 1": "custom_var_1",
    "متغیر سفارشی 2": "custom_var_2",
    "متغیر سفارشی 3": "custom_var_3",
    "متغیر سفارشی 4": "custom_var_4",
    "متغیر سفارشی 5": "custom_var_5",
    "ایمیل پاسخگو": "respondent_email",
    "لیست ایمیل": "email_list",
    "کد کشور": "country_code",
    "منطقه": "region",
    "کپی کردن": "copy_flag",
}

DROP_PREFIXES = [
    "کاربر گرامی سلام",
    "از وقتی که در اختیار ما قرار دادید",
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


def norm_key(col):
    s = fa_norm(col)
    s = re.sub(r"[^\w\u0600-\u06ff\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip().lower()


_FIXED_LOOKUP = {norm_key(k): v for k, v in FIXED_COLUMNS.items()}
_DROP_PREFIXES_NORM = [norm_key(p) for p in DROP_PREFIXES]


def should_drop(key):
    return any(key.startswith(p) for p in _DROP_PREFIXES_NORM)


# =============================================================================
# MULTI CHOICE SPLITTER
# =============================================================================

_LAST_DASH = re.compile(r"\s*[-–—]\s*(?=[^-–—]*$)")


def split_stem_option(col):
    col = str(col).split("\n")[0]
    norm = fa_norm(col)
    m = _LAST_DASH.search(norm)
    if m:
        stem = norm[:m.start()].strip()
        option = norm[m.end():].strip()
        if option and len(option) <= 120:
            return stem, option
    return norm, None


def safe_name(text, maxlen=60):
    s = re.sub(r"[^\w\u0600-\u06ff\s]", " ", str(text))
    s = re.sub(r"\s+", "_", s).strip("_")
    return s[:maxlen]


# =============================================================================
# COLUMN DEDUPLICATION
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
            df = pd.read_excel(f, nrows=3)
            for col in df.columns:
                key = norm_key(col)
                stem, option = split_stem_option(col)
                if key not in registry:
                    registry[key] = {
                        "stem": stem,
                        "option": option or "",
                        "files": 0,
                        "fixed": _FIXED_LOOKUP.get(key, ""),
                        "drop": should_drop(key),
                    }
                registry[key]["files"] += 1
        except Exception:
            print(f"Skipped {f.name}")

    rows = []
    for k, v in registry.items():
        rows.append({
            "norm_key": k,
            "stem": v["stem"],
            "option": v["option"],
            "fixed_name": v["fixed"],
            "drop": v["drop"],
            "appears_in": v["files"],
            "out_of": total,
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

    # Pass 1 – collect stems
    for f in files:
        df = pd.read_excel(f, nrows=1)
        for col in df.columns:
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
            print(f"Skipped {f.name}")
            traceback.print_exc()

    if not frames:
        print("No files loaded.")
        return

    merged = pd.concat(frames, ignore_index=True, sort=False)
    merged = collapse_multi_choice(merged)

    merged.to_csv(
        os.path.join(OUTPUT_DIR, "merged_wide.csv"),
        index=False,
        encoding="utf-8-sig"
    )

    long = to_long(merged)
    long.to_csv(
        os.path.join(OUTPUT_DIR, "merged_long.csv"),
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
    drop_cols = []
    mc_cols = []

    for col in df.columns:
        if col.startswith("_"):
            continue

        base = re.sub(r"__dup\d+$", "", col)
        key = norm_key(base)
        stem, option = split_stem_option(base)

        if should_drop(key):
            drop_cols.append(col)
            continue

        if key in _FIXED_LOOKUP:
            rename_map[col] = _FIXED_LOOKUP[key]
            continue

        if option and stem in all_mc_stems:
            new_name = f"_mc__{safe_name(stem)}__{safe_name(option)}"
            rename_map[col] = new_name
            mc_cols.append(col)
            continue

        rename_map[col] = safe_name(stem)

    df.drop(columns=drop_cols, inplace=True, errors="ignore")
    df.rename(columns=rename_map, inplace=True)

    df.columns = deduplicate_columns(df.columns)

    # Encode multi choice
    for col in [c for c in df.columns if c.startswith("_mc__")]:
        s = df[col]
        df[col] = ~(s.isna() | (
            s.astype(str).str.strip().isin(["", "nan", "NaN", "None"])
        ))

    if "survey_datetime" in df.columns:
        df["survey_datetime"] = pd.to_datetime(
            df["survey_datetime"],
            errors="coerce"
        )

    if "record_id" in df.columns:
        df.drop_duplicates(subset=["record_id"], inplace=True)

    return df


def collapse_multi_choice(df):

    groups = defaultdict(list)

    for col in df.columns:
        if col.startswith("_mc__"):
            stem = col.split("__")[1]
            groups[stem].append(col)

    for stem, cols in groups.items():
        bool_df = df[cols].fillna(False).astype(bool)
        df[stem + "_selected"] = bool_df.apply(
            lambda row: "|".join(
                [c.split("__")[2] for c, val in zip(cols, row) if val]
            ) or None,
            axis=1
        )
        df.drop(columns=cols, inplace=True)

    return df


def to_long(df):

    id_cols = [c for c in [
        "_source_file",
        "_week_label",
        "record_id",
        "survey_datetime"
    ] if c in df.columns]

    value_cols = [c for c in df.columns if c not in id_cols]

    long = df.melt(
        id_vars=id_cols,
        value_vars=value_cols,
        var_name="question_key",
        value_name="answer"
    )

    long = long[
        long["answer"].notna() &
        (long["answer"].astype(str).str.strip() != "")
    ]

    return long.reset_index(drop=True)


# =============================================================================
# EXPORT DB
# =============================================================================

def export_db():

    wide_path = os.path.join(OUTPUT_DIR, "merged_wide.csv")
    if not os.path.exists(wide_path):
        print("Run --clean first.")
        return

    df = pd.read_csv(wide_path, low_memory=False)

    meta_cols = [
        c for c in [
            "_source_file",
            "_week_label",
            "record_id",
            "survey_datetime",
            "response_status",
            "ip_address",
            "completion_seconds",
            "respondent_email",
            "country_code",
            "region"
        ] if c in df.columns
    ]

    q_cols = [c for c in df.columns if c not in meta_cols]

    surveys = df[meta_cols].copy()
    surveys.insert(0, "survey_id", range(1, len(surveys) + 1))
    surveys.to_csv(
        os.path.join(OUTPUT_DIR, "db_surveys.csv"),
        index=False,
        encoding="utf-8-sig"
    )

    df["survey_id"] = surveys["survey_id"].values

    responses = df[["survey_id"] + q_cols].melt(
        id_vars=["survey_id"],
        var_name="question_key",
        value_name="answer"
    )

    responses = responses[responses["answer"].notna()]
    responses.insert(0, "response_id", range(1, len(responses) + 1))

    responses.to_csv(
        os.path.join(OUTPUT_DIR, "db_responses.csv"),
        index=False,
        encoding="utf-8-sig"
    )

    print("DB export complete.")


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":

    mode = sys.argv[1] if len(sys.argv) > 1 else "--help"

    if mode == "--explore":
        explore_data()
    elif mode == "--clean":
        clean_and_merge()
    elif mode == "--export":
        export_db()
    elif mode == "--all":
        explore_data()
        clean_and_merge()
        export_db()
    else:
        print("Usage: python DS_cleaning.py --explore | --clean | --export | --all")
