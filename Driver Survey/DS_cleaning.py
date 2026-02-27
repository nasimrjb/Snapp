"""
Snapp Driver Survey Cleaner (Stable v7 - with English Translation)
===================================================================
Adds two new outputs on top of v6:
  - column_report_translated.csv  : column_report with english_column_name + unique_values_english
  - merged_wide_translated.csv    : merged_wide with English column names and cell values

Usage:
    python DS_cleaning.py --explore          # column_report.csv only
    python DS_cleaning.py --clean            # merged_wide.csv only
    python DS_cleaning.py --translate        # translated versions of both (requires above outputs)
    python DS_cleaning.py --export           # (reserved)
    python DS_cleaning.py --all              # explore + clean + translate
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
OUTPUT_DIR = r"D:\OneDrive\Work\Driver Survey\cleaned7"
# <-- path to your mapping xlsx
MAPPING_FILE = r"D:\OneDrive\Work\Driver Survey\DataSources\column_rename.xlsx"

Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

# =============================================================================
# FIXED METADATA MAP
# =============================================================================

FIXED_COLUMNS = {
    "شناسه پاسخ":                   "record_id",
    "آدرس آی پی":                   "ip_address",
    "مهر زمان (mm/dd/yyyy)":        "survey_datetime",
    "زمان لازم برای تکمیل (ثانیه)": "completion_seconds",
    "کد کشور":                       "country_code",
    "منطقه":                         "region",
}

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
    r"[\u064b-\u065f\u0610-\u061a\u06d6-\u06dc\u06df-\u06e4\u06e7\u06e8\u06ea-\u06ed]"
)


def fa_norm(text: str) -> str:
    text = str(text).translate(_AR_TO_FA)
    text = _DIACRITICS.sub("", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def safe_name(s: str) -> str:
    s = fa_norm(s)
    s = re.sub(r"[^\w\u0600-\u06ff]", "_", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("_").lower()


def norm_key(col: str) -> str:
    s = fa_norm(col)
    s = re.sub(r"[^\w\u0600-\u06ff\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip().lower()


_FIXED_LOOKUP = {norm_key(k): v for k, v in FIXED_COLUMNS.items()}
_DROP_PREFIXES_NORM = [norm_key(p) for p in DROP_PREFIXES]


def should_drop(key: str) -> bool:
    return any(key.startswith(p) for p in _DROP_PREFIXES_NORM)


def is_other_question(text: str) -> bool:
    return any(trigger in text for trigger in OTHER_TRIGGERS)


# =============================================================================
# MULTI-CHOICE SPLITTER  (strict: ؟ + dash only)
# =============================================================================

_OPTION_LINE = re.compile(r"[-–—]\s*(.+)")


def split_stem_option(col: str):
    norm = fa_norm(col)
    if "؟" not in norm:
        return norm, None
    q_index = norm.rfind("؟")
    stem = norm[: q_index + 1].strip()
    remainder = norm[q_index + 1:]
    match = _OPTION_LINE.search(remainder)
    if match:
        return stem, match.group(1).strip()
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
                        "norm_key":     key,
                        "question_type": q_type,
                        "files":         0,
                        "unique_values": set(),
                    }

                registry[full_q]["files"] += 1

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
            "question_type":      v["question_type"],
            "appears_in":         v["files"],
            "out_of":             total,
            "unique_values":      "|".join(sorted(v["unique_values"]))[:5000],
        })

    pd.DataFrame(rows).to_csv(
        os.path.join(OUTPUT_DIR, "column_report.csv"),
        index=False,
        encoding="utf-8-sig",
    )
    print("column_report.csv generated.")


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
        encoding="utf-8-sig",
    )
    print("merged_wide.csv generated.")


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
            s.astype(str).str.strip().isin(["", "nan", "NaN", "None"])))

    return df


def collapse_multi_choice(df):
    groups = defaultdict(list)
    for col in df.columns:
        if col.startswith("_mc__"):
            stem = col.split("__")[1]
            groups[stem].append(col)

    if not groups:
        return df

    new_columns = {}
    for stem, cols in groups.items():
        bool_df = df[cols].notna() & (
            df[cols].astype(str).apply(lambda s: s.str.strip() != "")
        )
        selected_series = bool_df.apply(
            lambda row: "|".join(
                [c.split("__")[2] for c, val in zip(cols, row) if val]
            ) or None,
            axis=1,
        )
        new_columns[stem + "_selected"] = selected_series

    all_mc_cols = [c for cols in groups.values() for c in cols]
    df = df.drop(columns=all_mc_cols)
    df = pd.concat([df, pd.DataFrame(new_columns)], axis=1)
    return df.copy()


# =============================================================================
# TRANSLATION HELPERS  (new in v7)
# =============================================================================


def _load_mapping(mapping_file: str):
    """
    Load column_rename.xlsx and return:
        q_rename      : {Persian full question text -> English column name}
        answer_maps   : {English column name -> {Persian answer -> English answer}}
        safe_to_eng   : {safe_name(Persian question) -> English column name}
        flat_answer_map: {fa_norm(Persian answer) -> English answer}  (cross-question fallback)
    """
    qs = pd.read_excel(
        mapping_file, sheet_name="questions",         header=None)
    ra = pd.read_excel(
        mapping_file, sheet_name="replaced_answers",  header=None)

    # ── question rename map ──────────────────────────────────────────────────
    q_rename = {}
    for i, row in qs.iterrows():
        persian_q = str(row[0]).strip() if pd.notna(row[0]) else ""
        eng_name = str(row[1]).strip() if pd.notna(row[1]) else ""
        if persian_q and eng_name:
            q_rename[persian_q] = eng_name

    # ── per-question answer maps ─────────────────────────────────────────────
    answer_maps = {}
    for i, row_q in qs.iterrows():
        persian_q = str(row_q[0]).strip() if pd.notna(row_q[0]) else ""
        eng_name = str(row_q[1]).strip() if pd.notna(row_q[1]) else ""
        if not eng_name or not persian_q or i >= len(ra):
            continue
        row_r = ra.iloc[i]
        persian_ans = [str(v).strip() for v in row_q[2:] if pd.notna(v)]
        english_ans = [str(v).strip() for v in row_r[2:] if pd.notna(v)]
        if persian_ans and english_ans and len(persian_ans) == len(english_ans):
            answer_maps[eng_name] = dict(zip(persian_ans, english_ans))

    # ── derived lookups ──────────────────────────────────────────────────────
    safe_to_eng = {safe_name(pk): ev for pk, ev in q_rename.items()}
    q_rename_norm = {fa_norm(pk): ev for pk, ev in q_rename.items()}

    flat_answer_map = {}
    for amap in answer_maps.values():
        for persian_a, english_a in amap.items():
            norm = fa_norm(persian_a)
            if norm not in flat_answer_map:
                flat_answer_map[norm] = english_a

    return q_rename_norm, answer_maps, safe_to_eng, flat_answer_map


def _map_question(full_q: str, q_rename_norm: dict, safe_to_eng: dict):
    norm = fa_norm(full_q)
    if norm in q_rename_norm:
        return q_rename_norm[norm]
    sn = safe_name(full_q)
    return safe_to_eng.get(sn)


def _translate_unique_values(eng_col, uv_str, answer_maps: dict, flat_answer_map: dict):
    if not isinstance(uv_str, str) or not uv_str.strip():
        return uv_str
    amap = answer_maps.get(eng_col, {}) if eng_col else {}
    result = []
    for v in uv_str.split("|"):
        v = v.strip()
        if v in amap:
            result.append(amap[v])
        else:
            norm = fa_norm(v)
            result.append(flat_answer_map.get(norm, v))
    return "|".join(result)


def _translate_cell(val, col: str, answer_maps: dict, safe_to_eng: dict, flat_answer_map: dict):
    """Translate a single cell value to English."""
    if not isinstance(val, str):
        return val
    v = val.strip()
    if not v or v in ("nan", "None", "True", "False"):
        return val

    # Col-specific answer map
    amap = answer_maps.get(col, {})
    if v in amap:
        return amap[v]
    norm_v = fa_norm(v)
    for pk, ev in amap.items():
        if fa_norm(pk) == norm_v:
            return ev

    # Pipe-separated MC selected values
    if "|" in v:
        parts = []
        for p in v.split("|"):
            p = p.strip()
            if p in safe_to_eng:
                parts.append(safe_to_eng[p])
            else:
                parts.append(flat_answer_map.get(fa_norm(p), p))
        return "|".join(parts)

    # Cross-question flat fallback
    return flat_answer_map.get(norm_v, val)


# =============================================================================
# TRANSLATE
# =============================================================================

_SKIP_TRANSLATE_COLS = {
    "_source_file", "_week_label", "record_id",
    "ip_address", "survey_datetime", "completion_seconds",
}


def translate_outputs():
    if not Path(MAPPING_FILE).exists():
        print(f"Mapping file not found: {MAPPING_FILE}")
        return

    cr_path = os.path.join(OUTPUT_DIR, "column_report.csv")
    mw_path = os.path.join(OUTPUT_DIR, "merged_wide.csv")

    if not Path(cr_path).exists():
        print("column_report.csv not found — run --explore first.")
        return
    if not Path(mw_path).exists():
        print("merged_wide.csv not found — run --clean first.")
        return

    print("Loading mapping file...")
    q_rename_norm, answer_maps, safe_to_eng, flat_answer_map = _load_mapping(
        MAPPING_FILE)

    # ── Translate column_report.csv ──────────────────────────────────────────
    cr = pd.read_csv(cr_path)

    cr["english_column_name"] = cr["full_question_text"].apply(
        lambda q: _map_question(q, q_rename_norm, safe_to_eng)
    )
    cr["unique_values_english"] = cr.apply(
        lambda r: _translate_unique_values(
            r["english_column_name"], r["unique_values"], answer_maps, flat_answer_map
        ),
        axis=1,
    )

    cr_out = cr[[
        "full_question_text", "english_column_name", "question_type",
        "appears_in", "out_of", "unique_values", "unique_values_english",
    ]]
    cr_out_path = os.path.join(OUTPUT_DIR, "column_report_translated.csv")
    cr_out.to_csv(cr_out_path, index=False, encoding="utf-8-sig")
    mapped = cr_out["english_column_name"].notna().sum()
    print(
        f"column_report_translated.csv written  ({mapped}/{len(cr_out)} questions mapped)")

    # ── Translate merged_wide.csv ────────────────────────────────────────────
    mw = pd.read_csv(mw_path, dtype=str)

    # Rename columns
    col_rename_mw = {}
    for col in mw.columns:
        if col.startswith("_") or col in _SKIP_TRANSLATE_COLS:
            continue
        if col in safe_to_eng:
            col_rename_mw[col] = safe_to_eng[col]
        elif col.endswith("_selected"):
            base = col[:-9]  # strip "_selected"
            if base in safe_to_eng:
                col_rename_mw[col] = safe_to_eng[base] + "_selected"

    mw = mw.rename(columns=col_rename_mw)
    print(f"  Columns renamed: {len(col_rename_mw)}")

    remaining_persian = [
        c for c in mw.columns
        if any("\u0600" <= ch <= "\u06ff" for ch in c)
    ]
    if remaining_persian:
        print(
            f"  Columns without mapping ({len(remaining_persian)}): {remaining_persian[:5]}")

    # Translate cell values
    for col in mw.columns:
        if col in _SKIP_TRANSLATE_COLS or col.startswith("_"):
            continue
        mw[col] = mw[col].apply(
            lambda v: _translate_cell(
                v, col, answer_maps, safe_to_eng, flat_answer_map)
        )

    mw_out_path = os.path.join(OUTPUT_DIR, "merged_wide_translated.csv")
    mw.to_csv(mw_out_path, index=False, encoding="utf-8-sig")
    print(
        f"merged_wide_translated.csv written  ({mw.shape[0]} rows × {mw.shape[1]} cols)")


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "--help"

    if mode == "--explore":
        explore_data()
    elif mode == "--clean":
        clean_and_merge()
    elif mode == "--translate":
        translate_outputs()
    elif mode == "--all":
        explore_data()
        clean_and_merge()
        translate_outputs()
    else:
        print("Usage: python DS_cleaning.py --explore | --clean | --translate | --all")
