"""
generate_mapping.py  —  Step 1 of the Driver Survey Pipeline
=============================================================

PURPOSE:
    This script reads a hand-maintained Excel file ("column_rename.xlsx") that
    describes every survey question, and converts it into a machine-readable
    JSON file ("column_rename_mapping.json").

    Think of the Excel file as a "dictionary" that humans maintain, and the JSON
    file as the same dictionary in a format that Python can quickly look up.

WHY IT EXISTS:
    Raw survey files arrive with messy Persian/Arabic column headers that change
    slightly from week to week.  The Excel file maps each raw header to a clean
    short name, a data type, a question type (single-choice, multi-choice, etc.),
    how often the question appears (always / often / rare), and the allowed
    answer values with their English replacements.

    Downstream scripts (data_cleaning.py, survey_analysis_v6.py) load the
    generated JSON to know how to rename, recode, and categorize every column.

HOW TO RUN:
    python generate_mapping.py

OUTPUT:
    DataSources/column_rename_mapping.json

PIPELINE POSITION:
    [generate_mapping.py]  →  data_cleaning.py  →  survey_analysis_v6.py
         (you are here)
"""

import pandas as pd
import json
import os

# ---------------------------------------------------------------------------
# File paths — point to the Excel source and where the JSON output should go
# ---------------------------------------------------------------------------
BASE_DIR = r"D:\Work\Driver Survey\Sources"
XLSX_PATH = os.path.join(BASE_DIR, "column_rename.xlsx")
JSON_PATH = os.path.join(BASE_DIR, "column_rename_mapping.json")

# ---------------------------------------------------------------------------
# Lookup tables that translate the Excel's free-text labels into short,
# consistent codes used throughout the rest of the pipeline.
#
# Example: the Excel might say "string" for data type → we store "str".
#          the Excel might say "single_choice" → we store "single".
# ---------------------------------------------------------------------------
DTYPE_MAP = {"string": "str", "integer": "int", "float": "float",
             "datetime": "datetime", "date": "date", "boolean": "bool"}
QTYPE_MAP = {"single_choice": "single", "multi_choice": "multi", "multiple_choice": "multi",
             "protected_meta": "meta", "open_ended": "open", "rating": "rating", "scale": "scale", "other": "other"}
FREQ_MAP = {"ALWAYS": "always", "OFTEN": "often", "RARE": "rare"}


def generate_mapping(xlsx_path=XLSX_PATH, json_path=JSON_PATH):
    """
    Read the Excel file and produce a JSON mapping.

    The Excel workbook has two sheets:
      • "questions"          – one row per survey question, with columns like
                               question_raw, question_short, question_long,
                               section, and multiple "answersN" columns that
                               list all possible raw answer texts.
      • "replaced_answers"   – same row order, with columns data_type,
                               question_type, question_freq, and the same
                               "answersN" columns but filled with the
                               *replacement* (English/clean) answer values.

    The output JSON is a dictionary keyed by question_short (e.g. "snapp_age"),
    where each value is another dict:
        {
            "raw":     ["original Persian header", ...],  # may have >1 variant
            "long":    "full English question text",
            "type":    "single" | "multi" | "meta" | ...,
            "freq":    "always" | "often" | "rare",
            "dtype":   "str" | "int" | ...,
            "section": "Demographics" | ...,
            "answers": {"raw_answer_1": "clean_answer_1", ...} or null
        }
    """

    # --- Read both sheets into DataFrames -----------------------------------
    questions = pd.read_excel(xlsx_path, sheet_name="questions")
    replaced = pd.read_excel(xlsx_path, sheet_name="replaced_answers")

    # Identify all answer columns (they start with "answers", e.g. answers1, answers2, ...)
    answer_cols = [c for c in questions.columns if c.startswith("answers")]

    # --- Build the mapping dict, one entry per question_short ---------------
    result = {}
    for i in range(len(questions)):
        # The short name is the key we use everywhere (e.g. "snapp_age")
        key = str(questions.iloc[i]["question_short"])

        # Read fields, converting to None when the cell is empty/NaN
        q_raw = str(questions.iloc[i]["question_raw"]) if pd.notna(
            questions.iloc[i]["question_raw"]) else None
        q_long = str(questions.iloc[i]["question_long"]) if pd.notna(
            questions.iloc[i]["question_long"]) else None
        raw_dtype = str(replaced.iloc[i]["data_type"]) if pd.notna(
            replaced.iloc[i]["data_type"]) else None
        raw_qtype = str(replaced.iloc[i]["question_type"]) if pd.notna(
            replaced.iloc[i]["question_type"]) else None
        raw_freq = str(replaced.iloc[i]["question_freq"]) if pd.notna(
            replaced.iloc[i]["question_freq"]) else None

        # Read section from the questions sheet (may not exist in older files)
        raw_section = str(questions.iloc[i]["section"]) if (
            "section" in questions.columns and pd.notna(
                questions.iloc[i].get("section"))
        ) else None

        # Build the answer mapping: {original_answer: replacement_answer}
        # If the replacement cell is empty, fall back to using the original text
        answers = {}
        for ac in answer_cols:
            qv = questions.iloc[i].get(ac)    # original answer text
            rv = replaced.iloc[i].get(ac)      # replacement answer text
            if pd.notna(qv):
                answers[str(qv)] = str(rv) if pd.notna(rv) else str(qv)

        # --- First time seeing this key → create new entry ------------------
        if key not in result:
            result[key] = {
                "raw": [q_raw] if q_raw else [],
                "long": q_long,
                "type": QTYPE_MAP.get(raw_qtype, raw_qtype),
                "freq": FREQ_MAP.get(raw_freq, raw_freq),
                "dtype": DTYPE_MAP.get(raw_dtype, raw_dtype),
                "section": raw_section,
                "answers": answers or None,
            }
        else:
            # --- Duplicate key (same question_short appears again) ----------
            # This happens for multi-choice questions where each answer option
            # is its own row in the Excel.  We merge the raw headers and
            # answer mappings into the existing entry.
            if q_raw and q_raw not in result[key]["raw"]:
                result[key]["raw"].append(q_raw)
            if answers:
                if result[key]["answers"]:
                    result[key]["answers"].update(answers)
                else:
                    result[key]["answers"] = answers
            # If section was null on first encounter but set on duplicate, fill it
            if raw_section and not result[key].get("section"):
                result[key]["section"] = raw_section

    # --- Write the JSON file ------------------------------------------------
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    # --- Print a summary to the console ------------------------------------
    print(f"Generated {json_path}")
    print(
        f"  {len(result)} questions, {sum(1 for v in result.values() if v['answers'])} with answer mappings")

    # Count how many questions belong to each section
    from collections import Counter
    sec_counts = Counter(v.get("section") for v in result.values())
    print(f"  {len(sec_counts)} sections:")
    for sec, cnt in sec_counts.most_common():
        print(f"    {sec}: {cnt}")


# ---------------------------------------------------------------------------
# When you run "python generate_mapping.py" from the command line, this block
# executes.  It just calls the function above with the default file paths.
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    generate_mapping()
