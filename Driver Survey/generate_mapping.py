import pandas as pd
import json
import os

XLSX_PATH = r"D:\OneDrive\Work\Driver Survey\DataSources\column_rename.xlsx"
JSON_PATH = os.path.join(os.path.dirname(XLSX_PATH),
                         "column_rename_mapping.json")

DTYPE_MAP = {"string": "str", "integer": "int", "float": "float",
             "datetime": "datetime", "date": "date", "boolean": "bool"}
QTYPE_MAP = {"single_choice": "single", "multi_choice": "multi", "multiple_choice": "multi",
             "protected_meta": "meta", "open_ended": "open", "rating": "rating", "scale": "scale", "other": "other"}
FREQ_MAP = {"ALWAYS": "always", "OFTEN": "often", "RARE": "rare"}


def generate_mapping(xlsx_path=XLSX_PATH, json_path=JSON_PATH):
    questions = pd.read_excel(xlsx_path, sheet_name="questions")
    replaced = pd.read_excel(xlsx_path, sheet_name="replaced_answers")

    answer_cols = [c for c in questions.columns if c.startswith("answers")]

    result = {}
    for i in range(len(questions)):
        key = str(questions.iloc[i]["question_short"])
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

        answers = {}
        for ac in answer_cols:
            qv = questions.iloc[i].get(ac)
            rv = replaced.iloc[i].get(ac)
            if pd.notna(qv):
                answers[str(qv)] = str(rv) if pd.notna(rv) else str(qv)

        if key not in result:
            result[key] = {
                "raw": [q_raw] if q_raw else [],
                "long": q_long,
                "type": QTYPE_MAP.get(raw_qtype, raw_qtype),
                "freq": FREQ_MAP.get(raw_freq, raw_freq),
                "dtype": DTYPE_MAP.get(raw_dtype, raw_dtype),
                "answers": answers or None,
            }
        else:
            if q_raw and q_raw not in result[key]["raw"]:
                result[key]["raw"].append(q_raw)
            if answers:
                if result[key]["answers"]:
                    result[key]["answers"].update(answers)
                else:
                    result[key]["answers"] = answers

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"Generated {json_path}")
    print(
        f"  {len(result)} questions, {sum(1 for v in result.values() if v['answers'])} with answer mappings")


if __name__ == "__main__":
    generate_mapping()
