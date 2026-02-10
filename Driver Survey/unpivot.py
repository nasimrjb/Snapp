import pandas as pd
import numpy as np

# =========================
# PATHS
# =========================
RAW_SURVEY_PATH = r"D:\OneDrive\Work\Driver Survey\Outputs\survey_raw_database.xlsx"
MULTIPLE_CHOICE_PATH = r"D:\OneDrive\Work\Driver Survey\DataSources\multiple_choice.xlsx"

OUTPUT_WIDE_PATH = r"D:\OneDrive\Work\Driver Survey\Outputs\survey_clean_wide.xlsx"
OUTPUT_MC_LONG_PATH = r"D:\OneDrive\Work\Driver Survey\Outputs\survey_multiple_choice_long.xlsx"
OUTPUT_CODEBOOK_PATH = r"D:\OneDrive\Work\Driver Survey\Outputs\survey_codebook.csv"


# =========================
# LOAD DATA
# =========================
survey_df = pd.read_excel(RAW_SURVEY_PATH)
mc_df = pd.read_excel(MULTIPLE_CHOICE_PATH)

# Normalize column names (safe, minimal)
survey_df.columns = survey_df.columns.str.strip()

# Drop ignored columns
survey_df = survey_df.loc[:, ~
                          survey_df.columns.str.startswith("ignore", na=False)]

# =========================
# BASIC TYPE CLEANING
# =========================

# Keep datetime intact
if "datetime" in survey_df.columns:
    survey_df["datetime"] = pd.to_datetime(
        survey_df["datetime"], errors="coerce")

# Convert numeric-looking columns to numeric
for col in survey_df.columns:
    if col == "datetime":
        continue
    survey_df[col] = pd.to_numeric(survey_df[col], errors="ignore")


# =========================
# MULTIPLE CHOICE STRUCTURE
# =========================

mc_df = mc_df.rename(
    columns={
        "Main Question": "main_question",
        "Column Headers": "column_name",
        "Multiple Choices": "choice_label",
    }
)

mc_df["column_name"] = mc_df["column_name"].str.strip()

# Only keep MC columns that actually exist in survey
mc_df = mc_df[mc_df["column_name"].isin(survey_df.columns)]

# =========================
# BUILD LONG FORMAT MC DATA
# =========================

mc_long_records = []

id_cols = ["recordID"]
if "datetime" in survey_df.columns:
    id_cols.append("datetime")

for _, row in mc_df.iterrows():
    col = row["column_name"]

    temp = survey_df[id_cols + [col]].copy()
    temp["main_question"] = row["main_question"]
    temp["choice_column"] = col
    temp["choice_label"] = row["choice_label"]

    # A choice is selected if value is not null / not zero
    temp["selected"] = temp[col].notna() & (temp[col] != 0)

    mc_long_records.append(
        temp[id_cols + ["main_question", "choice_label", "selected"]]
    )

mc_long_df = pd.concat(mc_long_records, ignore_index=True)

# Keep only selected choices (clean long table)
mc_long_df = mc_long_df[mc_long_df["selected"]].drop(columns="selected")


# =========================
# CLEAN WIDE FORMAT
# =========================

# Convert MC columns to clean binary indicators (0/1)
survey_clean = survey_df.copy()

for col in mc_df["column_name"].unique():
    survey_clean[col] = np.where(
        survey_clean[col].notna() & (survey_clean[col] != 0), 1, 0
    )

# =========================
# CODEBOOK
# =========================

codebook = []

for col in survey_clean.columns:
    entry = {
        "column_name": col,
        "type": str(survey_clean[col].dtype),
        "is_multiple_choice": col in mc_df["column_name"].values,
    }

    if entry["is_multiple_choice"]:
        entry["main_question"] = mc_df.loc[
            mc_df["column_name"] == col, "main_question"
        ].iloc[0]
        entry["choice_label"] = mc_df.loc[
            mc_df["column_name"] == col, "choice_label"
        ].iloc[0]
    else:
        entry["main_question"] = None
        entry["choice_label"] = None

    codebook.append(entry)

codebook_df = pd.DataFrame(codebook)


# =========================
# EXPORT
# =========================

survey_clean.to_excel(OUTPUT_WIDE_PATH, index=False)
mc_long_df.to_excel(OUTPUT_MC_LONG_PATH, index=False)
codebook_df.to_csv(OUTPUT_CODEBOOK_PATH, index=False, encoding="utf-8-sig")

print("✔ Survey data cleaned and ready for analysis")
