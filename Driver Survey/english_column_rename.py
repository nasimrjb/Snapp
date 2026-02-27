import pandas as pd
from pathlib import Path

# ============================================================
# Path
# ============================================================

FILE_PATH = Path(
    r"D:\OneDrive\Work\Driver Survey\DataSources\column_rename.xlsx")
OUTPUT_PATH = FILE_PATH  # change this if you want to save as a new file

# ============================================================
# Helper Function
# ============================================================


def transform_value(val):
    """
    If value ends with '_snapp' or '_tapsi',
    move that suffix to the beginning.
    """
    if not isinstance(val, str):
        return val

    if val.endswith("_snapp"):
        core = val[:-6]  # remove "_snapp"
        return f"snapp_{core}"

    if val.endswith("_tapsi"):
        core = val[:-6]  # remove "_tapsi"
        return f"tapsi_{core}"

    return val


# ============================================================
# Load Excel (all sheets)
# ============================================================

sheets_dict = pd.read_excel(FILE_PATH, sheet_name=None)

# ============================================================
# Process Each Sheet
# ============================================================

for sheet_name, df in sheets_dict.items():

    if "replaced_question" in df.columns:
        df["replaced_question"] = df["replaced_question"].apply(
            transform_value)
        sheets_dict[sheet_name] = df
    else:
        print(
            f"⚠️ 'replaced_question' column not found in sheet: {sheet_name}")

# ============================================================
# Save Back to Excel
# ============================================================

with pd.ExcelWriter(OUTPUT_PATH, engine="openpyxl") as writer:
    for sheet_name, df in sheets_dict.items():
        df.to_excel(writer, sheet_name=sheet_name, index=False)

print("✅ Done. All sheets updated successfully.")
