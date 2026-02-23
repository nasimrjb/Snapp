import os
import pandas as pd
from glob import glob

# Path to the folder containing xlsx files
folder_path = r"D:\OneDrive\Work\Driver Survey\raw"
sheet_name = "داده خام"
datetime_col = "مهر زمان (mm/dd/yyyy)"

# Find all xlsx files
xlsx_files = glob(os.path.join(folder_path, "*.xlsx"))

if not xlsx_files:
    print("No .xlsx files found in the specified folder.")
    exit()

dfs = []

for file in xlsx_files:
    try:
        df = pd.read_excel(file, sheet_name=sheet_name)
        df["__source_file__"] = os.path.basename(file)
        dfs.append(df)
        print(f"Loaded: {os.path.basename(file)} — {len(df)} rows")
    except Exception as e:
        print(f"Skipped {os.path.basename(file)}: {e}")

if not dfs:
    print("No data loaded. Check sheet names.")
    exit()

# Append all dataframes (outer join keeps all columns)
combined = pd.concat(dfs, axis=0, ignore_index=True, join="outer")

# Parse and sort by datetime column
if datetime_col in combined.columns:
    combined[datetime_col] = pd.to_datetime(
        combined[datetime_col], dayfirst=False, errors="coerce")
    combined.sort_values(by=datetime_col, ascending=True,
                         inplace=True, na_position="last")
    combined.reset_index(drop=True, inplace=True)
else:
    print(f"Warning: Column '{datetime_col}' not found. Data won't be sorted.")

# Save output
output_path = os.path.join(folder_path, "combined_raw.xlsx")
combined.to_excel(output_path, index=False, sheet_name=sheet_name)
print(f"\nDone! Combined file saved to:\n{output_path}")
print(f"Total rows: {len(combined)} | Total columns: {len(combined.columns)}")
