import pandas as pd

# =========================
# Paths
# =========================
INPUT_EXCEL_PATH = r"C:\Users\Snapp\Desktop\survey_raw_database.xlsx"
OUTPUT_EXCEL_PATH = r"C:\Users\Snapp\Desktop\survey_raw_database_unique.xlsx"

# =========================
# Read Excel file
# =========================
df = pd.read_excel(INPUT_EXCEL_PATH)
# =========================
# Remove duplicates per column
# =========================
unique_columns = {}

for col in df.columns:
    # Drop duplicates only within this column
    # Also drop NaNs to avoid meaningless duplicates
    unique_values = (
        df[col]
        .dropna()
        .drop_duplicates()
        .reset_index(drop=True)
    )
    unique_columns[col] = unique_values

# =========================
# Reconstruct DataFrame
# (columns may have different lengths)
# =========================
result_df = pd.DataFrame(unique_columns)

# =========================
# Write output to Excel
# =========================
result_df.to_excel(OUTPUT_EXCEL_PATH, index=False)

print("✅ Excel file created with per-column duplicates removed.")
print(f"📄 Output saved to: {OUTPUT_EXCEL_PATH}")
