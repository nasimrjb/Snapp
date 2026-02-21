import pandas as pd

# =========================
# Paths
# =========================
RAW_DATA_FILE = r"D:\OneDrive\Work\Driver Survey\Outputs\survey_raw_database.xlsx"
MULTI_CHOICE_FILE = r"D:\OneDrive\Work\Driver Survey\DataSources\multiple_choice.xlsx"
CODEBOOK_FILE = r"D:\OneDrive\Work\Driver Survey\Outputs\codebook.xlsx"
OUTPUT_LONG_FILE = r"D:\OneDrive\Work\Driver Survey\Outputs\survey_long_format_multichoice.xlsx"
OUTPUT_SHORT_FILE = r"D:\OneDrive\Work\Driver Survey\Outputs\survey_short_format_single.xlsx"
OUTPUT_LONG_CSV = r"D:\OneDrive\Work\Driver Survey\Outputs\survey_long_format_multichoice.csv"
OUTPUT_SHORT_CSV = r"D:\OneDrive\Work\Driver Survey\Outputs\survey_short_format_single.csv"

# =========================
# Load data
# =========================
df = pd.read_excel(RAW_DATA_FILE)

if 'recordID' not in df.columns:
    raise ValueError("recordID column not found.")

if 'city' not in df.columns:
    raise ValueError("city column not found.")

multi_choice_df = pd.read_excel(MULTI_CHOICE_FILE)
codebook_df = pd.read_excel(CODEBOOK_FILE)

# =========================
# Identify customized columns (to exclude)
# =========================
customized_cols = codebook_df[
    codebook_df['replaced_answers'].str.contains(
        'custom', case=False, na=False)
]['column_name'].tolist()

# =========================
# Get ALL multiple choice columns from mapping file
# =========================
multi_choice_cols = multi_choice_df['Column Headers'].unique().tolist()

# Keep only those that actually exist in raw data
multi_choice_cols = [
    col for col in multi_choice_cols
    if col in df.columns and col not in customized_cols
]

# =========================
# LONG FORMAT (Unpivot ALL multi-choice columns)
# =========================
long_df = df[['recordID', 'city'] + multi_choice_cols].copy()

long_df = long_df.melt(
    id_vars=['recordID', 'city'],
    value_vars=multi_choice_cols,
    var_name='sub_question',
    value_name='answer'
)

# Remove null answers
long_df = long_df[long_df['answer'].notna()]

# Optional: keep only valid answers from codebook
valid_answers = codebook_df[
    codebook_df['column_name'].isin(multi_choice_cols)
]['replaced_answers'].dropna().tolist()

valid_answers = [
    ans.strip()
    for group in valid_answers
    for ans in group.split(',,')
]

long_df = long_df[long_df['answer'].isin(valid_answers)]

# =========================
# Merge back ALL other columns (everything except multi-choice)
# =========================
other_cols = [col for col in df.columns if col not in multi_choice_cols]

long_df = long_df.merge(
    df[other_cols],
    on='recordID',
    how='left'
)

# =========================
# SHORT FORMAT (Everything except multi-choice columns)
# =========================
short_cols = [col for col in df.columns if col not in multi_choice_cols]

short_df = df[short_cols].copy()

# =========================
# Save outputs
# =========================
long_df.to_excel(OUTPUT_LONG_FILE, index=False)
short_df.to_excel(OUTPUT_SHORT_FILE, index=False)

long_df.to_csv(OUTPUT_LONG_CSV, index=False, encoding='utf-8-sig')
short_df.to_csv(OUTPUT_SHORT_CSV, index=False, encoding='utf-8-sig')

print("Outputs saved successfully.")
