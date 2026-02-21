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
multi_choice_df = pd.read_excel(MULTI_CHOICE_FILE)
codebook_df = pd.read_excel(CODEBOOK_FILE)

for col in ['recordID', 'city']:
    if col not in df.columns:
        raise ValueError(f"'{col}' column not found in raw data.")

# =========================
# Identify columns to exclude entirely (marked "customized")
# =========================
customized_cols = set(
    codebook_df.loc[
        codebook_df['replaced_answers'].str.contains(
            'customized', case=False, na=False),
        'column_name'
    ].tolist()
)

# =========================
# Identify valid multi-choice columns
# (exist in raw data, not customized)
# =========================
multi_choice_cols = [
    col for col in multi_choice_df['Column Headers'].unique()
    if col in df.columns and col not in customized_cols
]

# =========================
# Build valid answers set from codebook
# =========================
valid_answers = set(
    ans.strip()
    for group in codebook_df.loc[
        codebook_df['column_name'].isin(multi_choice_cols),
        'replaced_answers'
    ].dropna()
    for ans in group.split(',,')
)

# =========================
# Columns for non-multi-choice outputs
# (excludes multi-choice and customized columns, but always keeps recordID)
# =========================
other_cols = [
    col for col in df.columns
    if col not in multi_choice_cols and col not in customized_cols
]

# Ensure recordID is present for merging
if 'recordID' not in other_cols:
    other_cols = ['recordID'] + other_cols

# =========================
# LONG FORMAT
# Melt multi-choice columns, filter valid answers, merge other columns back
# =========================
melted_df = (
    df[['recordID', 'city'] + multi_choice_cols]
    .melt(
        id_vars=['recordID', 'city'],
        value_vars=multi_choice_cols,
        var_name='sub_question',
        value_name='answer'
    )
    .dropna(subset=['answer'])
    .pipe(lambda d: d[d['answer'].isin(valid_answers)])
)

# Merge other columns; drop 'city' from other_cols before merge to avoid duplication
other_cols_for_merge = [col for col in other_cols if col != 'city']
long_df = melted_df.merge(df[other_cols_for_merge], on='recordID', how='left')

# =========================
# SHORT FORMAT
# All columns except multi-choice and customized
# =========================
short_df = df[other_cols].copy()

# =========================
# Save outputs
# =========================
long_df.to_excel(OUTPUT_LONG_FILE,  index=False)
short_df.to_excel(OUTPUT_SHORT_FILE, index=False)

long_df.to_csv(OUTPUT_LONG_CSV,  index=False, encoding='utf-8-sig')
short_df.to_csv(OUTPUT_SHORT_CSV, index=False, encoding='utf-8-sig')

print("Outputs saved successfully.")
