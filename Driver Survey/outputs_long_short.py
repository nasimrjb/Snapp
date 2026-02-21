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

# Ensure city column exists
if 'city' not in df.columns:
    raise ValueError("Column 'city' not found in raw database.")

# Load multiple choice mapping
multi_choice_df = pd.read_excel(MULTI_CHOICE_FILE)

# Load codebook
codebook_df = pd.read_excel(CODEBOOK_FILE)

# =========================
# Identify columns to skip (those with 'customized' answers)
# =========================
customized_cols = codebook_df[
    codebook_df['replaced_answers'].str.contains(
        'custom', case=False, na=False)
]['column_name'].tolist()

# =========================
# Filter multiple choice mapping
# =========================
multi_choice_df = multi_choice_df[
    ~multi_choice_df['Column Headers'].isin(customized_cols)
]

multi_choice_map = multi_choice_df.groupby(
    'Main Question'
)['Column Headers'].apply(list).to_dict()

# Get all multi-choice columns (flattened)
multi_choice_cols = [
    col for cols in multi_choice_map.values()
    for col in cols if col in df.columns
]

# =========================
# Process MULTI-CHOICE questions (LONG FORMAT)
# =========================
long_rows = []

for main_q, cols in multi_choice_map.items():

    cols = [c for c in cols if c in df.columns]
    if not cols:
        continue

    # Include city in melt base
    temp = df[['recordID', 'city'] + cols].copy()

    melted = temp.melt(
        id_vars=['recordID', 'city'],
        value_vars=cols,
        var_name='sub_question',
        value_name='answer'
    )

    melted = melted[melted['answer'].notna()]

    # Valid answers
    filtered_answers = codebook_df[
        codebook_df['column_name'].isin(cols)
    ]['replaced_answers'].tolist()

    filtered_answers = [ans.split(',,') for ans in filtered_answers]
    filtered_answers = [
        item for sublist in filtered_answers for item in sublist
    ]

    melted = melted[melted['answer'].isin(filtered_answers)]

    melted['main_question'] = main_q

    melted = melted[
        ['recordID', 'city', 'main_question', 'sub_question', 'answer']
    ]

    long_rows.append(melted)

# Concatenate long-format
if long_rows:
    long_df = pd.concat(long_rows, ignore_index=True)
else:
    long_df = pd.DataFrame(
        columns=['recordID', 'city', 'main_question', 'sub_question', 'answer']
    )

# =========================
# Add other demographics (excluding city to avoid duplication)
# =========================
demographics_cols = [
    col for col in df.columns
    if col not in customized_cols
    and col not in multi_choice_cols
    and col not in ['recordID', 'city']
]

if demographics_cols:
    demo_df = df[['recordID'] + demographics_cols]
    long_df = long_df.merge(demo_df, on='recordID', how='left')

# =========================
# Process SINGLE-ANSWER questions (SHORT FORMAT)
# =========================
single_answer_cols = [
    col for col in df.columns
    if col not in ['recordID']
    and col not in customized_cols
    and col not in multi_choice_cols
]

short_df = df[['recordID', 'city'] + [
    col for col in single_answer_cols if col != 'city'
]].copy()

# =========================
# Save outputs
# =========================
long_df.to_excel(OUTPUT_LONG_FILE, index=False)
print(f"Long-format multi-choice survey saved to: {OUTPUT_LONG_FILE}")

short_df.to_excel(OUTPUT_SHORT_FILE, index=False)
print(f"Short-format single-answer survey saved to: {OUTPUT_SHORT_FILE}")

long_df.to_csv(OUTPUT_LONG_CSV, index=False, encoding='utf-8-sig')
print(f"Long-format multi-choice survey CSV saved to: {OUTPUT_LONG_CSV}")

short_df.to_csv(OUTPUT_SHORT_CSV, index=False, encoding='utf-8-sig')
print(f"Short-format single-answer survey CSV saved to: {OUTPUT_SHORT_CSV}")
