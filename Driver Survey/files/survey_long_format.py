import pandas as pd

# =========================
# Paths
# =========================
RAW_DATA_FILE = r"D:\OneDrive\Work\Driver Survey\Outputs\survey_raw_database.xlsx"
MULTI_CHOICE_FILE = r"D:\OneDrive\Work\Driver Survey\DataSources\multiple_choice.xlsx"
CODEBOOK_FILE = r"D:\OneDrive\Work\Driver Survey\Outputs\codebook.xlsx"
OUTPUT_FILE = r"D:\OneDrive\Work\Driver Survey\Outputs\survey_long_format.xlsx"

# =========================
# Load data
# =========================
df = pd.read_excel(RAW_DATA_FILE)

# Load multiple choice mapping
multi_choice_df = pd.read_excel(MULTI_CHOICE_FILE)
# Columns: 'Main Question', 'Column Headers', 'Multiple Choices'

# Load codebook
codebook_df = pd.read_excel(CODEBOOK_FILE)
# Columns: 'column_name', 'replaced_answers'

# =========================
# Identify columns to skip (those with 'customized' answers)
# =========================
customized_cols = codebook_df[codebook_df['replaced_answers'].str.contains(
    'custom', case=False, na=False)]['column_name'].tolist()

# =========================
# Filter multiple choice mapping
# =========================
# Only keep columns that exist in raw data and are not customized
multi_choice_df = multi_choice_df[~multi_choice_df['Column Headers'].isin(
    customized_cols)]
multi_choice_map = multi_choice_df.groupby(
    'Main Question')['Column Headers'].apply(list).to_dict()

# =========================
# Melt multi-choice columns
# =========================
long_rows = []

for main_q, cols in multi_choice_map.items():
    # Only keep columns that exist in raw data
    cols = [c for c in cols if c in df.columns]
    if not cols:
        continue

    temp = df[['recordID'] + cols].copy()

    # Melt
    melted = temp.melt(id_vars=['recordID'], value_vars=cols,
                       var_name='sub_question', value_name='answer')

    # Remove empty answers
    melted = melted[melted['answer'].notna()]

    # Filter only valid answers from codebook (already ignored customized columns)
    filtered_answers = codebook_df[codebook_df['column_name'].isin(
        cols)]['replaced_answers'].tolist()
    filtered_answers = [ans.split(',,') for ans in filtered_answers]
    filtered_answers = [
        item for sublist in filtered_answers for item in sublist]

    melted = melted[melted['answer'].isin(filtered_answers)]

    melted['main_question'] = main_q
    melted = melted[['recordID', 'main_question', 'sub_question', 'answer']]

    long_rows.append(melted)

# =========================
# Concatenate all long-format data
# =========================
if long_rows:
    long_df = pd.concat(long_rows, ignore_index=True)
else:
    long_df = pd.DataFrame(
        columns=['recordID', 'main_question', 'sub_question', 'answer'])

# =========================
# Optional: merge demographics / other non-customized columns
# =========================
demographics_cols = [col for col in df.columns if col not in customized_cols and col !=
                     'recordID' and col not in long_df['sub_question'].tolist()]
if demographics_cols:
    demo_df = df[['recordID'] + demographics_cols]
    long_df = long_df.merge(demo_df, on='recordID', how='left')

# =========================
# Save output
# =========================
long_df.to_excel(OUTPUT_FILE, index=False)
print(f"Long-format survey saved to: {OUTPUT_FILE}")
