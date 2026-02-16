import pandas as pd
import numpy as np

# ============================
# Paths
# ============================

CSV_PATH = r"D:\OneDrive\Work\Cancellation\DataSources\carpooling_export_7.csv"
EXCEL_PATH = r"D:\OneDrive\Work\Cancellation\DataSources\Route.xlsx"
REAL_DATA_PATH = r"D:\OneDrive\Work\Cancellation\DataSources\real_data_7.csv"

OUTPUT_FROM_AGG = r"D:\OneDrive\Work\Cancellation\Outputs\Origin_Bucket_Agg.xlsx"
OUTPUT_TIME_AGG = r"D:\OneDrive\Work\Cancellation\Outputs\Time_Bucket_Agg.xlsx"
OUTPUT_DISTANCE_AGG = r"D:\OneDrive\Work\Cancellation\Outputs\Distance_Bucket_Agg.xlsx"

MIN_PAIRED = 9
ADJ1 = 0.75
ADJ2 = 0.8

# ============================
# Columns Hidden in Outputs
# ============================

HIDDEN_OUTPUT_COLUMNS = [
    'NMV_sum',
    'ride_sum',
    'SN_pair_count_nf',
    'TP_pair_count_nf',
    'SN_accept_count_nf',
    'req_share_nf %'
]


def drop_for_export(df):
    """Remove selected columns when saving."""
    return df.drop(
        columns=[c for c in HIDDEN_OUTPUT_COLUMNS if c in df.columns],
        errors="ignore"
    )


# ============================
# Load Data
# ============================

def load_data(csv_path, excel_path, real_data_path):
    return (
        pd.read_csv(csv_path, encoding="utf-8-sig"),
        pd.read_excel(excel_path),
        pd.read_csv(real_data_path, encoding="utf-8-sig")
    )


# ============================
# Base Preparation
# ============================

def prepare_base_df(df):
    flag_cols = ['snapp_paired', 'tapsi_paired',
                 'snapp_accepted', 'tapsi_accepted']

    # Normalize flags
    for col in flag_cols:
        df[col] = df[col].astype(str).str.strip().str.lower()

    # Remove invalid acceptances
    invalid_mask = (
        ((df['snapp_paired'] == 'no') & (df['snapp_accepted'] == 'yes')) |
        ((df['tapsi_paired'] == 'no') & (df['tapsi_accepted'] == 'yes'))
    )
    removed_rows = invalid_mask.sum()
    if removed_rows > 0:
        print(f"🧹 Removed {removed_rows:,} invalid acceptance rows")
    df = df.loc[~invalid_mask].copy()

    # Drop duplicates and assign ride IDs
    df = df.drop_duplicates().reset_index(drop=True)
    df['ride_id'] = np.arange(len(df))

    # Convert flags to boolean
    for col in flag_cols:
        df[col] = df[col].eq('yes')

    # Convert fare columns to numeric
    fare_cols = [
        'snapp_before_fare', 'snapp_after_fare',
        'tapsi_before_fare', 'tapsi_after_fare',
        'snapp_normal_fare', 'tapsi_normal_fare'
    ]
    for col in fare_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    return df


# ============================
# Real Data Preparation
# ============================

def prepare_real_data(df):
    city_mapping = {
        1.0: "Tehran",
        2.0: "Karaj",
        3.0: "Isfahan",
        5.0: "Mashhad"
    }

    df = df.copy()
    df['city'] = df['org_city_id'].map(city_mapping)
    df = df.rename(columns={
        'Week_Num': 'week_number',
        'org_dist_id': 'from_coded'
    })
    df['from_coded'] = df['from_coded'].astype('Int64')

    for col in ['reqs', 'pairs', 'accepts', 'NMV', 'ride']:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    return df


# ============================
# Time Features
# ============================

def add_time_features(df):
    df['travel_date'] = pd.to_datetime(df['travel_date'], errors='coerce')
    df['travel_time_dt'] = pd.to_datetime(
        df['travel_time'], format='%H:%M:%S', errors='coerce')

    # Extract year
    df['year'] = df['travel_date'].dt.year

    # Time bucket
    def bucket(dt):
        if pd.isna(dt):
            return None
        h = dt.hour
        if h < 9:
            return "06_09"
        elif h < 15:
            return "09_15"
        elif h < 18:
            return "15_18"
        elif h < 21:
            return "18_21"
        return None

    df['time_bucket'] = df['travel_time_dt'].apply(bucket)

    # Week number
    df['week_number'] = df['travel_date'].dt.isocalendar().week.astype(int)
    df['week_number'] += (df['travel_date'].dt.weekday >= 5).astype(int)

    return df


# ============================
# Routes Merge
# ============================

def merge_routes(df, routes_df):
    routes_df = routes_df.copy()
    routes_df = routes_df.rename(columns={
        'Origin_Add': 'from',
        'Destination_Add': 'to',
        'Distance': 'distance_bucket',
        'Or': 'from_coded',
        'DstDistID': 'to_coded'
    })

    def normalize_text(text):
        if pd.isna(text):
            return None
        text = str(text).strip()
        if text.lower() == 'nan' or text == '':
            return None
        text = text.replace('ي', 'ی').replace('ك', 'ک')
        text = text.replace('\u200c', '').replace(
            '\u200b', '').replace('\u200d', '')
        text = text.replace('\ufeff', '').replace(
            '\xa0', '').replace('\u202a', '')
        text = text.replace('\u202b', '').replace('\u202c', '')
        text = text.replace(' ', '').lower()
        return text

    # Normalize addresses
    df['from_original'] = df['from'].copy()
    df['to_original'] = df['to'].copy()
    df['from_normalized'] = df['from'].apply(normalize_text)
    df['to_normalized'] = df['to'].apply(normalize_text)

    routes_df['from_normalized'] = routes_df['from'].apply(normalize_text)
    routes_df['to_normalized'] = routes_df['to'].apply(normalize_text)

    # Remove rows with missing addresses
    initial_len = len(df)
    df = df[df['from_normalized'].notna() & df['to_normalized'].notna()].copy()
    removed = initial_len - len(df)
    if removed > 0:
        print(f"ℹ️  Removed {removed} rows with missing/invalid addresses")

    # Prepare routes lookup
    routes_df = routes_df[routes_df['from_normalized'].notna(
    ) & routes_df['to_normalized'].notna()].copy()
    routes_df = routes_df.drop_duplicates(
        subset=['from_normalized', 'to_normalized'], keep='first')
    routes_df['from_coded'] = routes_df['from_coded'].astype('Int64')

    routes_lookup = routes_df[['from_normalized', 'to_normalized',
                               'distance_bucket', 'from_coded', 'to_coded']].copy()

    # Merge
    merged = df.merge(routes_lookup, on=[
                      'from_normalized', 'to_normalized'], how='left')

    # Report matching statistics
    unmatched_count = merged['from_coded'].isna().sum()
    matched_count = merged['from_coded'].notna().sum()
    print(f"\n✓ Merge Results:")
    print(
        f"  Matched:   {matched_count:,} rows ({matched_count/len(merged)*100:.1f}%)")
    print(
        f"  Unmatched: {unmatched_count:,} rows ({unmatched_count/len(merged)*100:.1f}%)")

    if unmatched_count > 0:
        print(
            f"\n⚠️  WARNING: {unmatched_count} rows didn't match any route in Route.xlsx")
        unmatched_routes = merged[merged['from_coded'].isna(
        )][['from_original', 'to_original']].drop_duplicates()
        if len(unmatched_routes) > 0:
            unmatched_routes.to_csv(
                'UNMATCHED_ROUTES_CHECK.csv', index=False, encoding='utf-8-sig')
            print(
                f"  → Saved {len(unmatched_routes)} unique unmatched routes to: UNMATCHED_ROUTES_CHECK.csv")

    merged = merged.drop(
        columns=['from_normalized', 'to_normalized', 'from_original', 'to_original'])
    return merged


# ============================
# Main Aggregation
# ============================

def aggregate_metrics(df, dims):
    df = df.copy()

    # Conditional fare columns
    df['snapp_before_fare_acc'] = df['snapp_before_fare'].where(
        df['snapp_accepted'])
    df['snapp_after_fare_acc'] = df['snapp_after_fare'].where(
        df['snapp_accepted'])
    df['tapsi_before_fare_acc'] = df['tapsi_before_fare'].where(
        df['tapsi_accepted'])
    df['tapsi_after_fare_acc'] = df['tapsi_after_fare'].where(
        df['tapsi_accepted'])

    agg = (
        df.groupby(dims, dropna=False)
        .agg(
            year=('year', 'first'),  # Keep year from the group
            total_rides=('ride_id', 'nunique'),
            SN_paired=('snapp_paired', 'sum'),
            TP_paired=('tapsi_paired', 'sum'),
            SN_accepted=('snapp_accepted', 'sum'),
            TP_accepted=('tapsi_accepted', 'sum'),
            Avg_SN_carp=('snapp_before_fare_acc', 'mean'),
            Avg_SN_Psub=('snapp_after_fare_acc', 'mean'),
            Avg_TP_carp=('tapsi_before_fare_acc', 'mean'),
            Avg_TP_Psub=('tapsi_after_fare_acc', 'mean'),
            Avg_SN_Eco=('snapp_normal_fare', 'mean'),
            Avg_TP_Eco=('tapsi_normal_fare', 'mean'),
        )
        .reset_index()
    )

    # Calculate percentages
    agg['SN_pairing %'] = agg['SN_paired'] / agg['total_rides']
    agg['TP_pairing %'] = agg['TP_paired'] / agg['total_rides']
    agg['SN_acceptance %'] = np.where(
        agg['SN_paired'] > 0, agg['SN_accepted'] / agg['SN_paired'], np.nan)
    agg['TP_acceptance %'] = np.where(
        agg['TP_paired'] > 0, agg['TP_accepted'] / agg['TP_paired'], np.nan)

    return agg


# ============================
# Real Data Aggregation
# ============================

def aggregate_real_data(df, dims):
    df = df.dropna(subset=dims)
    return (
        df.groupby(dims, dropna=False)
        .agg(
            req_count=('reqs', 'sum'),
            SN_pair_count_raw=('pairs', 'sum'),
            SN_accept_count_raw=('accepts', 'sum'),
            NMV_sum=('NMV', 'sum'),
            ride_sum=('ride', 'sum')
        )
        .reset_index()
    )


def merge_real_data_with_main(main_agg, real_agg, dims):
    merged = main_agg.merge(real_agg, on=dims, how='left')

    for col in ['req_count', 'SN_pair_count_raw', 'SN_accept_count_raw', 'NMV_sum', 'ride_sum']:
        merged[col] = merged[col].fillna(0)

    merged['SN_pair_count_nf'] = merged['SN_pair_count_raw']
    merged['TP_pair_count_nf'] = merged['SN_pair_count_raw']
    merged['SN_accept_count_nf'] = merged['SN_accept_count_raw']

    merged['SN_pair_count'] = np.where(
        merged['SN_paired'] > MIN_PAIRED, merged['SN_pair_count_raw'], np.nan)
    merged['TP_pair_count'] = np.where(
        merged['TP_paired'] > MIN_PAIRED, merged['SN_pair_count_raw'], np.nan)
    merged['SN_accept_count'] = np.where(
        merged['SN_paired'] > MIN_PAIRED, merged['SN_accept_count_raw'], np.nan)

    return merged.drop(columns=['SN_pair_count_raw', 'SN_accept_count_raw'])


# ============================
# Derived Metrics
# ============================

def calculate_derived_metrics(agg, first_two_dims):
    agg = agg.copy()

    req_group_sum = agg.groupby(first_two_dims)['req_count'].transform('sum')

    agg['req_share %'] = np.where(
        req_group_sum > MIN_PAIRED, agg['req_count'] / req_group_sum, np.nan)
    agg['req_share_nf %'] = np.where(
        req_group_sum > 0, agg['req_count'] / req_group_sum, np.nan)
    agg['pairing %'] = np.where(
        agg['req_count'] > 0, agg['SN_pair_count_nf'] / agg['req_count'], np.nan)
    agg['acceptance %'] = np.where(
        agg['SN_pair_count_nf'] > 0, agg['SN_accept_count_nf'] / agg['SN_pair_count_nf'], np.nan)

    return agg


# ============================
# AOV Metrics
# ============================

def add_aov_metrics_for_from_table(df):
    df = df.copy()

    df['AOV_real_SN'] = np.where(
        (df['SN_accepted'] > MIN_PAIRED) & (df['ride_sum'] > 0),
        df['NMV_sum'] / df['ride_sum'],
        np.nan
    )

    df['AOV_T_R'] = np.where(
        df['AOV_real_SN'] > 0,
        (ADJ1 * df['Avg_SN_carp']) / (ADJ2 * df['AOV_real_SN']),
        np.nan
    )

    df['AOV_est'] = np.where(
        df['AOV_T_R'] > 0,
        df['Avg_TP_carp'] / df['AOV_T_R'],
        np.nan
    )

    df['SN_finished_ride'] = np.where(
        df['SN_accepted'] > MIN_PAIRED,
        df['ride_sum'],
        np.nan
    )

    for col in ['AOV_real_SN', 'AOV_T_R', 'AOV_est']:
        df[col] = pd.to_numeric(df[col], errors='coerce').round(2)

    df['SN_finished_ride'] = pd.to_numeric(
        df['SN_finished_ride'], errors='coerce')

    return df


# ============================
# Ratio Columns
# ============================

def add_ratio_columns(df):
    df = df.copy()

    df['pairing_ratio'] = np.where(
        df['pairing %'] > 0, df['SN_pairing %'] / df['pairing %'], np.nan)
    df['acceptance_ratio'] = np.where(
        df['acceptance %'] > 0, df['SN_acceptance %'] / df['acceptance %'], np.nan)
    df['pairing_3'] = np.where(
        df['pairing_ratio'] > 0, df['TP_pairing %'] / df['pairing_ratio'], np.nan)
    df['acceptance_3'] = np.where(
        df['acceptance_ratio'] > 0, df['TP_acceptance %'] / df['acceptance_ratio'], np.nan)

    for col in ['pairing_ratio', 'acceptance_ratio', 'pairing_3', 'acceptance_3']:
        df[col] = pd.to_numeric(df[col], errors='coerce').round(3)

    return df


# ============================
# Formatting
# ============================

def format_output(df):
    pct_cols = [c for c in df.columns if '%' in c]
    aov_cols = [c for c in df.columns if c.startswith('AOV_')]
    avg_cols = [c for c in df.columns if c.startswith(
        'Avg_') and c not in aov_cols]

    df[avg_cols] = df[avg_cols].round(1)
    df[aov_cols] = df[aov_cols].round(2)
    df[pct_cols] = df[pct_cols].round(3)

    return df


# ============================
# Add Aggregation Rows
# ============================

def add_aggregation_rows(df, group_dims, third_dim, real_data_df):
    """Add 'Total' aggregation rows with weighted average logic."""
    result_rows = []

    def get_weighted_avg(sub_df, val_col, weight_col):
        """Calculate weighted average, dropping NaN rows."""
        if val_col not in sub_df.columns or weight_col not in sub_df.columns:
            return np.nan
        clean_df = sub_df[[val_col, weight_col]].dropna()
        if len(clean_df) == 0 or clean_df[weight_col].sum() == 0:
            return np.nan
        return np.average(clean_df[val_col], weights=clean_df[weight_col])

    all_columns = df.columns.tolist()

    for group_keys, group_df in df.groupby(group_dims, dropna=False):
        result_rows.append(group_df)

        # Initialize Total Row
        agg_row = {col: np.nan for col in all_columns}
        if len(group_dims) == 2:
            agg_row[group_dims[0]] = group_keys[0]
            agg_row[group_dims[1]] = group_keys[1]
        agg_row[third_dim] = 'Total'

        # Set year from the group
        if 'year' in group_df.columns:
            agg_row['year'] = group_df['year'].iloc[0]

        # Filter real data
        week_val = group_keys[0]
        city_val = group_keys[1]
        rd_filtered = real_data_df[
            (real_data_df['week_number'] == week_val) &
            (real_data_df['city'] == city_val)
        ]

        # Simple sums
        sum_cols = [
            'total_rides', 'SN_paired', 'TP_paired', 'SN_accepted', 'TP_accepted',
            'req_count', 'NMV_sum', 'ride_sum',
            'SN_pair_count_nf', 'TP_pair_count_nf', 'SN_accept_count_nf',
            'SN_pair_count', 'TP_pair_count', 'SN_accept_count'
        ]
        for col in sum_cols:
            if col in group_df.columns:
                agg_row[col] = group_df[col].sum(min_count=0)

        # Real data direct calculations
        rd_pairs = rd_filtered['pairs'].sum()
        rd_reqs = rd_filtered['reqs'].sum()
        agg_row['pairing %'] = (rd_pairs / rd_reqs) if rd_reqs > 0 else np.nan

        rd_accepts = rd_filtered['accepts'].sum()
        agg_row['acceptance %'] = (
            rd_accepts / rd_pairs) if rd_pairs > 0 else np.nan

        total_ride = rd_filtered['ride'].sum()
        total_nmv = rd_filtered['NMV'].sum()
        agg_row['SN_finished_ride'] = float(total_ride)
        agg_row['AOV_real_SN'] = (
            total_nmv / total_ride) if total_ride > 0 else np.nan

        # Weighted averages
        agg_row['SN_pairing %'] = get_weighted_avg(
            group_df, 'SN_pairing %', 'req_share %')
        agg_row['TP_pairing %'] = get_weighted_avg(
            group_df, 'TP_pairing %', 'req_share %')
        agg_row['SN_acceptance %'] = get_weighted_avg(
            group_df, 'SN_acceptance %', 'SN_pair_count')
        agg_row['TP_acceptance %'] = get_weighted_avg(
            group_df, 'TP_acceptance %', 'TP_pair_count')

        for col in ['pairing_ratio', 'acceptance_ratio', 'pairing_3', 'acceptance_3']:
            agg_row[col] = get_weighted_avg(group_df, col, 'req_share %')

        fare_weights = {
            'Avg_SN_Eco': 'total_rides',
            'Avg_TP_Eco': 'total_rides',
            'Avg_SN_carp': 'SN_accepted',
            'Avg_SN_Psub': 'SN_accepted',
            'Avg_TP_carp': 'TP_accepted',
            'Avg_TP_Psub': 'TP_accepted',
            'AOV_T_R': 'SN_finished_ride'
        }
        for col, weight in fare_weights.items():
            agg_row[col] = get_weighted_avg(group_df, col, weight)

        # Recalculate AOV_est
        if pd.notna(agg_row.get('AOV_T_R')) and agg_row['AOV_T_R'] > 0 and pd.notna(agg_row.get('Avg_TP_carp')):
            agg_row['AOV_est'] = agg_row['Avg_TP_carp'] / agg_row['AOV_T_R']

        agg_row['req_share %'] = 1.0
        agg_row['req_share_nf %'] = 1.0

        agg_df = pd.DataFrame([agg_row], columns=all_columns)
        result_rows.append(agg_df)

    result = pd.concat(result_rows, ignore_index=True)

    # Ensure numeric types
    for col in result.columns:
        if col not in [group_dims[0], group_dims[1], third_dim, 'year']:
            result[col] = pd.to_numeric(result[col], errors='ignore')

    return result.reset_index(drop=True)


# ============================
# Reorder and Sort
# ============================

def reorder_and_sort_output(df, group_dims, third_dim):
    """Move year to first position and sort by year, week_number, city, third_dim."""
    if 'year' not in df.columns:
        return df

    # Reorder columns: year first, then group dimensions, then third dimension, then rest
    priority_cols = ['year'] + group_dims + [third_dim]
    other_cols = [c for c in df.columns if c not in priority_cols]
    new_order = priority_cols + other_cols
    df = df[new_order]

    # Sort by year, week_number, city, and third_dim (with 'Total' at the end of each group)
    def sort_key(col):
        """Create sort key that puts 'Total' at the end."""
        if col.name == third_dim:
            return col.map(lambda v: (1, v) if v == 'Total' else (0, v))
        return col

    df = df.sort_values(
        by=['year', 'week_number', 'city', third_dim],
        key=sort_key
    ).reset_index(drop=True)

    return df


# ============================
# Builder
# ============================

def build_table_with_real_data(df, real_data_df, dims, first_two_dims):
    main_agg = aggregate_metrics(df, dims)
    real_agg = aggregate_real_data(real_data_df, dims)
    merged = merge_real_data_with_main(main_agg, real_agg, dims)
    merged = calculate_derived_metrics(merged, first_two_dims)
    merged = format_output(merged)
    return merged


# ============================
# Main
# ============================

def main():
    print("\n" + "="*60)
    print("CARPOOLING DATA AGGREGATION - WITH TOTAL ROWS")
    print("="*60)

    df, routes_df, real_data_df = load_data(
        CSV_PATH, EXCEL_PATH, REAL_DATA_PATH)

    df = (
        df.pipe(prepare_base_df)
          .pipe(add_time_features)
          .pipe(merge_routes, routes_df)
    )

    real_data_df = prepare_real_data(real_data_df)

    # -------- from_coded table --------
    table_from = build_table_with_real_data(
        df, real_data_df,
        dims=['week_number', 'city', 'from_coded'],
        first_two_dims=['week_number', 'city']
    )
    table_from = add_aov_metrics_for_from_table(table_from)
    table_from = add_ratio_columns(table_from)

    table_from_agg = add_aggregation_rows(
        table_from,
        group_dims=['week_number', 'city'],
        third_dim='from_coded',
        real_data_df=real_data_df
    )
    table_from_agg = format_output(table_from_agg)
    table_from_agg = reorder_and_sort_output(
        table_from_agg, ['week_number', 'city'], 'from_coded')

    drop_for_export(table_from_agg).to_excel(
        OUTPUT_FROM_AGG, index=False, engine='openpyxl'
    )

    # -------- time_bucket table --------
    table_time = build_table_with_real_data(
        df, real_data_df,
        dims=['week_number', 'city', 'time_bucket'],
        first_two_dims=['week_number', 'city']
    )
    table_time = add_ratio_columns(table_time)

    table_time_agg = add_aggregation_rows(
        table_time,
        group_dims=['week_number', 'city'],
        third_dim='time_bucket',
        real_data_df=real_data_df
    )
    table_time_agg = format_output(table_time_agg)
    table_time_agg = reorder_and_sort_output(
        table_time_agg, ['week_number', 'city'], 'time_bucket')

    drop_for_export(table_time_agg).to_excel(
        OUTPUT_TIME_AGG, index=False, engine='openpyxl'
    )

    # -------- distance_bucket table --------
    table_distance = build_table_with_real_data(
        df, real_data_df,
        dims=['week_number', 'city', 'distance_bucket'],
        first_two_dims=['week_number', 'city']
    )
    table_distance = add_ratio_columns(table_distance)

    table_distance_agg = add_aggregation_rows(
        table_distance,
        group_dims=['week_number', 'city'],
        third_dim='distance_bucket',
        real_data_df=real_data_df
    )
    table_distance_agg = format_output(table_distance_agg)
    table_distance_agg = reorder_and_sort_output(
        table_distance_agg, ['week_number', 'city'], 'distance_bucket')

    drop_for_export(table_distance_agg).to_excel(
        OUTPUT_DISTANCE_AGG, index=False, engine='openpyxl'
    )

    print("\n✅ Aggregation complete.")


if __name__ == "__main__":
    main()
