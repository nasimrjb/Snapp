import pandas as pd
import numpy as np

# ============================
# Paths
# ============================

CSV_PATH = r"D:\OneDrive\Work\Automation Project\DataSources\carpooling_export_5.csv"
EXCEL_PATH = r"D:\OneDrive\Work\Automation Project\DataSources\Route.xlsx"
REAL_DATA_PATH = r"D:\OneDrive\Work\Automation Project\DataSources\real_data_5.csv"

OUTPUT_FROM = r"D:\OneDrive\Work\Automation Project\Outputs\weekly_city_from_coded.csv"
OUTPUT_TIME = r"D:\OneDrive\Work\Automation Project\Outputs\weekly_city_time_bucket.csv"
OUTPUT_DISTANCE = r"D:\OneDrive\Work\Automation Project\Outputs\weekly_city_distance_bucket.csv"

OUTPUT_FROM_AGG = r"D:\OneDrive\Work\Automation Project\Outputs\weekly_city_from_coded_agg.xlsx"
OUTPUT_TIME_AGG = r"D:\OneDrive\Work\Automation Project\Outputs\weekly_city_time_bucket_agg.xlsx"
OUTPUT_DISTANCE_AGG = r"D:\OneDrive\Work\Automation Project\Outputs\weekly_city_distance_bucket_agg.xlsx"

MIN_PAIRED = 9
ADJ1 = 0.75
ADJ2 = 0.8

# ============================
# Load
# ============================


def load_data(csv_path, excel_path, real_data_path):
    return (
        pd.read_csv(csv_path, encoding="utf-8-sig"),
        pd.read_excel(excel_path),
        pd.read_csv(real_data_path, encoding="utf-8-sig")
    )

# ============================
# Base Prep
# ============================


def prepare_base_df(df):
    flag_cols = ['snapp_paired', 'tapsi_paired',
                 'snapp_accepted', 'tapsi_accepted']

    df = df.drop_duplicates().reset_index(drop=True)
    df['ride_id'] = np.arange(len(df))

    for col in flag_cols:
        df[col] = df[col].eq('Yes')

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
# Real Data Prep
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
        text = text.replace(' ', '')
        text = text.lower()
        return text

    df['from_original'] = df['from'].copy()
    df['to_original'] = df['to'].copy()
    df['from_normalized'] = df['from'].apply(normalize_text)
    df['to_normalized'] = df['to'].apply(normalize_text)

    routes_df['from_normalized'] = routes_df['from'].apply(normalize_text)
    routes_df['to_normalized'] = routes_df['to'].apply(normalize_text)

    initial_len = len(df)
    df = df[df['from_normalized'].notna() & df['to_normalized'].notna()].copy()
    removed = initial_len - len(df)

    if removed > 0:
        print(f"ℹ️  Removed {removed} rows with missing/invalid addresses")

    routes_df = routes_df[routes_df['from_normalized'].notna(
    ) & routes_df['to_normalized'].notna()].copy()
    routes_df = routes_df.drop_duplicates(
        subset=['from_normalized', 'to_normalized'], keep='first')
    routes_df['from_coded'] = routes_df['from_coded'].astype('Int64')

    routes_lookup = routes_df[['from_normalized', 'to_normalized',
                               'distance_bucket', 'from_coded', 'to_coded']].copy()

    merged = df.merge(routes_lookup, on=[
                      'from_normalized', 'to_normalized'], how='left')

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

    agg['SN_pairing %'] = agg['SN_paired'] / agg['total_rides']
    agg['TP_pairing %'] = agg['TP_paired'] / agg['total_rides']

    agg['SN_acceptance %'] = np.where(
        agg['SN_paired'] > 0,
        agg['SN_accepted'] / agg['SN_paired'],
        np.nan
    )

    agg['TP_acceptance %'] = np.where(
        agg['TP_paired'] > 0,
        agg['TP_accepted'] / agg['TP_paired'],
        np.nan
    )

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
        merged['SN_paired'] > MIN_PAIRED,
        merged['SN_pair_count_raw'],
        np.nan
    )

    merged['TP_pair_count'] = np.where(
        merged['TP_paired'] > MIN_PAIRED,
        merged['SN_pair_count_raw'],
        np.nan
    )

    merged['SN_accept_count'] = np.where(
        merged['SN_paired'] > MIN_PAIRED,
        merged['SN_accept_count_raw'],
        np.nan
    )

    return merged.drop(columns=['SN_pair_count_raw', 'SN_accept_count_raw'])

# ============================
# Derived Metrics
# ============================


def calculate_derived_metrics(agg, first_two_dims):
    agg = agg.copy()

    req_group_sum = agg.groupby(first_two_dims)['req_count'].transform('sum')

    agg['req_share %'] = np.where(
        req_group_sum > MIN_PAIRED,
        agg['req_count'] / req_group_sum,
        np.nan
    )

    agg['req_share_nf %'] = np.where(
        req_group_sum > 0,
        agg['req_count'] / req_group_sum,
        np.nan
    )

    agg['pairing %'] = np.where(
        agg['req_count'] > 0,
        agg['SN_pair_count_nf'] / agg['req_count'],
        np.nan
    )

    agg['acceptance %'] = np.where(
        agg['SN_pair_count_nf'] > 0,
        agg['SN_accept_count_nf'] / agg['SN_pair_count_nf'],
        np.nan
    )

    return agg

# ============================
# AOV (from_coded only)
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
        df['pairing %'] > 0,
        df['SN_pairing %'] / df['pairing %'],
        np.nan
    )

    df['acceptance_ratio'] = np.where(
        df['acceptance %'] > 0,
        df['SN_acceptance %'] / df['acceptance %'],
        np.nan
    )

    df['pairing_3'] = np.where(
        df['pairing_ratio'] > 0,
        df['TP_pairing %'] / df['pairing_ratio'],
        np.nan
    )

    df['acceptance_3'] = np.where(
        df['acceptance_ratio'] > 0,
        df['TP_acceptance %'] / df['acceptance_ratio'],
        np.nan
    )

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
    """
    Add 'Total' aggregation rows for each combination of the first two dimensions.

    Args:
        df: DataFrame with the detailed data
        group_dims: List of the first two dimension columns (e.g., ['week_number', 'city'])
        third_dim: Name of the third dimension column to aggregate over
        real_data_df: The original real_data DataFrame to calculate totals from
    """
    result_rows = []

    # Get all columns from original dataframe to maintain order and types
    all_columns = df.columns.tolist()

    # Group by the first two dimensions
    for group_keys, group_df in df.groupby(group_dims, dropna=False):
        # Add all original rows from this group
        result_rows.append(group_df)

        # Create aggregation row as a dictionary with proper column order
        agg_row = {col: np.nan for col in all_columns}

        # Set the group dimension values
        if len(group_dims) == 2:
            agg_row[group_dims[0]] = group_keys[0]
            agg_row[group_dims[1]] = group_keys[1]

        # Set third dimension to 'Total'
        agg_row[third_dim] = 'Total'

        # Filter real_data for this specific week_number and city
        week_val = group_keys[0]
        city_val = group_keys[1]

        real_data_filtered = real_data_df[
            (real_data_df['week_number'] == week_val) &
            (real_data_df['city'] == city_val)
        ]

        # Sum directly from real_data for SN_finished_ride and AOV_real_SN
        total_ride = real_data_filtered['ride'].sum()
        total_nmv = real_data_filtered['NMV'].sum()

        # Aggregate counts (sum) - ensure they stay as numeric
        count_cols = ['total_rides', 'SN_paired', 'TP_paired', 'SN_accepted', 'TP_accepted',
                      'req_count', 'NMV_sum', 'ride_sum', 'SN_pair_count_nf', 'TP_pair_count_nf',
                      'SN_accept_count_nf', 'SN_pair_count', 'TP_pair_count', 'SN_accept_count']

        for col in count_cols:
            if col in group_df.columns:
                agg_row[col] = float(group_df[col].sum())

        # Calculate weighted averages using sumproduct for ECO metrics
        # AVG_SN_ECO (total): sumproduct of (total_rides, AVG_SN_ECO) / sum of total_rides
        if 'Avg_SN_Eco' in group_df.columns and 'total_rides' in group_df.columns:
            valid_mask = group_df['Avg_SN_Eco'].notna(
            ) & group_df['total_rides'].notna()
            if valid_mask.any():
                sumproduct = (group_df.loc[valid_mask, 'total_rides'] *
                              group_df.loc[valid_mask, 'Avg_SN_Eco']).sum()
                sum_rides = group_df.loc[valid_mask, 'total_rides'].sum()
                agg_row['Avg_SN_Eco'] = float(
                    sumproduct / sum_rides) if sum_rides > 0 else np.nan

        # AVG_TP_ECO (total): sumproduct of (total_rides, AVG_TP_ECO) / sum of total_rides
        if 'Avg_TP_Eco' in group_df.columns and 'total_rides' in group_df.columns:
            valid_mask = group_df['Avg_TP_Eco'].notna(
            ) & group_df['total_rides'].notna()
            if valid_mask.any():
                sumproduct = (group_df.loc[valid_mask, 'total_rides'] *
                              group_df.loc[valid_mask, 'Avg_TP_Eco']).sum()
                sum_rides = group_df.loc[valid_mask, 'total_rides'].sum()
                agg_row['Avg_TP_Eco'] = float(
                    sumproduct / sum_rides) if sum_rides > 0 else np.nan

        # AVG_SN_Carp (total): sumproduct of (SN_accepted, AVG_SN_Carp) / sum of SN_accepted
        if 'Avg_SN_carp' in group_df.columns and 'SN_accepted' in group_df.columns:
            valid_mask = group_df['Avg_SN_carp'].notna(
            ) & group_df['SN_accepted'].notna()
            if valid_mask.any():
                sumproduct = (group_df.loc[valid_mask, 'SN_accepted'] *
                              group_df.loc[valid_mask, 'Avg_SN_carp']).sum()
                sum_accepted = group_df.loc[valid_mask, 'SN_accepted'].sum()
                agg_row['Avg_SN_carp'] = float(
                    sumproduct / sum_accepted) if sum_accepted > 0 else np.nan

        # AVG_TP_Carp (total): sumproduct of (TP_accepted, AVG_TP_Carp) / sum of TP_accepted
        if 'Avg_TP_carp' in group_df.columns and 'TP_accepted' in group_df.columns:
            valid_mask = group_df['Avg_TP_carp'].notna(
            ) & group_df['TP_accepted'].notna()
            if valid_mask.any():
                sumproduct = (group_df.loc[valid_mask, 'TP_accepted'] *
                              group_df.loc[valid_mask, 'Avg_TP_carp']).sum()
                sum_accepted = group_df.loc[valid_mask, 'TP_accepted'].sum()
                agg_row['Avg_TP_carp'] = float(
                    sumproduct / sum_accepted) if sum_accepted > 0 else np.nan

        # AVG_SN_Psub (total): sumproduct of (SN_accepted, AVG_SN_Psub) / sum of SN_accepted
        if 'Avg_SN_Psub' in group_df.columns and 'SN_accepted' in group_df.columns:
            valid_mask = group_df['Avg_SN_Psub'].notna(
            ) & group_df['SN_accepted'].notna()
            if valid_mask.any():
                sumproduct = (group_df.loc[valid_mask, 'SN_accepted'] *
                              group_df.loc[valid_mask, 'Avg_SN_Psub']).sum()
                sum_accepted = group_df.loc[valid_mask, 'SN_accepted'].sum()
                agg_row['Avg_SN_Psub'] = float(
                    sumproduct / sum_accepted) if sum_accepted > 0 else np.nan

        # AVG_TP_Psub (total): sumproduct of (TP_accepted, AVG_TP_Psub) / sum of TP_accepted
        if 'Avg_TP_Psub' in group_df.columns and 'TP_accepted' in group_df.columns:
            valid_mask = group_df['Avg_TP_Psub'].notna(
            ) & group_df['TP_accepted'].notna()
            if valid_mask.any():
                sumproduct = (group_df.loc[valid_mask, 'TP_accepted'] *
                              group_df.loc[valid_mask, 'Avg_TP_Psub']).sum()
                sum_accepted = group_df.loc[valid_mask, 'TP_accepted'].sum()
                agg_row['Avg_TP_Psub'] = float(
                    sumproduct / sum_accepted) if sum_accepted > 0 else np.nan

        # SN_finished_ride (total): sum of "ride" from real_data directly
        agg_row['SN_finished_ride'] = float(total_ride)

        # AOV_real_SN (total): sum of "NMV" / sum of "ride" from real_data directly
        if total_ride > 0:
            agg_row['AOV_real_SN'] = float(total_nmv / total_ride)
        else:
            agg_row['AOV_real_SN'] = np.nan

        # AOV_T/R (total): sumproduct of (AOV_T/R, SN_finished_ride) / sum(SN_finished_ride)
        if 'AOV_T_R' in group_df.columns and 'SN_finished_ride' in group_df.columns:
            valid_mask = group_df['AOV_T_R'].notna(
            ) & group_df['SN_finished_ride'].notna()
            if valid_mask.any():
                sumproduct = (group_df.loc[valid_mask, 'SN_finished_ride'] *
                              group_df.loc[valid_mask, 'AOV_T_R']).sum()
                sum_finished = group_df.loc[valid_mask,
                                            'SN_finished_ride'].sum()
                agg_row['AOV_T_R'] = float(
                    sumproduct / sum_finished) if sum_finished > 0 else np.nan

        # AOV_est: recalculate based on total row values
        if 'AOV_T_R' in agg_row and agg_row['AOV_T_R'] is not np.nan and agg_row['AOV_T_R'] > 0:
            if 'Avg_TP_carp' in agg_row and agg_row['Avg_TP_carp'] is not np.nan:
                agg_row['AOV_est'] = float(
                    agg_row['Avg_TP_carp'] / agg_row['AOV_T_R'])
            else:
                agg_row['AOV_est'] = np.nan
        else:
            agg_row['AOV_est'] = np.nan

        # Recalculate percentage columns for the total row
        if 'total_rides' in agg_row and agg_row['total_rides'] > 0:
            if 'SN_paired' in agg_row:
                agg_row['SN_pairing %'] = float(
                    agg_row['SN_paired'] / agg_row['total_rides'])
            if 'TP_paired' in agg_row:
                agg_row['TP_pairing %'] = float(
                    agg_row['TP_paired'] / agg_row['total_rides'])

        if 'SN_paired' in agg_row and agg_row['SN_paired'] > 0:
            if 'SN_accepted' in agg_row:
                agg_row['SN_acceptance %'] = float(
                    agg_row['SN_accepted'] / agg_row['SN_paired'])

        if 'TP_paired' in agg_row and agg_row['TP_paired'] > 0:
            if 'TP_accepted' in agg_row:
                agg_row['TP_acceptance %'] = float(
                    agg_row['TP_accepted'] / agg_row['TP_paired'])

        if 'req_count' in agg_row and agg_row['req_count'] > 0:
            if 'SN_pair_count_nf' in agg_row:
                agg_row['pairing %'] = float(
                    agg_row['SN_pair_count_nf'] / agg_row['req_count'])

        if 'SN_pair_count_nf' in agg_row and agg_row['SN_pair_count_nf'] > 0:
            if 'SN_accept_count_nf' in agg_row:
                agg_row['acceptance %'] = float(
                    agg_row['SN_accept_count_nf'] / agg_row['SN_pair_count_nf'])

        # Recalculate ratio columns
        if 'pairing %' in agg_row and agg_row['pairing %'] > 0:
            if 'SN_pairing %' in agg_row:
                agg_row['pairing_ratio'] = float(
                    agg_row['SN_pairing %'] / agg_row['pairing %'])

        if 'acceptance %' in agg_row and agg_row['acceptance %'] > 0:
            if 'SN_acceptance %' in agg_row:
                agg_row['acceptance_ratio'] = float(
                    agg_row['SN_acceptance %'] / agg_row['acceptance %'])

        if 'pairing_ratio' in agg_row and agg_row['pairing_ratio'] > 0:
            if 'TP_pairing %' in agg_row:
                agg_row['pairing_3'] = float(
                    agg_row['TP_pairing %'] / agg_row['pairing_ratio'])

        if 'acceptance_ratio' in agg_row and agg_row['acceptance_ratio'] > 0:
            if 'TP_acceptance %' in agg_row:
                agg_row['acceptance_3'] = float(
                    agg_row['TP_acceptance %'] / agg_row['acceptance_ratio'])

        # Recalculate req_share % for total row (should be 100%)
        if 'req_count' in agg_row:
            agg_row['req_share %'] = 1.0  # Total is always 100%
            agg_row['req_share_nf %'] = 1.0

        # Create DataFrame from aggregation row with explicit column order
        agg_df = pd.DataFrame([agg_row], columns=all_columns)
        result_rows.append(agg_df)

    # Concatenate all rows
    result = pd.concat(result_rows, ignore_index=True)

    # Ensure numeric columns stay numeric (convert any that became object type)
    for col in result.columns:
        if col not in [group_dims[0], group_dims[1], third_dim]:
            # Try to convert to numeric, keep as-is if it fails
            result[col] = pd.to_numeric(result[col], errors='ignore')

    # Sort by group dimensions and third dimension (Total should come last)
    sort_cols = group_dims + [third_dim]
    result = result.sort_values(by=sort_cols, key=lambda x: x.map(
        lambda v: (0, v) if v != 'Total' else (1, v)))

    return result.reset_index(drop=True)

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

    print(f"\n📊 Data loaded:")
    print(f"  Carpooling data: {len(df):,} rows")
    print(f"  Routes:          {len(routes_df):,} routes")
    print(f"  Real data:       {len(real_data_df):,} rows")

    df = (
        df.pipe(prepare_base_df)
          .pipe(add_time_features)
          .pipe(merge_routes, routes_df)
    )

    real_data_df = prepare_real_data(real_data_df)

    print("\n" + "="*60)
    print("GENERATING OUTPUT TABLES")
    print("="*60)

    # -------- from_coded table --------
    print("\n[1/6] Building from_coded table...")
    table_from = build_table_with_real_data(
        df, real_data_df,
        dims=['week_number', 'city', 'from_coded'],
        first_two_dims=['week_number', 'city']
    )
    table_from = add_aov_metrics_for_from_table(table_from)
    table_from = add_ratio_columns(table_from)
    table_from.to_csv(OUTPUT_FROM, index=False, encoding="utf-8-sig")
    print(f"  ✓ Saved: {OUTPUT_FROM} ({len(table_from):,} rows)")

    print("\n[2/6] Building from_coded table with aggregations...")
    table_from_agg = add_aggregation_rows(
        table_from,
        group_dims=['week_number', 'city'],
        third_dim='from_coded',
        real_data_df=real_data_df
    )
    table_from_agg = format_output(table_from_agg)
    table_from_agg.to_excel(OUTPUT_FROM_AGG, index=False, engine='openpyxl')
    print(f"  ✓ Saved: {OUTPUT_FROM_AGG} ({len(table_from_agg):,} rows)")

    # -------- time_bucket table --------
    print("\n[3/6] Building time_bucket table...")
    table_time = build_table_with_real_data(
        df, real_data_df,
        dims=['week_number', 'city', 'time_bucket'],
        first_two_dims=['week_number', 'city']
    )
    table_time = add_ratio_columns(table_time)
    table_time.to_csv(OUTPUT_TIME, index=False, encoding="utf-8-sig")
    print(f"  ✓ Saved: {OUTPUT_TIME} ({len(table_time):,} rows)")

    print("\n[4/6] Building time_bucket table with aggregations...")
    table_time_agg = add_aggregation_rows(
        table_time,
        group_dims=['week_number', 'city'],
        third_dim='time_bucket',
        real_data_df=real_data_df
    )
    table_time_agg = format_output(table_time_agg)
    table_time_agg.to_excel(OUTPUT_TIME_AGG, index=False, engine='openpyxl')
    print(f"  ✓ Saved: {OUTPUT_TIME_AGG} ({len(table_time_agg):,} rows)")

    # -------- distance_bucket table --------
    print("\n[5/6] Building distance_bucket table...")
    table_distance = build_table_with_real_data(
        df, real_data_df,
        dims=['week_number', 'city', 'distance_bucket'],
        first_two_dims=['week_number', 'city']
    )
    table_distance = add_ratio_columns(table_distance)
    table_distance.to_csv(OUTPUT_DISTANCE, index=False, encoding="utf-8-sig")
    print(f"  ✓ Saved: {OUTPUT_DISTANCE} ({len(table_distance):,} rows)")

    print("\n[6/6] Building distance_bucket table with aggregations...")
    table_distance_agg = add_aggregation_rows(
        table_distance,
        group_dims=['week_number', 'city'],
        third_dim='distance_bucket',
        real_data_df=real_data_df
    )
    table_distance_agg = format_output(table_distance_agg)
    table_distance_agg.to_excel(
        OUTPUT_DISTANCE_AGG, index=False, engine='openpyxl')
    print(
        f"  ✓ Saved: {OUTPUT_DISTANCE_AGG} ({len(table_distance_agg):,} rows)")

    print("\n" + "="*60)
    print("✅ AGGREGATION COMPLETE")
    print("="*60)
    print("\nOriginal output files:")
    print(f"  1. {OUTPUT_FROM}")
    print(f"  2. {OUTPUT_TIME}")
    print(f"  3. {OUTPUT_DISTANCE}")
    print("\nAggregated output files (with Total rows):")
    print(f"  4. {OUTPUT_FROM_AGG}")
    print(f"  5. {OUTPUT_TIME_AGG}")
    print(f"  6. {OUTPUT_DISTANCE_AGG}")

    print("\n" + "="*60)
    print("AGGREGATION SUMMARY")
    print("="*60)
    print("\nEach aggregated file includes:")
    print("  - All original detailed rows")
    print("  - 'Total' rows for each (week_number, city) combination")
    print("  - Counts are summed")
    print("  - Weighted averages using sumproduct for ECO/Carp/Psub")
    print("  - AOV_real_SN & SN_finished_ride calculated from real_data directly")
    print("  - Percentages are recalculated for totals")


if __name__ == "__main__":
    main()
