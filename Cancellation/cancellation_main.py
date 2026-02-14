import pandas as pd
import numpy as np

# ============================
# Paths
# ============================

CSV_PATH = r"D:\OneDrive\Work\Cancellation\DataSources\carpooling_export_7.csv"
EXCEL_PATH = r"D:\OneDrive\Work\Cancellation\DataSources\Route.xlsx"
REAL_DATA_PATH = r"D:\OneDrive\Work\Cancellation\DataSources\real_data_7.csv"

OUTPUT_FROM = r"D:\OneDrive\Work\Cancellation\Outputs\Origin_Bucket.csv"
OUTPUT_TIME = r"D:\OneDrive\Work\Cancellation\Outputs\Time_Bucket.csv"
OUTPUT_DISTANCE = r"D:\OneDrive\Work\Cancellation\Outputs\Distance_Bucket.csv"

OUTPUT_FROM_AGG = r"D:\OneDrive\Work\Cancellation\Outputs\Origin_Bucket_Agg.xlsx"
OUTPUT_TIME_AGG = r"D:\OneDrive\Work\Cancellation\Outputs\Time_Bucket_Agg.xlsx"
OUTPUT_DISTANCE_AGG = r"D:\OneDrive\Work\Cancellation\Outputs\Distance_Bucket_Agg.xlsx"

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
    # --- Flag columns to normalize ---
    flag_cols = ['snapp_paired', 'tapsi_paired',
                 'snapp_accepted', 'tapsi_accepted']

    # --- Normalize flags to lowercase & strip whitespace ---
    for col in flag_cols:
        df[col] = df[col].astype(str).str.strip().str.lower()

   # --- Remove logically impossible acceptance rows - --
    invalid_mask = (
        ((df['snapp_paired'] == 'no') & (df['snapp_accepted'] == 'yes')) |
        ((df['tapsi_paired'] == 'no') & (df['tapsi_accepted'] == 'yes'))
    )
    removed_rows = invalid_mask.sum()
    if removed_rows > 0:
        print(f"🧹 Removed {removed_rows:,} invalid acceptance rows")
    df = df.loc[~invalid_mask].copy()

    # --- Drop duplicates and reset index ---
    df = df.drop_duplicates().reset_index(drop=True)

    # --- Assign unique ride IDs ---
    df['ride_id'] = np.arange(len(df))

    # --- Convert flags to boolean ---
    for col in flag_cols:
        df[col] = df[col].eq('yes')

    # --- Fare columns ---
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
    Add 'Total' aggregation rows with specific Weighted Average logic.
    """
    result_rows = []

    # --- Helper: Weighted Average ---
    def get_weighted_avg(sub_df, val_col, weight_col):
        """
        Calculates weighted average, automatically dropping rows where 
        either the value or the weight is NaN (respecting MIN_PAIRED filters).
        """
        if val_col not in sub_df.columns or weight_col not in sub_df.columns:
            return np.nan

        # Filter for valid rows only
        clean_df = sub_df[[val_col, weight_col]].dropna()

        if len(clean_df) == 0 or clean_df[weight_col].sum() == 0:
            return np.nan

        return np.average(clean_df[val_col], weights=clean_df[weight_col])

    # Get all columns
    all_columns = df.columns.tolist()

    # Group by the first two dimensions
    for group_keys, group_df in df.groupby(group_dims, dropna=False):

        # 1. Keep original rows
        result_rows.append(group_df)

        # 2. Initialize Total Row
        agg_row = {col: np.nan for col in all_columns}

        if len(group_dims) == 2:
            agg_row[group_dims[0]] = group_keys[0]
            agg_row[group_dims[1]] = group_keys[1]
        agg_row[third_dim] = 'Total'

        # --- Filter Real Data for Direct Calculations ---
        week_val = group_keys[0]
        city_val = group_keys[1]
        rd_filtered = real_data_df[
            (real_data_df['week_number'] == week_val) &
            (real_data_df['city'] == city_val)
        ]

        # =========================================
        # A. Simple Sums (Counts)
        # =========================================
        sum_cols = [
            'total_rides', 'SN_paired', 'TP_paired', 'SN_accepted', 'TP_accepted',
            'req_count', 'NMV_sum', 'ride_sum',
            'SN_pair_count_nf', 'TP_pair_count_nf', 'SN_accept_count_nf',
            'SN_pair_count', 'TP_pair_count', 'SN_accept_count'
        ]
        for col in sum_cols:
            if col in group_df.columns:
                # returns 0 if empty, preserve float
                agg_row[col] = group_df[col].sum(min_count=0)

        # =========================================
        # B. Real Data Direct Calculations
        # =========================================
        # pairing % = sum(pairs) / sum(reqs) (from real_data)
        rd_pairs = rd_filtered['pairs'].sum()
        rd_reqs = rd_filtered['reqs'].sum()
        agg_row['pairing %'] = (rd_pairs / rd_reqs) if rd_reqs > 0 else np.nan

        # acceptance % = sum(accepts) / sum(pairs) (from real_data)
        rd_accepts = rd_filtered['accepts'].sum()
        agg_row['acceptance %'] = (
            rd_accepts / rd_pairs) if rd_pairs > 0 else np.nan

        # SN_finished_ride & AOV_real_SN
        total_ride = rd_filtered['ride'].sum()
        total_nmv = rd_filtered['NMV'].sum()
        agg_row['SN_finished_ride'] = float(total_ride)
        agg_row['AOV_real_SN'] = (
            total_nmv / total_ride) if total_ride > 0 else np.nan

        # =========================================
        # C. Weighted Averages (The 4 Specific Fixes)
        # =========================================

        # 1. SN_pairing % (Weight: req_share %)
        agg_row['SN_pairing %'] = get_weighted_avg(
            group_df, 'SN_pairing %', 'req_share %')

        # 2. TP_pairing % (Weight: req_share %)
        agg_row['TP_pairing %'] = get_weighted_avg(
            group_df, 'TP_pairing %', 'req_share %')

        # 3. SN_acceptance % (Weight: SN_pair_count)
        agg_row['SN_acceptance %'] = get_weighted_avg(
            group_df, 'SN_acceptance %', 'SN_pair_count')

        # 4. TP_acceptance % (Weight: TP_pair_count)
        agg_row['TP_acceptance %'] = get_weighted_avg(
            group_df, 'TP_acceptance %', 'TP_pair_count')

        # =========================================
        # D. Other Weighted Averages
        # =========================================

        # Other metrics weighted by req_share %
        for col in ['pairing_ratio', 'acceptance_ratio', 'pairing_3', 'acceptance_3']:
            agg_row[col] = get_weighted_avg(group_df, col, 'req_share %')

        # Fares weighted by Rides/Accepted
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

        # =========================================
        # E. Final Cleanup
        # =========================================

        # Recalculate AOV_est from total row values
        if pd.notna(agg_row.get('AOV_T_R')) and agg_row['AOV_T_R'] > 0 and pd.notna(agg_row.get('Avg_TP_carp')):
            agg_row['AOV_est'] = agg_row['Avg_TP_carp'] / agg_row['AOV_T_R']

        # Total req_share is always 100%
        agg_row['req_share %'] = 1.0
        agg_row['req_share_nf %'] = 1.0

        # Create DataFrame row
        agg_df = pd.DataFrame([agg_row], columns=all_columns)
        result_rows.append(agg_df)

    # Concatenate
    result = pd.concat(result_rows, ignore_index=True)

    # Ensure numeric types
    for col in result.columns:
        if col not in [group_dims[0], group_dims[1], third_dim]:
            result[col] = pd.to_numeric(result[col], errors='ignore')

    # Sort
    sort_cols = group_dims + [third_dim]
    result = result.sort_values(
        by=sort_cols,
        key=lambda x: x.map(lambda v: (0, v) if v != 'Total' else (1, v))
    )

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
