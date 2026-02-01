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
# Routes Merge - FIXED VERSION
# ============================


def merge_routes(df, routes_df):
    """
    Merge routes with robust text normalization to handle spacing variations
    """
    # Prepare routes dataframe
    routes_df = routes_df.copy()
    routes_df = routes_df.rename(columns={
        'Origin_Add': 'from',
        'Destination_Add': 'to',
        'Distance': 'distance_bucket',
        'Or': 'from_coded',
        'DstDistID': 'to_coded'
    })

    # Text normalization function
    def normalize_text(text):
        """
        Normalize Persian text by removing all spaces and invisible characters
        This handles spacing variations like 'چهار باغ' vs 'چهارباغ'
        """
        if pd.isna(text):
            return None

        text = str(text).strip()

        # Filter out 'nan' strings
        if text.lower() == 'nan' or text == '':
            return None

        # Replace Arabic characters with Persian equivalents
        text = text.replace('ي', 'ی').replace('ك', 'ک')

        # Remove all invisible Unicode characters
        text = text.replace('\u200c', '')  # Zero-width non-joiner
        text = text.replace('\u200b', '')  # Zero-width space
        text = text.replace('\u200d', '')  # Zero-width joiner
        text = text.replace('\ufeff', '')  # Zero-width no-break space
        text = text.replace('\xa0', '')    # Non-breaking space
        text = text.replace('\u202a', '')  # Left-to-right embedding
        text = text.replace('\u202b', '')  # Right-to-left embedding
        text = text.replace('\u202c', '')  # Pop directional formatting

        # CRITICAL: Remove ALL spaces to handle spacing variations
        text = text.replace(' ', '')

        # Convert to lowercase for case-insensitive matching
        text = text.lower()

        return text

    # Keep original values
    df['from_original'] = df['from'].copy()
    df['to_original'] = df['to'].copy()

    # Create normalized columns for matching
    df['from_normalized'] = df['from'].apply(normalize_text)
    df['to_normalized'] = df['to'].apply(normalize_text)

    routes_df['from_normalized'] = routes_df['from'].apply(normalize_text)
    routes_df['to_normalized'] = routes_df['to'].apply(normalize_text)

    # Remove rows with null addresses from main data
    initial_len = len(df)
    df = df[df['from_normalized'].notna() & df['to_normalized'].notna()].copy()
    removed = initial_len - len(df)

    if removed > 0:
        print(f"ℹ️  Removed {removed} rows with missing/invalid addresses")

    # Remove rows with null addresses from routes
    routes_df = routes_df[routes_df['from_normalized'].notna(
    ) & routes_df['to_normalized'].notna()].copy()

    # Remove duplicate routes (keep first occurrence)
    routes_df = routes_df.drop_duplicates(
        subset=['from_normalized', 'to_normalized'], keep='first')

    # Convert from_coded to Int64 for proper handling of NaN
    routes_df['from_coded'] = routes_df['from_coded'].astype('Int64')

    # Prepare lookup table with only necessary columns
    routes_lookup = routes_df[['from_normalized', 'to_normalized',
                               'distance_bucket', 'from_coded', 'to_coded']].copy()

    # Perform the merge using normalized columns
    merged = df.merge(
        routes_lookup,
        on=['from_normalized', 'to_normalized'],
        how='left'
    )

    # Check for unmatched routes
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
        print(
            f"  These rows will have NULL values in from_coded and distance_bucket columns")
        print(f"  Check your Route.xlsx file to ensure all routes are included")

        # Save unmatched routes for review
        unmatched_routes = merged[merged['from_coded'].isna(
        )][['from_original', 'to_original']].drop_duplicates()
        if len(unmatched_routes) > 0:
            unmatched_routes.to_csv(
                'UNMATCHED_ROUTES_CHECK.csv', index=False, encoding='utf-8-sig')
            print(
                f"  → Saved {len(unmatched_routes)} unique unmatched routes to: UNMATCHED_ROUTES_CHECK.csv")

    # Clean up: remove normalized columns and restore original from/to
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

    # Create no-filter versions (nf) - these are always calculated
    merged['SN_pair_count_nf'] = merged['SN_pair_count_raw']
    merged['TP_pair_count_nf'] = merged['SN_pair_count_raw']
    merged['SN_accept_count_nf'] = merged['SN_accept_count_raw']

    # Create filtered versions - these apply MIN_PAIRED filter
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

    # req_share % with filter (original)
    agg['req_share %'] = np.where(
        req_group_sum > MIN_PAIRED,
        agg['req_count'] / req_group_sum,
        np.nan
    )

    # req_share_nf % without filter (new)
    agg['req_share_nf %'] = np.where(
        req_group_sum > 0,
        agg['req_count'] / req_group_sum,
        np.nan
    )

    # UPDATED: pairing % now uses SN_pair_count_nf (no filter)
    agg['pairing %'] = np.where(
        agg['req_count'] > 0,
        agg['SN_pair_count_nf'] / agg['req_count'],
        np.nan
    )

    # UPDATED: acceptance % now uses SN_pair_count_nf and SN_accept_count_nf (no filter)
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
    print("CARPOOLING DATA AGGREGATION - REVISED WITH NF METRICS")
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
    print("\n[1/3] Building from_coded table...")
    table_from = build_table_with_real_data(
        df, real_data_df,
        dims=['week_number', 'city', 'from_coded'],
        first_two_dims=['week_number', 'city']
    )
    table_from = add_aov_metrics_for_from_table(table_from)
    table_from = add_ratio_columns(table_from)
    table_from.to_csv(OUTPUT_FROM, index=False, encoding="utf-8-sig")
    print(f"  ✓ Saved: {OUTPUT_FROM} ({len(table_from):,} rows)")

    # -------- time_bucket table --------
    print("\n[2/3] Building time_bucket table...")
    table_time = build_table_with_real_data(
        df, real_data_df,
        dims=['week_number', 'city', 'time_bucket'],
        first_two_dims=['week_number', 'city']
    )
    table_time = add_ratio_columns(table_time)
    table_time.to_csv(OUTPUT_TIME, index=False, encoding="utf-8-sig")
    print(f"  ✓ Saved: {OUTPUT_TIME} ({len(table_time):,} rows)")

    # -------- distance_bucket table --------
    print("\n[3/3] Building distance_bucket table...")
    table_distance = build_table_with_real_data(
        df, real_data_df,
        dims=['week_number', 'city', 'distance_bucket'],
        first_two_dims=['week_number', 'city']
    )
    table_distance = add_ratio_columns(table_distance)
    table_distance.to_csv(OUTPUT_DISTANCE, index=False, encoding="utf-8-sig")
    print(f"  ✓ Saved: {OUTPUT_DISTANCE} ({len(table_distance):,} rows)")

    print("\n" + "="*60)
    print("✅ AGGREGATION COMPLETE")
    print("="*60)
    print("\nOutput files generated:")
    print(f"  1. {OUTPUT_FROM}")
    print(f"  2. {OUTPUT_TIME}")
    print(f"  3. {OUTPUT_DISTANCE}")

    # Check for NULL buckets in outputs
    print("\n" + "="*60)
    print("DATA QUALITY CHECK")
    print("="*60)

    null_from = table_from['from_coded'].isna().sum()
    null_time = table_time['time_bucket'].isna().sum()
    null_dist = table_distance['distance_bucket'].isna().sum()

    print(f"\nNull values in grouping columns:")
    print(f"  from_coded:       {null_from} rows")
    print(f"  time_bucket:      {null_time} rows")
    print(f"  distance_bucket:  {null_dist} rows")

    if null_from == 0 and null_time == 0 and null_dist == 0:
        print("\n✓ All grouping columns are complete - no NULL values!")
    else:
        print("\n⚠️  Some grouping columns have NULL values")
        print("   This is normal if some rides couldn't be matched to routes")

    print("\n" + "="*60)
    print("METRICS SUMMARY")
    print("="*60)
    print("\nFiltered metrics (with MIN_PAIRED filter):")
    print("  - SN_pair_count")
    print("  - TP_pair_count")
    print("  - SN_accept_count")
    print("  - req_share %")
    print("\nNo-filter metrics (nf - without MIN_PAIRED):")
    print("  - SN_pair_count_nf")
    print("  - TP_pair_count_nf")
    print("  - SN_accept_count_nf")
    print("  - req_share_nf %")
    print("\nUpdated formulas using nf metrics:")
    print("  - pairing % = SN_pair_count_nf / req_count")
    print("  - acceptance % = SN_accept_count_nf / SN_pair_count_nf")


if __name__ == "__main__":
    main()
