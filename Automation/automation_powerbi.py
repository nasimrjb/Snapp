import pandas as pd
import numpy as np
from datetime import time


# ============================
# Paths
# ============================

CSV_PATH = r"D:\Work\Automation Project\DataSources\carpooling_export_11_10_to_01_21.csv"
EXCEL_PATH = r"D:\Work\Automation Project\DataSources\AllAvailableRoutes.xlsx"
REAL_DATA_PATH = r"D:\Work\Automation Project\DataSources\real_data_11_10_to_01_23.csv"

OUTPUT_FROM = r"D:\Work\Automation Project\Outputs\weekly_city_from_coded.csv"
OUTPUT_TIME = r"D:\Work\Automation Project\Outputs\weekly_city_time_bucket.csv"
OUTPUT_DISTANCE = r"D:\Work\Automation Project\Outputs\weekly_city_distance_bucket.csv"

MIN_PAIRED = 9


# ============================
# Load & prepare data
# ============================

def load_data(csv_path, excel_path, real_data_path):
    return (
        pd.read_csv(csv_path, encoding="utf-8-sig"),
        pd.read_excel(excel_path),
        pd.read_csv(real_data_path, encoding="utf-8-sig")
    )


def prepare_base_df(df):
    """
    - Remove duplicates
    - Create unique ride_id
    - Convert Yes/No flags to boolean
    """
    flag_cols = ['snapp_paired', 'tapsi_paired',
                 'snapp_accepted', 'tapsi_accepted']

    df = df.drop_duplicates().reset_index(drop=True)
    df['ride_id'] = np.arange(len(df))

    for col in flag_cols:
        df[col] = df[col].eq('Yes')

    return df


def prepare_real_data(df):
    """
    - Map org_city_id to city
    - Rename Week_Num to week_number
    - Map org_dist_id to from_coded
    - Prepare for aggregation
    """
    city_mapping = {
        1.0: "Tehran",
        2.0: "Karaj",
        5.0: "Mashhad"
    }

    df = df.copy()

    # Create city column from org_city_id
    df['city'] = df['org_city_id'].map(city_mapping)

    # Rename columns to match main dataframe
    df = df.rename(columns={
        'Week_Num': 'week_number',
        'org_dist_id': 'from_coded'
    })

    # Convert to int for from_coded (to match main data)
    df['from_coded'] = df['from_coded'].astype('Int64')

    # Ensure numeric columns
    df['reqs'] = pd.to_numeric(
        df['reqs'], errors='coerce').fillna(0).astype(int)
    df['pairs'] = pd.to_numeric(
        df['pairs'], errors='coerce').fillna(0).astype(int)
    df['accepts'] = pd.to_numeric(
        df['accepts'], errors='coerce').fillna(0).astype(int)

    print("\nReal data prepared:")
    print(f"Shape: {df.shape}")
    print(f"City distribution:\n{df['city'].value_counts()}")
    print(
        f"week_number distribution:\n{df['week_number'].value_counts().sort_index()}")
    print(f"time_bucket distribution:\n{df['time_bucket'].value_counts()}")

    return df


# ============================
# Time features
# ============================

def add_time_features(df):
    # Convert travel_date to datetime if it's not already
    df['travel_date'] = pd.to_datetime(df['travel_date'], errors='coerce')

    # Print first few values to debug
    print("Sample travel_time values before conversion:")
    print(df['travel_time'].head(10))
    print(f"travel_time dtype: {df['travel_time'].dtype}")

    # Try to parse travel_time - handle various formats
    # First, ensure it's a string
    df['travel_time'] = df['travel_time'].astype(str)

    # Parse as datetime, trying multiple formats
    df['travel_time_dt'] = pd.to_datetime(
        df['travel_time'],
        format="%H:%M:%S",
        errors='coerce'
    )

    # If that didn't work, try H:M format
    mask = df['travel_time_dt'].isna()
    if mask.any():
        df.loc[mask, 'travel_time_dt'] = pd.to_datetime(
            df.loc[mask, 'travel_time'],
            format="%H:%M",
            errors='coerce'
        )

    # If still issues, try inferring format
    mask = df['travel_time_dt'].isna()
    if mask.any():
        df.loc[mask, 'travel_time_dt'] = pd.to_datetime(
            df.loc[mask, 'travel_time'],
            errors='coerce'
        )

    print("\nSample travel_time_dt after conversion:")
    print(df['travel_time_dt'].head(10))
    print(f"Null count: {df['travel_time_dt'].isna().sum()}")

    def bucket(dt):
        if pd.isna(dt):
            return None
        hour = dt.hour
        if hour < 9:
            return "06_09"
        elif hour < 15:
            return "09_15"
        elif hour < 18:
            return "15_18"
        elif hour < 21:
            return "18_21"
        return None

    df['time_bucket'] = df['travel_time_dt'].apply(bucket)

    print("\ntime_bucket distribution:")
    print(df['time_bucket'].value_counts(dropna=False))

    df['week_number'] = df['travel_date'].dt.isocalendar().week.astype(int)
    df['week_number'] += (df['travel_date'].dt.weekday >= 5).astype(int)

    return df


# ============================
# Merge routes lookup
# ============================

def merge_routes(df, routes_df):
    routes_df = routes_df.rename(columns={
        'Origin_Add': 'from',
        'Destination_Add': 'to',
        'Distance': 'distance_bucket',
        'Or': 'from_coded',
        'DstDistID': 'to_coded'
    })

    for col in ['from', 'to']:
        df[col] = df[col].astype(str).str.strip()
        routes_df[col] = routes_df[col].astype(str).str.strip()

    return df.merge(
        routes_df[['from', 'to', 'distance_bucket', 'from_coded', 'to_coded']],
        how='left',
        on=['from', 'to']
    )


# ============================
# Aggregation with ACCEPTED-based averages
# ============================

def aggregate_metrics(df, dims):
    df = df.copy()

    # Mask fares ONLY for carpool-based products (accepted only)
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

            # Carpool & Psub → accepted only
            Avg_SN_carp=('snapp_before_fare_acc', 'mean'),
            Avg_SN_Psub=('snapp_after_fare_acc', 'mean'),

            Avg_TP_carp=('tapsi_before_fare_acc', 'mean'),
            Avg_TP_Psub=('tapsi_after_fare_acc', 'mean'),

            # Eco → no filtering
            Avg_SN_Eco=('snapp_normal_fare', 'mean'),
            Avg_TP_Eco=('tapsi_normal_fare', 'mean'),
        )
        .reset_index()
    )

    # Pairing rates
    agg['SN_pairing %'] = agg['SN_paired'] / agg['total_rides']
    agg['TP_pairing %'] = agg['TP_paired'] / agg['total_rides']

    # Acceptance rates
    agg['SN_acceptance %'] = np.where(
        agg['SN_paired'] > 0,
        agg['SN_accepted'] / agg['SN_paired'],
        0
    )

    agg['TP_acceptance %'] = np.where(
        agg['TP_paired'] > 0,
        agg['TP_accepted'] / agg['TP_paired'],
        0
    )

    return agg


def aggregate_real_data(df, dims):
    """
    Aggregate real data by given dimensions.
    Calculates req_count, pair counts (conditional on min_paired), and acceptance counts.
    """
    df = df.copy()

    # Filter out rows where any dimension is NaN
    df = df.dropna(subset=dims, how='any')

    agg = (
        df.groupby(dims, dropna=False)
        .agg(
            req_count=('reqs', 'sum'),
            SN_pair_count_raw=('pairs', 'sum'),
            SN_accept_count_raw=('accepts', 'sum'),
        )
        .reset_index()
    )

    print(f"\nReal data aggregated by {dims}:")
    print(f"Aggregation shape: {agg.shape}")
    print(f"Sample:\n{agg.head()}")

    return agg


def merge_real_data_with_main(main_agg, real_agg, dims):
    """
    Merge real data aggregation with main aggregation.
    Apply conditional logic for pair/accept counts based on SN_paired threshold.
    """
    print(f"\n--- Merging real data for dims: {dims} ---")
    print(f"Main agg shape before merge: {main_agg.shape}")
    print(f"Real agg shape before merge: {real_agg.shape}")

    merged = main_agg.merge(
        real_agg,
        on=dims,
        how='left'
    )

    print(f"Merged shape: {merged.shape}")

    # Fill missing real data with 0
    merged['req_count'] = merged['req_count'].fillna(0).astype(int)
    merged['SN_pair_count_raw'] = merged['SN_pair_count_raw'].fillna(
        0).astype(int)
    merged['SN_accept_count_raw'] = merged['SN_accept_count_raw'].fillna(
        0).astype(int)

    # Apply conditional logic
    merged['SN_pair_count'] = np.where(
        merged['SN_paired'] > MIN_PAIRED,
        merged['SN_pair_count_raw'],
        np.nan
    )

    merged['TP_pair_count'] = np.where(
        merged['TP_paired'] > MIN_PAIRED,
        merged['SN_pair_count_raw'],  # Using SN values for TP as well
        np.nan
    )

    merged['SN_accept_count'] = np.where(
        merged['SN_paired'] > MIN_PAIRED,
        merged['SN_accept_count_raw'],
        np.nan
    )

    # Drop raw columns
    merged = merged.drop(columns=['SN_pair_count_raw', 'SN_accept_count_raw'])

    return merged


def calculate_derived_metrics(agg, first_two_dims):
    """
    Calculate req_share %, pairing %, and acceptance %.
    first_two_dims: list of first two dimension names for req_share grouping
    """
    agg = agg.copy()

    # Calculate total req_count by first two dimensions for req_share %
    req_share_group = agg.groupby(first_two_dims, dropna=False)[
        'req_count'].transform('sum')

    agg['req_share %'] = np.where(
        req_share_group > MIN_PAIRED,
        agg['req_count'] / req_share_group,
        np.nan
    )

    # Pairing %: SN_pair_count / req_count
    agg['pairing %'] = np.where(
        agg['req_count'] > 0,
        agg['SN_pair_count'] / agg['req_count'],
        np.nan
    )

    # Acceptance %: SN_accept_count / SN_pair_count
    agg['acceptance %'] = np.where(
        agg['SN_pair_count'] > 0,
        agg['SN_accept_count'] / agg['SN_pair_count'],
        np.nan
    )

    return agg


# ============================
# Formatting for BI
# ============================

def format_output(df):
    int_cols = ['total_rides', 'SN_paired',
                'TP_paired', 'SN_accepted', 'TP_accepted', 'req_count',
                'SN_pair_count', 'TP_pair_count', 'SN_accept_count']

    avg_cols = [c for c in df.columns if c.startswith('Avg_')]
    pct_cols = [c for c in df.columns if '%' in c]

    # Convert int columns - handle NaN
    for col in int_cols:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: int(x) if pd.notna(x) else x)

    df[avg_cols] = df[avg_cols].round(1)
    df[pct_cols] = df[pct_cols].round(3)

    return df


# ============================
# Build final table with real data integration
# ============================

def build_table_with_real_data(df, real_data_df, dims, first_two_dims):
    """
    Build aggregated table by:
    1. Aggregating main carpooling data
    2. Aggregating real data
    3. Merging both
    4. Calculating derived metrics
    5. Formatting output
    """
    # Aggregate main data
    main_agg = aggregate_metrics(df, dims)

    # Aggregate real data
    real_agg = aggregate_real_data(real_data_df, dims)

    # Merge real data into main aggregation
    merged_agg = merge_real_data_with_main(main_agg, real_agg, dims)

    # Calculate derived metrics
    merged_agg = calculate_derived_metrics(merged_agg, first_two_dims)

    # Format for output
    merged_agg = format_output(merged_agg)

    return merged_agg


# ============================
# Main
# ============================

def main():
    df, routes_df, real_data_df = load_data(
        CSV_PATH, EXCEL_PATH, REAL_DATA_PATH)

    print("\n" + "="*60)
    print("=== REAL DATA INSPECTION ===")
    print("="*60)
    print(f"\nReal data shape: {real_data_df.shape}")
    print(f"\nReal data columns:\n{real_data_df.columns.tolist()}")
    print(f"\nFirst few rows:")
    print(real_data_df.head(10))
    print(f"\nData types:")
    print(real_data_df.dtypes)
    print("="*60 + "\n")

    df = (
        df.pipe(prepare_base_df)
          .pipe(add_time_features)
          .pipe(merge_routes, routes_df)
    )

    real_data_df = prepare_real_data(real_data_df)

    # Build tables with real data integration
    table_from = build_table_with_real_data(
        df,
        real_data_df,
        dims=['week_number', 'city', 'from_coded'],
        first_two_dims=['week_number', 'city']
    )

    table_time = build_table_with_real_data(
        df,
        real_data_df,
        dims=['week_number', 'city', 'time_bucket'],
        first_two_dims=['week_number', 'city']
    )

    table_distance = build_table_with_real_data(
        df,
        real_data_df,
        dims=['week_number', 'city', 'distance_bucket'],
        first_two_dims=['week_number', 'city']
    )

    # Save outputs
    table_from.to_csv(OUTPUT_FROM, index=False, encoding="utf-8-sig")
    table_time.to_csv(OUTPUT_TIME, index=False, encoding="utf-8-sig")
    table_distance.to_csv(OUTPUT_DISTANCE, index=False, encoding="utf-8-sig")

    print("✅ Aggregation complete with real data integration.")
    print(f"   - {OUTPUT_FROM}")
    print(f"   - {OUTPUT_TIME}")
    print(f"   - {OUTPUT_DISTANCE}")


if __name__ == "__main__":
    main()
