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
# Routes Merge
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

    routes_df['from_coded'] = routes_df['from_coded'].astype('Int64')

    return df.merge(
        routes_df[['from', 'to', 'distance_bucket', 'from_coded', 'to_coded']],
        how='left',
        on=['from', 'to']
    )

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

    agg['pairing %'] = np.where(
        agg['req_count'] > 0,
        agg['SN_pair_count'] / agg['req_count'],
        np.nan
    )

    agg['acceptance %'] = np.where(
        agg['SN_pair_count'] > 0,
        agg['SN_accept_count'] / agg['SN_pair_count'],
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
    table_from.to_csv(OUTPUT_FROM, index=False, encoding="utf-8-sig")

    # -------- time_bucket table --------
    table_time = build_table_with_real_data(
        df, real_data_df,
        dims=['week_number', 'city', 'time_bucket'],
        first_two_dims=['week_number', 'city']
    )
    table_time = add_ratio_columns(table_time)
    table_time.to_csv(OUTPUT_TIME, index=False, encoding="utf-8-sig")

    # -------- distance_bucket table --------
    table_distance = build_table_with_real_data(
        df, real_data_df,
        dims=['week_number', 'city', 'distance_bucket'],
        first_two_dims=['week_number', 'city']
    )
    table_distance = add_ratio_columns(table_distance)
    table_distance.to_csv(OUTPUT_DISTANCE, index=False, encoding="utf-8-sig")

    print("✅ Aggregation complete with AOV + 4 ratio columns (numeric-safe).")


if __name__ == "__main__":
    main()
