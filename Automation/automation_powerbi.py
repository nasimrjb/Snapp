import pandas as pd
import numpy as np

# ============================
# File paths
# ============================
CSV_PATH = r"D:\Work\Automation Project\DataSources\carpooling_export.csv"
EXCEL_PATH = r"D:\Work\Automation Project\DataSources\AllAvailableRoutes.xlsx"

OUTPUT_FROM = r"D:\Work\Automation Project\My Exploration\weekly_city_from_coded.csv"
OUTPUT_TIME = r"D:\Work\Automation Project\My Exploration\weekly_city_timebucket.csv"
OUTPUT_DISTANCE = r"D:\Work\Automation Project\My Exploration\weekly_city_distancebucket.csv"

# ============================
# Load & prepare base data
# ============================


def load_data(csv_path, excel_path):
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    routes_df = pd.read_excel(excel_path)
    return df, routes_df


def ensure_distinct_rides(df):
    df = df.drop_duplicates().reset_index(drop=True)
    df['ride_id'] = np.arange(len(df))
    return df


# ============================
# Feature engineering
# ============================
def assign_time_bucket(t):
    if pd.isna(t):
        return None
    if t < pd.to_datetime("09:00").time():
        return "06_09"
    elif t < pd.to_datetime("15:00").time():
        return "09_15"
    elif t < pd.to_datetime("18:00").time():
        return "15_18"
    elif t < pd.to_datetime("21:00").time():
        return "18_21"
    return None


def add_time_features(df):
    df['travel_time'] = pd.to_datetime(
        df['travel_time'], errors='coerce').dt.time
    df['time_bucket'] = df['travel_time'].apply(assign_time_bucket)

    df['travel_date'] = pd.to_datetime(df['travel_date'], errors='coerce')
    df['week_number'] = df['travel_date'].dt.isocalendar().week.astype(int)
    df['week_number'] += (df['travel_date'].dt.weekday >= 5).astype(int)

    return df


# ============================
# Routes lookup merge
# ============================
def prepare_routes_lookup(df, routes_df):
    routes_df = routes_df.rename(columns={
        'Origin_Add': 'from',
        'Destination_Add': 'to',
        'Distance': 'distance-bucket',
        'Or': 'from_coded',
        'DstDistID': 'to_coded'
    })

    for col in ['from', 'to']:
        df[col] = df[col].astype(str).str.strip()
        routes_df[col] = routes_df[col].astype(str).str.strip()

    df = df.merge(
        routes_df[['from', 'to', 'distance-bucket', 'from_coded', 'to_coded']],
        how='left',
        on=['from', 'to']
    )
    return df


# ============================
# Boolean conversion
# ============================
def convert_yes_no(df, cols):
    for col in cols:
        df[col] = df[col].eq('Yes')
    return df


# ============================
# Aggregation core
# ============================
def build_aggregation(df, group_cols):
    agg = df.groupby(group_cols, dropna=False).agg(
        total_rides=('ride_id', 'nunique'),
        SN_paired=('snapp_paired', 'sum'),
        TP_paired=('tapsi_paired', 'sum'),
        SN_accepted=('snapp_accepted', 'sum'),
        TP_accepted=('tapsi_accepted', 'sum'),
        Avg_SN_carp=('snapp_before_fare', 'mean'),
        Avg_TP_carp=('tapsi_before_fare', 'mean'),
        Avg_SN_Psub=('snapp_after_fare', 'mean'),
        Avg_TP_Psub=('tapsi_after_fare', 'mean'),
        Avg_SN_Eco=('snapp_normal_fare', 'mean'),
        Avg_TP_Eco=('tapsi_normal_fare', 'mean')
    ).reset_index()

    # Percentages stored as FRACTIONS (0–1)
    agg['SN_pairing %'] = agg['SN_paired'] / agg['total_rides']
    agg['TP_pairing %'] = agg['TP_paired'] / agg['total_rides']
    agg['SN_accepting %'] = agg['SN_accepted'] / agg['total_rides']
    agg['TP_accepting %'] = agg['TP_accepted'] / agg['total_rides']

    return agg


# ============================
# Add higher-level TOTAL rows
# ============================
def add_total_rows(agg_df, dim_cols, total_level_cols):
    result_rows = []

    grouped = agg_df.groupby(total_level_cols, dropna=False)

    for _, group_df in grouped:
        result_rows.extend(group_df.to_dict(orient='records'))

        total = group_df.drop(columns=dim_cols).sum(
            numeric_only=True).to_dict()

        for col in dim_cols:
            if col in total_level_cols:
                total[col] = group_df[col].iloc[0]
            else:
                total[col] = 'ALL'

        if total['total_rides'] > 0:
            total['SN_pairing %'] = total['SN_paired'] / total['total_rides']
            total['TP_pairing %'] = total['TP_paired'] / total['total_rides']
            total['SN_accepting %'] = total['SN_accepted'] / \
                total['total_rides']
            total['TP_accepting %'] = total['TP_accepted'] / \
                total['total_rides']
        else:
            total['SN_pairing %'] = 0
            total['TP_pairing %'] = 0
            total['SN_accepting %'] = 0
            total['TP_accepting %'] = 0

        result_rows.append(total)

    return pd.DataFrame(result_rows)


# ============================
# Measure formatting
# ============================
def format_measures(df):
    int_cols = [
        'total_rides', 'SN_paired', 'TP_paired',
        'SN_accepted', 'TP_accepted'
    ]

    avg_cols = [
        'Avg_SN_carp', 'Avg_TP_carp',
        'Avg_SN_Psub', 'Avg_TP_Psub',
        'Avg_SN_Eco', 'Avg_TP_Eco'
    ]

    pct_cols = [
        'SN_pairing %', 'TP_pairing %',
        'SN_accepting %', 'TP_accepting %'
    ]

    # Integers
    for col in int_cols:
        if col in df.columns:
            df[col] = df[col].round(0).astype('Int64')

    # Averages rounded
    for col in avg_cols:
        if col in df.columns:
            df[col] = df[col].round(1)

    # Percentages kept as FRACTIONS, rounded to 3 decimals → Excel shows 0.851 → 85.1%
    for col in pct_cols:
        if col in df.columns:
            df[col] = df[col].round(3)

    return df


# ============================
# Pipeline builder
# ============================
def build_table(df, dim_cols, total_level_cols):
    agg = build_aggregation(df, dim_cols)
    final = add_total_rows(agg, dim_cols, total_level_cols)
    final = format_measures(final)
    return final


# ============================
# Main execution
# ============================
def main():
    df, routes_df = load_data(CSV_PATH, EXCEL_PATH)

    df = ensure_distinct_rides(df)
    df = add_time_features(df)
    df = prepare_routes_lookup(df, routes_df)

    success_cols = ['snapp_paired', 'tapsi_paired',
                    'snapp_accepted', 'tapsi_accepted']
    df = convert_yes_no(df, success_cols)

    table_from = build_table(
        df,
        dim_cols=['week_number', 'city', 'from_coded'],
        total_level_cols=['week_number', 'city']
    )

    table_time = build_table(
        df,
        dim_cols=['week_number', 'city', 'time_bucket'],
        total_level_cols=['week_number', 'city']
    )

    table_distance = build_table(
        df,
        dim_cols=['week_number', 'city', 'distance-bucket'],
        total_level_cols=['week_number', 'city']
    )

    table_from.to_csv(OUTPUT_FROM, index=False, encoding="utf-8-sig")
    table_time.to_csv(OUTPUT_TIME, index=False, encoding="utf-8-sig")
    table_distance.to_csv(OUTPUT_DISTANCE, index=False, encoding="utf-8-sig")

    print("✅ Aggregation complete — percentages ready for Excel/BI formatting.")


if __name__ == "__main__":
    main()
