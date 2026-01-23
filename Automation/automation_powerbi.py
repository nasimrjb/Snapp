import pandas as pd
import numpy as np

# ============================
# Paths
# ============================
CSV_PATH = r"D:\Work\Automation Project\DataSources\carpooling_export.csv"
EXCEL_PATH = r"D:\Work\Automation Project\DataSources\AllAvailableRoutes.xlsx"

OUTPUT_FROM = r"D:\Work\Automation Project\My Exploration\weekly_city_from_coded.csv"
OUTPUT_TIME = r"D:\Work\Automation Project\My Exploration\weekly_city_timebucket.csv"
OUTPUT_DISTANCE = r"D:\Work\Automation Project\My Exploration\weekly_city_distancebucket.csv"


# ============================
# Core ETL functions
# ============================

def load_data(csv_path, excel_path):
    """
    Reads base ride data and routes lookup table.
    Keeps encoding Excel-friendly for Persian / UTF-8 text.
    """
    return (
        pd.read_csv(csv_path, encoding="utf-8-sig"),
        pd.read_excel(excel_path)
    )


def prepare_base_df(df):
    """
    - Removes duplicates
    - Creates a unique ride_id
    - Converts Yes/No flags to boolean
    """
    success_cols = ['snapp_paired', 'tapsi_paired',
                    'snapp_accepted', 'tapsi_accepted']

    return (
        df.drop_duplicates()
          .reset_index(drop=True)
          .assign(
              ride_id=lambda x: np.arange(len(x)),  # unique ID per row
              **{c: lambda x, col=c: x[col].eq('Yes') for c in success_cols}
        )
    )


def add_time_features(df):
    """
    - Extracts time bucket from travel_time
    - Computes ISO week number with custom weekend shift
    """
    # Parse datetime fields safely
    df = df.assign(
        travel_time=pd.to_datetime(df['travel_time'], errors='coerce').dt.time,
        travel_date=pd.to_datetime(df['travel_date'], errors='coerce')
    )

    # Define time bins and labels
    bins = [
        pd.to_datetime("06:00").time(),
        pd.to_datetime("09:00").time(),
        pd.to_datetime("15:00").time(),
        pd.to_datetime("18:00").time(),
        pd.to_datetime("21:00").time()
    ]
    labels = ["06_09", "09_15", "15_18", "18_21"]

    # Assign time buckets using vectorized logic
    df['time_bucket'] = pd.cut(
        pd.to_datetime(df['travel_time'].astype(str), errors='coerce').dt.hour,
        bins=[6, 9, 15, 18, 21],
        labels=labels,
        right=False
    )

    # Compute week number and adjust if weekend
    df['week_number'] = (
        df['travel_date'].dt.isocalendar().week.astype(int) +
        (df['travel_date'].dt.weekday >= 5).astype(int)
    )

    return df


def merge_routes(df, routes_df):
    """
    - Renames route columns into analytics-friendly format
    - Cleans join keys
    - Left joins route metadata to main dataframe
    """
    routes_df = (
        routes_df.rename(columns={
            'Origin_Add': 'from',
            'Destination_Add': 'to',
            'Distance': 'distance-bucket',
            'Or': 'from_coded',
            'DstDistID': 'to_coded'
        })
        .assign(
            **{c: lambda x, col=c: x[col].astype(str).str.strip() for c in ['from', 'to']}
        )
    )

    df = df.assign(
        **{c: lambda x, col=c: x[col].astype(str).str.strip() for c in ['from', 'to']}
    )

    return df.merge(
        routes_df[['from', 'to', 'distance-bucket', 'from_coded', 'to_coded']],
        on=['from', 'to'],
        how='left'
    )


# ============================
# Aggregation logic
# ============================

AGG_DICT = {
    'total_rides': ('ride_id', 'nunique'),
    'SN_paired': ('snapp_paired', 'sum'),
    'TP_paired': ('tapsi_paired', 'sum'),
    'SN_accepted': ('snapp_accepted', 'sum'),
    'TP_accepted': ('tapsi_accepted', 'sum'),
    'Avg_SN_carp': ('snapp_before_fare', 'mean'),
    'Avg_TP_carp': ('tapsi_before_fare', 'mean'),
    'Avg_SN_Psub': ('snapp_after_fare', 'mean'),
    'Avg_TP_Psub': ('tapsi_after_fare', 'mean'),
    'Avg_SN_Eco': ('snapp_normal_fare', 'mean'),
    'Avg_TP_Eco': ('tapsi_normal_fare', 'mean')
}


def aggregate_metrics(df, dims):
    """
    Groups by selected dimensions and computes all metrics in one shot.
    """
    agg = (
        df.groupby(dims, dropna=False)
        .agg(**AGG_DICT)
        .reset_index()
    )

    # Add ratios as FRACTIONS (Excel formats later)
    agg = agg.assign(
        **{
            'SN_pairing %': agg['SN_paired'] / agg['total_rides'],
            'TP_pairing %': agg['TP_paired'] / agg['total_rides'],
            'SN_accepting %': agg['SN_accepted'] / agg['total_rides'],
            'TP_accepting %': agg['TP_accepted'] / agg['total_rides']
        }
    )

    return agg


def add_totals(agg_df, dims, total_levels):
    """
    Adds 'ALL' rows for higher aggregation levels.
    This keeps your BI/Excel slicing clean.
    """
    rows = []

    for _, g in agg_df.groupby(total_levels, dropna=False):
        rows.extend(g.to_dict('records'))

        total = g.drop(columns=dims).sum(numeric_only=True)

        # Restore dimension columns
        for d in dims:
            total[d] = g[d].iloc[0] if d in total_levels else 'ALL'

        # Recompute percentages safely
        tr = total['total_rides']
        total['SN_pairing %'] = total['SN_paired'] / tr if tr else 0
        total['TP_pairing %'] = total['TP_paired'] / tr if tr else 0
        total['SN_accepting %'] = total['SN_accepted'] / tr if tr else 0
        total['TP_accepting %'] = total['TP_accepted'] / tr if tr else 0

        rows.append(total)

    return pd.DataFrame(rows)


def format_output(df):
    """
    Applies BI-friendly formatting:
    - Integers for counts
    - Rounded averages
    - Percentages kept as fractions
    """
    int_cols = ['total_rides', 'SN_paired',
                'TP_paired', 'SN_accepted', 'TP_accepted']
    avg_cols = [c for c in df.columns if c.startswith('Avg_')]
    pct_cols = [c for c in df.columns if '%' in c]

    df[int_cols] = df[int_cols].round(0).astype('Int64')
    df[avg_cols] = df[avg_cols].round(1)
    df[pct_cols] = df[pct_cols].round(3)

    return df


def build_table(df, dims, total_levels):
    """
    Full aggregation pipeline:
    raw → aggregated → totals → formatted
    """
    return (
        df.pipe(aggregate_metrics, dims)
          .pipe(add_totals, dims, total_levels)
          .pipe(format_output)
    )


# ============================
# Main execution
# ============================

def main():
    df, routes_df = load_data(CSV_PATH, EXCEL_PATH)

    df = (
        df.pipe(prepare_base_df)
          .pipe(add_time_features)
          .pipe(merge_routes, routes_df)
    )

    table_from = build_table(df,
                             dims=['week_number', 'city', 'from_coded'],
                             total_levels=['week_number', 'city']
                             )

    table_time = build_table(df,
                             dims=['week_number', 'city', 'time_bucket'],
                             total_levels=['week_number', 'city']
                             )

    table_distance = build_table(df,
                                 dims=['week_number', 'city',
                                       'distance-bucket'],
                                 total_levels=['week_number', 'city']
                                 )

    table_from.to_csv(OUTPUT_FROM, index=False, encoding="utf-8-sig")
    table_time.to_csv(OUTPUT_TIME, index=False, encoding="utf-8-sig")
    table_distance.to_csv(OUTPUT_DISTANCE, index=False, encoding="utf-8-sig")

    print("✅ Aggregation complete — Excel-ready outputs created.")


if __name__ == "__main__":
    main()
