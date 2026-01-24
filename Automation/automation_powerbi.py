import pandas as pd
import numpy as np

# ============================
# Paths
# ============================
CSV_PATH = r"D:\Work\Automation Project\DataSources\carpooling_export_11_10_25_to_01_21_26.csv"
EXCEL_PATH = r"D:\Work\Automation Project\DataSources\AllAvailableRoutes.xlsx"

OUTPUT_FROM = r"D:\Work\Automation Project\My Exploration\Outputs\weekly_city_from_coded.csv"
OUTPUT_TIME = r"D:\Work\Automation Project\My Exploration\Outputs\weekly_city_timebucket.csv"
OUTPUT_DISTANCE = r"D:\Work\Automation Project\My Exploration\Outputs\weekly_city_distancebucket.csv"


# ============================
# Load & prepare data
# ============================

def load_data(csv_path, excel_path):
    return (
        pd.read_csv(csv_path, encoding="utf-8-sig"),
        pd.read_excel(excel_path)
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


# ============================
# Time features
# ============================

def add_time_features(df):
    df['travel_time'] = pd.to_datetime(
        df['travel_time'],
        format="%H:%M",
        errors='coerce'
    ).dt.time
    df['travel_date'] = pd.to_datetime(df['travel_date'], errors='coerce')

    def bucket(t):
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

    df['time_bucket'] = df['travel_time'].apply(bucket)

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
        'Distance': 'distance-bucket',
        'Or': 'from_coded',
        'DstDistID': 'to_coded'
    })

    for col in ['from', 'to']:
        df[col] = df[col].astype(str).str.strip()
        routes_df[col] = routes_df[col].astype(str).str.strip()

    return df.merge(
        routes_df[['from', 'to', 'distance-bucket', 'from_coded', 'to_coded']],
        how='left',
        on=['from', 'to']
    )


# ============================
# Aggregation with ACCEPTED-based averages
# ============================

def aggregate_metrics(df, dims):
    df = df.copy()

    # Mask fares ONLY for carpool-based products
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

            # Eco → no filtering at all
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
        0
    )

    agg['TP_acceptance %'] = np.where(
        agg['TP_paired'] > 0,
        agg['TP_accepted'] / agg['TP_paired'],
        0
    )

    return agg


# ============================
# Add TOTAL rows
# ============================

def add_totals(agg_df, dims, total_levels):
    rows = []

    for _, g in agg_df.groupby(total_levels, dropna=False):
        rows.extend(g.to_dict('records'))

        total = g.drop(columns=dims).sum(numeric_only=True)

        for d in dims:
            total[d] = g[d].iloc[0] if d in total_levels else 'ALL'

        tr = total['total_rides']
        total['SN_pairing %'] = total['SN_paired'] / tr if tr else 0
        total['TP_pairing %'] = total['TP_paired'] / tr if tr else 0
        total['SN_acceptance %'] = (
            total['SN_accepted'] / total['SN_paired']
            if total['SN_paired'] > 0 else 0
        )

        total['TP_acceptance %'] = (
            total['TP_accepted'] / total['TP_paired']
            if total['TP_paired'] > 0 else 0
        )
        # Carpool-based averages → accepted only
        total['Avg_SN_carp'] = g.loc[g['SN_accepted'] > 0, 'Avg_SN_carp'].mean()
        total['Avg_SN_Psub'] = g.loc[g['SN_accepted'] > 0, 'Avg_SN_Psub'].mean()

        total['Avg_TP_carp'] = g.loc[g['TP_accepted'] > 0, 'Avg_TP_carp'].mean()
        total['Avg_TP_Psub'] = g.loc[g['TP_accepted'] > 0, 'Avg_TP_Psub'].mean()

        # Eco → simple mean across groups
        total['Avg_SN_Eco'] = g['Avg_SN_Eco'].mean()
        total['Avg_TP_Eco'] = g['Avg_TP_Eco'].mean()

        rows.append(total)

    return pd.DataFrame(rows)


# ============================
# Formatting for BI
# ============================

def format_output(df):
    int_cols = ['total_rides', 'SN_paired',
                'TP_paired', 'SN_accepted', 'TP_accepted']
    avg_cols = [c for c in df.columns if c.startswith('Avg_')]
    pct_cols = [c for c in df.columns if '%' in c]

    df[int_cols] = df[int_cols].round(0).astype('Int64')
    df[avg_cols] = df[avg_cols].round(1)
    df[pct_cols] = df[pct_cols].round(3)

    return df


# ============================
# Build final table
# ============================

def build_table(df, dims, total_levels):
    return (
        df.pipe(aggregate_metrics, dims)
          .pipe(add_totals, dims, total_levels)
          .pipe(format_output)
    )


# ============================
# Main
# ============================

def main():
    df, routes_df = load_data(CSV_PATH, EXCEL_PATH)

    df = (
        df.pipe(prepare_base_df)
          .pipe(add_time_features)
          .pipe(merge_routes, routes_df)
    )

    table_from = build_table(
        df,
        dims=['week_number', 'city', 'from_coded'],
        total_levels=['week_number', 'city']
    )

    table_time = build_table(
        df,
        dims=['week_number', 'city', 'time_bucket'],
        total_levels=['week_number', 'city']
    )

    table_distance = build_table(
        df,
        dims=['week_number', 'city', 'distance-bucket'],
        total_levels=['week_number', 'city']
    )

    table_from.to_csv(OUTPUT_FROM, index=False, encoding="utf-8-sig")
    table_time.to_csv(OUTPUT_TIME, index=False, encoding="utf-8-sig")
    table_distance.to_csv(OUTPUT_DISTANCE, index=False, encoding="utf-8-sig")

    print("✅ Aggregation complete — averages now based on ACCEPTED rides only.")


if __name__ == "__main__":
    main()
