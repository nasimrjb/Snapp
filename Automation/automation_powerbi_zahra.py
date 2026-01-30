import pandas as pd
import numpy as np

# ============================
# Paths
# ============================

CSV_PATH = r"D:\OneDrive\Work\Automation Project\DataSources\carpooling_export_zahra.csv"
EXCEL_PATH = r"D:\OneDrive\Work\Automation Project\DataSources\AllAvailableRoutes.xlsx"
REAL_DATA_PATH = r"D:\OneDrive\Work\Automation Project\DataSources\real_data_11_10_to_01_23.csv"

OUTPUT_FROM = r"D:\OneDrive\Work\Automation Project\Outputs\weekly_city_from_coded_zahra.csv"
OUTPUT_TIME = r"D:\OneDrive\Work\Automation Project\Outputs\weekly_city_time_bucket_zahra.csv"
OUTPUT_DISTANCE = r"D:\OneDrive\Work\Automation Project\Outputs\weekly_city_distance_bucket_zahra.csv"

MIN_PAIRED = 9
ADJ1 = 0.75
ADJ2 = 0.80

# ============================
# Load Data
# ============================


def load_data(csv_path, excel_path, real_data_path):
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    routes_df = pd.read_excel(excel_path)
    real_df = pd.read_csv(real_data_path, encoding="utf-8-sig")
    return df, routes_df, real_df


# ============================
# Base Prep
# ============================

def prepare_base_df(df):
    df = df.drop_duplicates().reset_index(drop=True)
    df["ride_id"] = np.arange(len(df))

    # Boolean flags
    flag_cols = ["snapp_paired", "tapsi_paired",
                 "snapp_accepted", "tapsi_accepted"]

    for col in flag_cols:
        df[col] = df[col].eq("Yes")

    # Numeric fares
    fare_cols = [
        "snapp_before_fare", "snapp_after_fare",
        "tapsi_before_fare", "tapsi_after_fare",
        "snapp_normal_fare", "tapsi_normal_fare"
    ]

    for col in fare_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


# ============================
# Time Buckets
# ============================

def add_time_features(df):
    df["travel_date"] = pd.to_datetime(df["travel_date"], errors="coerce")
    df["travel_time_dt"] = pd.to_datetime(
        df["travel_time"], format="%H:%M:%S", errors="coerce"
    )

    def bucket(dt):
        if pd.isna(dt):
            return np.nan
        h = dt.hour
        if h < 9:
            return "06_09"
        elif h < 15:
            return "09_15"
        elif h < 18:
            return "15_18"
        elif h < 21:
            return "18_21"
        return np.nan

    df["time_bucket"] = df["travel_time_dt"].apply(bucket)

    # Week number logic
    df["week_number"] = df["travel_date"].dt.isocalendar().week.astype(int)
    df["week_number"] += (df["travel_date"].dt.weekday >= 5).astype(int)

    return df


# ============================
# Merge Routes
# ============================

def merge_routes(df, routes_df):

    routes_df = routes_df.rename(columns={
        "Origin_Add": "from",
        "Destination_Add": "to",
        "Distance": "distance_bucket",
        "Or": "from_coded",
        "DstDistID": "to_coded"
    })

    for col in ["from", "to"]:
        df[col] = df[col].astype(str).str.strip()
        routes_df[col] = routes_df[col].astype(str).str.strip()

    routes_df["from_coded"] = pd.to_numeric(
        routes_df["from_coded"], errors="coerce").astype("Int64")

    return df.merge(
        routes_df[["from", "to", "distance_bucket", "from_coded", "to_coded"]],
        how="left",
        on=["from", "to"]
    )


# ============================
# Real Data Prep (ONLY for from_coded)
# ============================

def prepare_real_data(df):

    city_mapping = {
        1.0: "Tehran",
        2.0: "Karaj",
        3.0: "Isfahan",
        5.0: "Mashhad"
    }

    df = df.copy()

    df["city"] = df["org_city_id"].map(city_mapping)

    df = df.rename(columns={
        "Week_Num": "week_number",
        "org_dist_id": "from_coded"
    })

    df["from_coded"] = pd.to_numeric(
        df["from_coded"], errors="coerce").astype("Int64")

    for col in ["reqs", "pairs", "accepts", "NMV", "ride"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    return df


# ============================
# Main Aggregation (Export Data)
# ============================

def aggregate_metrics(df, dims):

    df = df.copy()

    # Accepted-only fares
    df["snapp_before_fare_acc"] = df["snapp_before_fare"].where(
        df["snapp_accepted"])
    df["snapp_after_fare_acc"] = df["snapp_after_fare"].where(
        df["snapp_accepted"])

    df["tapsi_before_fare_acc"] = df["tapsi_before_fare"].where(
        df["tapsi_accepted"])
    df["tapsi_after_fare_acc"] = df["tapsi_after_fare"].where(
        df["tapsi_accepted"])

    agg = (
        df.groupby(dims, dropna=False)
        .agg(
            total_rides=("ride_id", "nunique"),

            SN_paired=("snapp_paired", "sum"),
            TP_paired=("tapsi_paired", "sum"),

            SN_accepted=("snapp_accepted", "sum"),
            TP_accepted=("tapsi_accepted", "sum"),

            Avg_SN_carp=("snapp_before_fare_acc", "mean"),
            Avg_SN_Psub=("snapp_after_fare_acc", "mean"),

            Avg_TP_carp=("tapsi_before_fare_acc", "mean"),
            Avg_TP_Psub=("tapsi_after_fare_acc", "mean"),

            Avg_SN_Eco=("snapp_normal_fare", "mean"),
            Avg_TP_Eco=("tapsi_normal_fare", "mean"),
        )
        .reset_index()
    )

    # Rates
    agg["SN_pairing %"] = agg["SN_paired"] / agg["total_rides"]
    agg["TP_pairing %"] = agg["TP_paired"] / agg["total_rides"]

    agg["SN_acceptance %"] = np.where(
        agg["SN_paired"] > 0,
        agg["SN_accepted"] / agg["SN_paired"],
        np.nan
    )

    agg["TP_acceptance %"] = np.where(
        agg["TP_paired"] > 0,
        agg["TP_accepted"] / agg["TP_paired"],
        np.nan
    )

    return agg


# ============================
# Real Data Aggregation (ONLY for from_coded)
# ============================

def aggregate_real_data(real_df):

    dims = ["week_number", "city", "from_coded"]

    return (
        real_df.groupby(dims, dropna=False)
        .agg(
            req_count=("reqs", "sum"),
            SN_pair_count_raw=("pairs", "sum"),
            SN_accept_count_raw=("accepts", "sum"),
            NMV_sum=("NMV", "sum"),
            ride_sum=("ride", "sum"),
        )
        .reset_index()
    )


# ============================
# Merge Real Into From Table
# ============================

def merge_real_into_from(main_agg, real_agg):

    dims = ["week_number", "city", "from_coded"]

    merged = main_agg.merge(real_agg, on=dims, how="left")

    for col in ["req_count", "SN_pair_count_raw", "SN_accept_count_raw", "NMV_sum", "ride_sum"]:
        merged[col] = merged[col].fillna(0)

    merged["SN_pair_count"] = np.where(
        merged["SN_paired"] > MIN_PAIRED,
        merged["SN_pair_count_raw"],
        np.nan
    )

    merged["SN_accept_count"] = np.where(
        merged["SN_paired"] > MIN_PAIRED,
        merged["SN_accept_count_raw"],
        np.nan
    )

    return merged


# ============================
# AOV Metrics (From Table Only)
# ============================

def add_aov_metrics(df):

    df["AOV_real_SN"] = np.where(
        (df["SN_accepted"] > MIN_PAIRED) & (df["ride_sum"] > 0),
        df["NMV_sum"] / df["ride_sum"],
        np.nan
    )

    df["AOV_T_R"] = np.where(
        df["AOV_real_SN"] > 0,
        (ADJ1 * df["Avg_SN_carp"]) / (ADJ2 * df["AOV_real_SN"]),
        np.nan
    )

    df["AOV_est"] = np.where(
        df["AOV_T_R"] > 0,
        df["Avg_TP_carp"] / df["AOV_T_R"],
        np.nan
    )

    return df


# ============================
# Ratio Columns
# ============================

def add_ratio_columns(df):

    df["pairing_ratio"] = np.where(
        df["SN_pairing %"] > 0,
        df["TP_pairing %"] / df["SN_pairing %"],
        np.nan
    )

    df["acceptance_ratio"] = np.where(
        df["SN_acceptance %"] > 0,
        df["TP_acceptance %"] / df["SN_acceptance %"],
        np.nan
    )

    return df


# ============================
# Main
# ============================

def main():

    df, routes_df, real_df = load_data(CSV_PATH, EXCEL_PATH, REAL_DATA_PATH)

    # Export prep
    df = (
        df.pipe(prepare_base_df)
          .pipe(add_time_features)
          .pipe(merge_routes, routes_df)
    )

    # Real prep
    real_df = prepare_real_data(real_df)

    # =======================
    # 1) FROM TABLE (with real merge)
    # =======================

    table_from = aggregate_metrics(df, ["week_number", "city", "from_coded"])
    real_agg = aggregate_real_data(real_df)

    table_from = merge_real_into_from(table_from, real_agg)
    table_from = add_aov_metrics(table_from)
    table_from = add_ratio_columns(table_from)

    table_from.to_csv(OUTPUT_FROM, index=False, encoding="utf-8-sig")

    # =======================
    # 2) TIME TABLE (NO real merge)
    # =======================

    table_time = aggregate_metrics(df, ["week_number", "city", "time_bucket"])
    table_time = add_ratio_columns(table_time)

    table_time.to_csv(OUTPUT_TIME, index=False, encoding="utf-8-sig")

    # =======================
    # 3) DISTANCE TABLE (NO real merge)
    # =======================

    table_distance = aggregate_metrics(
        df, ["week_number", "city", "distance_bucket"])
    table_distance = add_ratio_columns(table_distance)

    table_distance.to_csv(OUTPUT_DISTANCE, index=False, encoding="utf-8-sig")

    print("✅ All 3 tables generated successfully (real merge only applied to from_coded).")


if __name__ == "__main__":
    main()
