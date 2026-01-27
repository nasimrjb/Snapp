import pandas as pd
import numpy as np

# =========================
# Global Pandas Safety
# =========================
pd.set_option('future.no_silent_downcasting', True)

# =========================
# Paths
# =========================

CSV1_PATH = r"D:\Work\Automation Project\Outputs\weekly_city_from_coded.csv"
CSV2_PATH = r"D:\Work\Automation Project\DataSources\real_data_11_10_to_01_23.csv"
OUTPUT_PATH = r"D:\Work\Automation Project\Outputs\weekly_city_aggregated_models.csv"

# =========================
# Utility Functions
# =========================


def safe_div(n, d):
    n = np.asarray(n, dtype="float64")
    d = np.asarray(d, dtype="float64")
    out = np.full_like(n, np.nan, dtype="float64")
    mask = d != 0
    out[mask] = n[mask] / d[mask]
    return out


def weighted_avg(df, value_col, weight_col):
    return safe_div((df[value_col] * df[weight_col]).sum(), df[weight_col].sum())


def wow(series):
    series = pd.to_numeric(series, errors="coerce")
    return series.pct_change(fill_method=None)

# =========================
# Load Data
# =========================


df1 = pd.read_csv(CSV1_PATH)
df2 = pd.read_csv(CSV2_PATH)

# =========================
# Normalize City Mapping (df2)
# =========================

city_map = {
    1.0: "Tehran",
    2.0: "Karaj",
    3.0: "Isfahan",
    5.0: "Mashhad"
}

df2["city"] = df2["org_city_id"].map(city_map)

# =========================
# Enforce Numeric Columns
# =========================

num_cols_df1 = [
    "SN_pairing %", "TP_pairing %", "SN_acceptance %", "TP_acceptance %",
    "req_share %", "pairing_ratio", "acceptance_ratio", "pairing_3", "acceptance_3",
    "SN_pair_count", "TP_pair_count"
]

for c in num_cols_df1:
    df1[c] = pd.to_numeric(df1[c], errors="coerce")

num_cols_df2 = ["reqs", "accepts", "pairs"]
for c in num_cols_df2:
    df2[c] = pd.to_numeric(df2[c], errors="coerce")

# =========================
# Aggregate FIRST CSV → week + city
# =========================

agg1 = (
    df1
    .groupby(["week_number", "city"], group_keys=False, observed=True)
    .apply(lambda g: pd.Series({
        "total_SN_pairing_pct": weighted_avg(g, "SN_pairing %", "req_share %"),
        "total_TP_pairing_pct": weighted_avg(g, "TP_pairing %", "req_share %"),
        "total_SN_acceptance_pct": safe_div((g["SN_acceptance %"] * g["SN_pair_count"]).sum(), g["SN_pair_count"].sum()),
        "total_TP_acceptance_pct": safe_div((g["TP_acceptance %"] * g["SN_pair_count"]).sum(), g["TP_pair_count"].sum()),
        "total_pairing_ratio": weighted_avg(g, "pairing_ratio", "req_share %"),
        "total_acceptance_ratio": weighted_avg(g, "acceptance_ratio", "req_share %"),
        "pairing_model_2": weighted_avg(g, "pairing_ratio", "req_share %"),
        "acceptance_model_2": weighted_avg(g, "acceptance_ratio", "req_share %"),
        "pairing_model_3": weighted_avg(g, "pairing_3", "req_share %"),
        "acceptance_model_3": weighted_avg(g, "acceptance_3", "req_share %"),
        "avg_pairing_ratio": g["pairing_ratio"].mean(),
        "avg_acceptance_ratio": g["acceptance_ratio"].mean()
    }), include_groups=False)  # type: ignore
    .reset_index()
)

# =========================
# Aggregate SECOND CSV → week + city
# =========================

agg2 = (
    df2
    .groupby(["Week_Num", "city"], as_index=False, observed=True)
    .agg({
        "reqs": "sum",
        "pairs": "sum",
        "accepts": "sum"
    })
    .rename(columns={"Week_Num": "week_number"})
)

agg2["total_pairing_pct"] = safe_div(agg2["pairs"], agg2["reqs"])
agg2["total_acceptance_pct"] = safe_div(agg2["accepts"], agg2["pairs"])

# =========================
# Merge Aggregates
# =========================

df = pd.merge(agg1, agg2, on=["week_number", "city"], how="left")

# =========================
# hp / ha Calculations
# =========================

df["hp1"] = safe_div(df["total_SN_pairing_pct"], df["total_pairing_pct"])
df["ha1"] = safe_div(df["total_SN_acceptance_pct"], df["total_acceptance_pct"])

# =========================
# Rolling 2-week hp2 / ha2
# =========================

df = df.sort_values(["city", "week_number"])

df["hp2"] = df.groupby("city")["hp1"].transform(lambda x: x.rolling(2).mean())
df["ha2"] = df.groupby("city")["ha1"].transform(lambda x: x.rolling(2).mean())

# =========================
# Model Calculations
# =========================

df["pairing_model_1"] = safe_div(df["total_TP_pairing_pct"], df["hp1"])
df["acceptance_model_1"] = safe_div(df["total_TP_acceptance_pct"], df["ha1"])

df["pairing_model_4"] = safe_div(
    df["total_TP_pairing_pct"], df["total_pairing_ratio"])
df["acceptance_model_4"] = safe_div(
    df["total_TP_acceptance_pct"], df["total_acceptance_ratio"])

df["pairing_model_5"] = safe_div(
    df["total_TP_pairing_pct"], df["avg_pairing_ratio"])
df["acceptance_model_6"] = safe_div(
    df["total_TP_acceptance_pct"], df["avg_acceptance_ratio"])

# =========================
# WoW for all model metrics
# =========================

model_cols = [c for c in df.columns if "model" in c]

for col in model_cols:
    df[col + "_WoW"] = df.groupby("city")[col].transform(wow)

# =========================
# Export
# =========================
# =========================
# Enforce numeric dtypes before export
# =========================

for col in df.columns:
    if col not in ["week_number", "city"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")

print("✅ Pipeline completed successfully.")
print("📁 Output saved to:", OUTPUT_PATH)
