import pandas as pd
import numpy as np

# =========================
# Paths
# =========================
CSV1_PATH = r"D:\OneDrive\Work\Automation Project\Outputs\weekly_city_from_coded_zahra.csv"
CSV2_PATH = r"D:\OneDrive\Work\Automation Project\DataSources\real_data_11_10_to_01_23.csv"

OUTPUT_PATH = r"D:\OneDrive\Work\Automation Project\Outputs\weekly_city_aggregated_models_zahra.xlsx"

# =========================
# Utility Functions
# =========================


def safe_div(n, d):
    """
    Safe division that works for scalars or arrays.
    Returns NaN if denominator is 0 or missing.
    """
    n = np.asarray(n, dtype="float64")
    d = np.asarray(d, dtype="float64")

    if n.ndim == 0 and d.ndim == 0:
        if np.isnan(n) or np.isnan(d) or d == 0:
            return np.nan
        return n / d

    out = np.full_like(n, np.nan, dtype="float64")
    mask = (d != 0) & (~np.isnan(d))
    out[mask] = n[mask] / d[mask]
    return out


def weighted_avg(df, value_col, weight_col):
    num = (df[value_col] * df[weight_col]).sum()
    den = df[weight_col].sum()
    return float(safe_div(num, den))


def wow(series):
    series = pd.to_numeric(series, errors="coerce")
    return series.pct_change(fill_method=None)


# =========================
# Load Data
# =========================
df1 = pd.read_csv(CSV1_PATH)
df2 = pd.read_csv(CSV2_PATH)

# =========================
# Normalize City Names
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
    "SN_pairing %", "TP_pairing %",
    "SN_acceptance %", "TP_acceptance %",
    "req_share %", "pairing_ratio", "acceptance_ratio",
    "pairing_3", "acceptance_3",
    "SN_pair_count", "TP_pair_count"
]
for c in num_cols_df1:
    df1[c] = pd.to_numeric(df1[c], errors="coerce")

num_cols_df2 = ["reqs", "pairs", "accepts"]
for c in num_cols_df2:
    df2[c] = pd.to_numeric(df2[c], errors="coerce")

# =========================
# Aggregate FIRST CSV
# =========================
agg1 = (
    df1
    .groupby(["week_number", "city"], observed=True)
    .apply(
        lambda g: pd.Series({
            "total_SN_pairing_pct": weighted_avg(g, "SN_pairing %", "req_share %"),
            "total_TP_pairing_pct": weighted_avg(g, "TP_pairing %", "req_share %"),
            "total_SN_acceptance_pct": safe_div(
                (g["SN_acceptance %"] * g["SN_pair_count"]).sum(),
                g["SN_pair_count"].sum()
            ),
            "total_TP_acceptance_pct": safe_div(
                (g["TP_acceptance %"] * g["TP_pair_count"]).sum(),
                g["TP_pair_count"].sum()
            ),
            "total_pairing_ratio": weighted_avg(g, "pairing_ratio", "req_share %"),
            "total_acceptance_ratio": weighted_avg(g, "acceptance_ratio", "req_share %"),
            "pairing_model_3": weighted_avg(g, "pairing_3", "req_share %"),
            "acceptance_model_3": weighted_avg(g, "acceptance_3", "req_share %"),
            "avg_pairing_ratio": g["pairing_ratio"].mean(),
            "avg_acceptance_ratio": g["acceptance_ratio"].mean()
        }),
        include_groups=False  # type: ignore
    )  # type: ignore
    .reset_index()
)

# =========================
# Aggregate SECOND CSV
# =========================
agg2 = (
    df2
    .groupby(["Week_Num", "city"], as_index=False)
    .agg({"reqs": "sum", "pairs": "sum", "accepts": "sum"})
    .rename(columns={"Week_Num": "week_number"})
)
agg2["total_pairing_pct"] = safe_div(agg2["pairs"], agg2["reqs"])
agg2["total_acceptance_pct"] = safe_div(agg2["accepts"], agg2["pairs"])

# =========================
# Merge Aggregates
# =========================
df = pd.merge(
    agg1,
    agg2[["week_number", "city", "total_pairing_pct", "total_acceptance_pct"]],
    on=["week_number", "city"],
    how="left"
)

# =========================
# hp / ha
# =========================
df["hp1"] = safe_div(df["total_SN_pairing_pct"], df["total_pairing_pct"])
df["ha1"] = safe_div(df["total_SN_acceptance_pct"], df["total_acceptance_pct"])
df = df.sort_values(["city", "week_number"])
df["hp2"] = df.groupby("city")["hp1"].transform(lambda x: x.rolling(2).mean())
df["ha2"] = df.groupby("city")["ha1"].transform(lambda x: x.rolling(2).mean())

# =========================
# Model Calculations
# =========================
df["pairing_model_1"] = safe_div(df["total_TP_pairing_pct"], df["hp1"])
df["acceptance_model_1"] = safe_div(df["total_TP_acceptance_pct"], df["ha1"])
df["pairing_model_2"] = safe_div(df["total_TP_pairing_pct"], df["hp2"])
df["acceptance_model_2"] = safe_div(df["total_TP_acceptance_pct"], df["ha2"])
df["pairing_model_4"] = safe_div(
    df["total_TP_pairing_pct"], df["total_pairing_ratio"])
df["acceptance_model_4"] = safe_div(
    df["total_TP_acceptance_pct"], df["total_acceptance_ratio"])
df["pairing_model_5"] = safe_div(
    df["total_TP_pairing_pct"], df["avg_pairing_ratio"])
df["acceptance_model_5"] = safe_div(
    df["total_TP_acceptance_pct"], df["avg_acceptance_ratio"])

# =========================
# WoW Calculations
# =========================
model_cols = [c for c in df.columns if "model" in c]
for col in model_cols:
    df[col + "_WoW"] = df.groupby("city")[col].transform(wow)

# =========================
# Round all numeric columns
# =========================
for col in df.columns:
    if col not in ["week_number", "city"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
df = df.round(4)

# =========================
# Select and order columns
# =========================
cols_order = [
    "week_number", "city",
    "total_SN_pairing_pct", "total_TP_pairing_pct",
    "total_SN_acceptance_pct", "total_TP_acceptance_pct",
    "total_pairing_ratio", "total_acceptance_ratio",
    "total_pairing_pct", "total_acceptance_pct",
    "pairing_model_1", "pairing_model_1_WoW",
    "acceptance_model_1", "acceptance_model_1_WoW",
    "pairing_model_2", "pairing_model_2_WoW",
    "acceptance_model_2", "acceptance_model_2_WoW",
    "pairing_model_3", "pairing_model_3_WoW",
    "acceptance_model_3", "acceptance_model_3_WoW",
    "pairing_model_4", "pairing_model_4_WoW",
    "acceptance_model_4", "acceptance_model_4_WoW",
    "pairing_model_5", "pairing_model_5_WoW",
    "acceptance_model_5", "acceptance_model_5_WoW"
]

df = df[cols_order]

# =========================
# Export to Excel
# =========================
df.to_excel(
    OUTPUT_PATH,
    index=False,
    engine="openpyxl"
)

print("✅ Pipeline completed successfully.")
print("📁 Output saved to:", OUTPUT_PATH)
