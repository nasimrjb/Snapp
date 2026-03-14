"""
Driver Survey Routine Analysis
Produces conceptually similar outputs to the weekly Excel routine report.
Reads from the 6 processed CSV files and generates analysis tables by city/week.
"""

import pandas as pd
import numpy as np
import os
import warnings
from openpyxl.formatting.rule import ColorScaleRule
from openpyxl.utils import get_column_letter
from openpyxl.styles import Border, Side, Font, Alignment, PatternFill

warnings.filterwarnings("ignore")

# ─── Config ──────────────────────────────────────────────────────────────────
BASE_DIR = r"D:\Work\Driver Survey"
PROCESSED_DIR = os.path.join(BASE_DIR, "processed")
OUTPUT_DIR = os.path.join(BASE_DIR, "routine_output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Ordered list of top cities (display order preserved in all outputs)
TOP_CITIES = [
    "Tehran(city)", "Karaj", "Isfahan", "Shiraz", "Mashhad", "Qom",
    "Tabriz", "Ahwaz", "Sari", "Rasht", "Urumieh", "Yazd",
    "Kerman", "Gorgan", "Ghazvin", "Arak", "Kermanshah", "Hamedan",
    "Ardebil", "Bojnurd", "Khorramabad", "Zanjan", "Kish",
]

MERGE_COLS = ["recordID", "city", "active_time", "cooperation_type",
              "age_group", "edu", "marr_stat", "gender", "original_job"]

# ─── Column Ordering Maps ────────────────────────────────────────────────────
INCENTIVE_AMOUNT_ORDER = [
    "< 100k", "100_200k", "100_250k", "200_400k", "250_500k",
    "400_600k", "500_750k", "600_800k", "750k_1m", "800k_1m",
    "1m_1.25m", "1.25m_1.5m", ">1.5m",
]
INCENTIVE_DURATION_ORDER = [
    "Few Hours", "1 Day", "1_6 Days", "7 Days", ">7 Days",
]
ACTIVITY_TYPE_ORDER = [
    "few hours/month", "<20hour/mo", "5_20hour/week",
    "20_40h/week", ">40h/week", "8_12hour/day", ">12h/day",
]
INACTIVITY_ORDER = [
    "Same Day", "1_3 Day Before", "3_7 Days Before",
    "8_14 Days Before", "15_30 Days_Before",
    "1_2 Month Before", "2_3 Month Before",
    "3_6Month Before", ">6 Month Before",
]

COLUMN_ORDERS = {
    "#1_Snapp_Incentive_Amt": INCENTIVE_AMOUNT_ORDER,
    "#2_Tapsi_Incentive_Amt": INCENTIVE_AMOUNT_ORDER,
    "#4_Incentive_Duration": INCENTIVE_DURATION_ORDER,
    "#15_Persona_Activity Type": ACTIVITY_TYPE_ORDER,
    "#17_Inactivity": INACTIVITY_ORDER,
}

# Columns that should NOT be treated as percentage
NON_PCT_COLS = {"n", "n_dissatisfied", "n_contacted", "respondent_count",
                "joint_count", "avg_snapp_LOC", "avg_tapsi_LOC",
                "avg_snapp_ride", "avg_tapsi_ride",
                "avg_magical_window_income", "count",
                "tapsi_carpooling_count"}

# Sheet prefixes where values are satisfaction scores (1-5), not percentages
SATISFACTION_SHEETS = {"#3_Sat_", "#CS_Sat_", "#Carfix_Sat_", "#Garage_Sat_",
                       "#NavReco_"}

# Sheets where values are absolute numbers, not percentages
ABSOLUTE_SHEETS = {"#12_Cities_Overview", "#18_CommFree", "#Demand_"}


# ─── Helpers ─────────────────────────────────────────────────────────────────
def load_data():
    print("Loading data...")
    data = {}
    files = {
        "short_main": "short_survey_main.csv",
        "short_rare": "short_survey_rare.csv",
        "wide_main": "wide_survey_main.csv",
        "wide_rare": "wide_survey_rare.csv",
        "long_main": "long_survey_main.csv",
        "long_rare": "long_survey_rare.csv",
    }
    for key, fname in files.items():
        path = os.path.join(PROCESSED_DIR, fname)
        print(f"  Loading {fname}...")
        data[key] = pd.read_csv(path, low_memory=False)
    print(f"  Done. short_main shape: {data['short_main'].shape}")
    available = [c for c in MERGE_COLS if c in data["short_main"].columns]
    data["_lookup"] = data["short_main"][available].drop_duplicates(subset="recordID")
    return data


def get_latest_week(df, min_respondents=100):
    counts = df.groupby("weeknumber").size()
    valid = counts[counts >= min_respondents]
    return valid.index.max() if len(valid) > 0 else df["weeknumber"].max()


def filter_week(df, week):
    return df[df["weeknumber"] == week].copy()


def filter_top_cities(df, city_col="city"):
    if city_col not in df.columns:
        return df
    return df[df[city_col].isin(TOP_CITIES)]


def add_city(df, lookup):
    if "city" in df.columns:
        return df
    merge_cols = [c for c in lookup.columns if c not in df.columns or c == "recordID"]
    return df.merge(lookup[merge_cols], on="recordID", how="left")


def sort_cities(df):
    """Reindex rows to match the TOP_CITIES display order."""
    if df.index.name == "City" or (hasattr(df.index, 'name') and df.index.name is None):
        # Keep cities that exist in data, in TOP_CITIES order, then any extras
        ordered = [c for c in TOP_CITIES if c in df.index]
        extras = [c for c in df.index if c not in TOP_CITIES and c != "Total"]
        has_total = "Total" in df.index
        new_order = ordered + extras + (["Total"] if has_total else [])
        return df.reindex(new_order)
    return df


def add_total_row(ct, n_total=None):
    total = ct.select_dtypes(include="number").mean().to_frame().T
    total.index = ["Total"]
    if n_total is not None and "n" in ct.columns:
        total["n"] = n_total
    result = pd.concat([ct, total]).round(4)
    return sort_cities(result)


def reorder_columns(df, sheet_name):
    order = COLUMN_ORDERS.get(sheet_name)
    if order is None:
        return df
    ordered = [c for c in order if c in df.columns]
    others = [c for c in df.columns if c not in order]
    return df[ordered + others]


def is_pct_sheet(sheet_name):
    for prefix in SATISFACTION_SHEETS:
        if sheet_name.startswith(prefix):
            return False
    for name in ABSOLUTE_SHEETS:
        if sheet_name.startswith(name):
            return False
    return True


def convert_pct_to_decimal(df, sheet_name):
    if not is_pct_sheet(sheet_name):
        for c in df.columns:
            if c.endswith("_%") or c.endswith("_pct"):
                df[c] = df[c] / 100
        return df
    for c in df.columns:
        if c in NON_PCT_COLS:
            continue
        if pd.api.types.is_numeric_dtype(df[c]):
            df[c] = df[c] / 100
    return df


def apply_conditional_formatting(ws, df, sheet_name):
    if df.empty:
        return
    n_rows = len(df)
    for col_idx, col_name in enumerate(df.columns, start=2):
        if col_name in NON_PCT_COLS:
            continue
        if not pd.api.types.is_numeric_dtype(df[col_name]):
            continue
        is_wow = "WoW" in str(col_name)
        col_letter = get_column_letter(col_idx)
        cell_range = f"{col_letter}2:{col_letter}{n_rows + 1}"

        if is_wow:
            rule = ColorScaleRule(
                start_type="min", start_color="F8696B",
                mid_type="num", mid_value=0, mid_color="FFFFFF",
                end_type="max", end_color="63BE7B",
            )
        elif any(sheet_name.startswith(p) for p in SATISFACTION_SHEETS):
            rule = ColorScaleRule(
                start_type="num", start_value=1, start_color="F8696B",
                mid_type="num", mid_value=3, mid_color="FFEB84",
                end_type="num", end_value=5, end_color="63BE7B",
            )
        else:
            rule = ColorScaleRule(
                start_type="min", start_color="FFFFFF",
                end_type="max", end_color="63BE7B",
            )
        ws.conditional_formatting.add(cell_range, rule)


def format_pct_cells(ws, df, sheet_name):
    if df.empty:
        return
    for col_idx, col_name in enumerate(df.columns, start=2):
        if col_name in NON_PCT_COLS:
            continue
        if not pd.api.types.is_numeric_dtype(df[col_name]):
            continue
        should_fmt_pct = False
        if is_pct_sheet(sheet_name) and col_name not in NON_PCT_COLS:
            should_fmt_pct = True
        elif col_name.endswith("_%") or col_name.endswith("_pct"):
            should_fmt_pct = True
        if should_fmt_pct:
            for row_idx in range(2, len(df) + 2):
                cell = ws.cell(row=row_idx, column=col_idx)
                cell.number_format = '0.0%'


def crosstab_by_city(df, col, top_cities=True):
    """Standard crosstab helper: % distribution of col by city, with n."""
    if top_cities:
        df = filter_top_cities(df)
    valid = df[df[col].notna()]
    if valid.empty:
        return pd.DataFrame()
    ct = pd.crosstab(valid["city"], valid[col], normalize="index") * 100
    ct = ct.round(1)
    ct["n"] = valid.groupby("city").size()
    ct = add_total_row(ct, len(valid))
    ct.index.name = "City"
    return ct


def mean_by_city(df, cols, top_cities=True):
    """Standard mean-by-city helper for satisfaction-type cols."""
    if top_cities:
        df = filter_top_cities(df)
    available = [c for c in cols if c in df.columns]
    if not available:
        return pd.DataFrame()
    for c in available:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    means = df.groupby("city")[available].mean().round(2)
    means["n"] = df.groupby("city")[available[0]].apply(lambda x: x.notna().sum())
    means = add_total_row(means)
    means.index.name = "City"
    return means


# ═══════════════════════════════════════════════════════════════════════════
#  ANALYSIS FUNCTIONS — from short_main / wide_main / long_main
# ═══════════════════════════════════════════════════════════════════════════

def analysis_incentive_amounts_snapp(data, week):
    print("\n[#1] Snapp Incentive Amounts Distribution...")
    df = filter_top_cities(filter_week(data["short_main"], week))
    return crosstab_by_city(df, "snapp_incentive_rial_details", top_cities=False)


def analysis_incentive_amounts_tapsi(data, week):
    print("\n[#2] Tapsi Incentive Amounts (Joint Drivers)...")
    df = filter_top_cities(filter_week(data["short_main"], week))
    # Joint drivers = active_joint == 1 (drives on both Snapp & Tapsi)
    df = df[df["active_joint"] == 1]
    return crosstab_by_city(df, "tapsi_incentive_rial_details", top_cities=False)


def analysis_satisfaction_review(data, week):
    print("\n[#3] Satisfaction Review...")
    sm = data["short_main"]
    sr = data["short_rare"]
    lookup = data["_lookup"]
    prev_week = week - 1

    snapp_sat = ["snapp_fare_satisfaction", "snapp_req_count_satisfaction",
                 "snapp_income_satisfaction", "snapp_overall_incentive_satisfaction",
                 "snapp_overall_satisfaction_snapp"]
    tapsi_sat = ["tapsi_fare_satisfaction", "tapsi_req_count_satisfaction",
                 "tapsi_income_satisfaction", "tapsi_overall_incentive_satisfaction"]
    main_sat = snapp_sat + tapsi_sat
    rare_sat = [c for c in ["snapp_overall_satisfaction", "tapsi_overall_satisfaction"]
                if c in sr.columns]

    results = {}
    for seg_name, seg_fn in [
        ("All Drivers", lambda d: d),
        ("Part-Time", lambda d: d[d["cooperation_type"] == "Part-Time"]),
        ("Full-Time", lambda d: d[d["cooperation_type"] == "Full-Time"]),
    ]:
        curr = seg_fn(filter_top_cities(filter_week(sm, week)))
        prev = seg_fn(filter_top_cities(filter_week(sm, prev_week)))
        curr_avg = curr.groupby("city")[main_sat].mean().round(2)
        prev_avg = prev.groupby("city")[main_sat].mean().round(2)

        if rare_sat:
            sr_curr = seg_fn(filter_top_cities(add_city(filter_week(sr, week), lookup)))
            sr_prev = seg_fn(filter_top_cities(add_city(filter_week(sr, prev_week), lookup)))
            curr_avg = curr_avg.join(sr_curr.groupby("city")[rare_sat].mean().round(2), how="outer")
            prev_avg = prev_avg.join(sr_prev.groupby("city")[rare_sat].mean().round(2), how="outer")

        if "snapp_incentive_message_participation" in curr.columns:
            part = curr.groupby("city")["snapp_incentive_message_participation"].apply(
                lambda x: (x.dropna() == "Yes").mean() * 100 if len(x.dropna()) > 0 else np.nan
            ).round(1).rename("incentive_participation_%")
            curr_avg = curr_avg.join(part)

        num_cols = curr_avg.select_dtypes(include="number").columns
        shared = num_cols.intersection(prev_avg.columns)
        wow = (curr_avg[shared] - prev_avg.reindex(curr_avg.index)[shared]).round(2)
        wow.columns = [f"{c}_WoW" for c in wow.columns]

        combined = pd.DataFrame(index=curr_avg.index)
        combined["n"] = curr.groupby("city").size()
        for c in curr_avg.columns:
            combined[c] = curr_avg[c]
            wc = f"{c}_WoW"
            if wc in wow.columns:
                combined[wc] = wow[wc]

        total = pd.DataFrame({"n": [len(curr)]}, index=["Total"])
        for c in curr_avg.select_dtypes(include="number").columns:
            total[c] = curr_avg[c].mean()
        combined = pd.concat([combined, total]).round(2)
        combined.index.name = "City"
        results[seg_name] = combined
    return results


def analysis_incentive_time_limitation(data, week):
    print("\n[#4] Incentive Time Limitation...")
    df = filter_top_cities(filter_week(data["short_main"], week))
    return crosstab_by_city(df, "snapp_incentive_active_duration", top_cities=False)


def analysis_received_incentive_types(data, week):
    print("\n[#5/#6] Received Incentive Types...")
    lm = filter_top_cities(add_city(filter_week(data["long_main"], week), data["_lookup"]))
    results = {}
    for platform, q in [("Snapp", "Snapp Incentive Type"), ("Tapsi", "Tapsi Incentive Type")]:
        qdf = lm[lm["question"] == q]
        if qdf.empty:
            results[platform] = pd.DataFrame()
            continue
        resp = qdf.groupby("city")["recordID"].nunique()
        ans = qdf.groupby(["city", "answer"]).size().unstack(fill_value=0)
        pct = (ans.div(resp, axis=0) * 100).round(1)
        pct["n"] = resp
        pct.index.name = "City"
        results[platform] = pct
    return results


def analysis_incentive_dissatisfaction(data, week):
    print("\n[#8/#9] Incentive Dissatisfaction Reasons...")
    sm = filter_week(data["short_main"], week)
    lm = filter_top_cities(add_city(filter_week(data["long_main"], week), data["_lookup"]))

    results = {}
    for platform, sat_col, q_list in [
        ("Snapp", "snapp_overall_incentive_satisfaction",
         ["Snapp Incentive Unsatisfaction", "Snapp Last Incentive Unsatisfaction"]),
        ("Tapsi", "tapsi_overall_incentive_satisfaction",
         ["Tapsi Incentive Unsatisfaction", "Tapsi Last Incentive Unsatisfaction"]),
    ]:
        dissat_ids = sm[sm[sat_col].le(3)]["recordID"].unique()
        reasons = lm[(lm["recordID"].isin(dissat_ids)) & (lm["question"].isin(q_list))]
        if reasons.empty:
            results[platform] = pd.DataFrame()
            continue
        resp = reasons.groupby("city")["recordID"].nunique()
        ans = reasons.groupby(["city", "answer"]).size().unstack(fill_value=0)
        pct = (ans.div(resp, axis=0) * 100).round(1)
        pct["n_dissatisfied"] = resp
        pct.index.name = "City"
        results[platform] = pct

    summary = {}
    for platform, sat_col, q_list in [
        ("Snapp", "snapp_overall_incentive_satisfaction",
         ["Snapp Incentive Unsatisfaction", "Snapp Last Incentive Unsatisfaction"]),
        ("Tapsi", "tapsi_overall_incentive_satisfaction",
         ["Tapsi Incentive Unsatisfaction", "Tapsi Last Incentive Unsatisfaction"]),
    ]:
        dissat_ids = sm[sm[sat_col].le(3)]["recordID"].unique()
        lm_all = filter_week(data["long_main"], week)
        reasons_all = lm_all[(lm_all["recordID"].isin(dissat_ids)) & (lm_all["question"].isin(q_list))]
        if reasons_all.empty:
            continue
        total_d = len(dissat_ids)
        counts = reasons_all["answer"].value_counts()
        summary[platform] = pd.DataFrame({
            "count": counts,
            "pct_of_dissatisfied": (counts / total_d * 100).round(1),
        })
    results["summary"] = summary
    return results


def analysis_all_cities_overview(data, week):
    print("\n[#12] All Cities Overview...")
    df = filter_top_cities(filter_week(data["short_main"], week))
    if df.empty:
        return pd.DataFrame()
    overview = df.groupby("city").agg(
        respondent_count=("recordID", "count"),
        joint_count=("active_joint", lambda x: (x == 1).sum()),
        joint_pct=("active_joint", lambda x: (x == 1).mean() * 100),
        avg_snapp_LOC=("snapp_LOC", "mean"),
        avg_tapsi_LOC=("tapsi_LOC", "mean"),
        avg_snapp_ride=("snapp_ride", "mean"),
        avg_tapsi_ride=("tapsi_ride", "mean"),
        got_incentive_msg_pct=("snapp_gotmessage_text_incentive",
                               lambda x: (x.dropna() == "Yes").mean() * 100),
    ).round(1)
    coop = pd.crosstab(df["city"], df["cooperation_type"], normalize="index") * 100
    coop = coop.round(1).add_prefix("coop_%_")
    overview = overview.join(coop)
    total = pd.DataFrame({"respondent_count": [len(df)]}, index=["Total"])
    total["joint_count"] = (df["active_joint"] == 1).sum()
    total["joint_pct"] = round((df["active_joint"] == 1).mean() * 100, 1)
    total["avg_snapp_LOC"] = round(df["snapp_LOC"].mean(), 1)
    total["avg_tapsi_LOC"] = round(df["tapsi_LOC"].mean(), 1)
    total["avg_snapp_ride"] = round(df["snapp_ride"].mean(), 1)
    total["avg_tapsi_ride"] = round(df["tapsi_ride"].mean(), 1)
    overview = pd.concat([overview, total])
    overview.index.name = "City"
    return overview


def analysis_ride_share(data, week):
    print("\n[#13] Drivers' Ride Share...")
    sm = data["short_main"]
    prev_week = week - 1
    results = {}
    for seg_name, seg_fn in [
        ("All Drivers", lambda d: d),
        ("Joint Drivers", lambda d: d[d["active_joint"] == 1]),
        ("Exclusive Snapp", lambda d: d[d["active_joint"] == 0]),
    ]:
        curr = seg_fn(filter_top_cities(filter_week(sm, week)))
        prev = seg_fn(filter_top_cities(filter_week(sm, prev_week)))
        if curr.empty:
            continue
        g = curr.groupby("city").agg(
            n=("recordID", "count"),
            avg_snapp_ride=("snapp_ride", "mean"),
            avg_tapsi_ride=("tapsi_ride", "mean"),
        ).round(1)
        total = g["avg_snapp_ride"] + g["avg_tapsi_ride"]
        g["snapp_share_pct"] = (g["avg_snapp_ride"] / total * 100).round(1)
        g["tapsi_share_pct"] = (100 - g["snapp_share_pct"]).round(1)
        prev_g = prev.groupby("city").agg(
            avg_snapp_ride=("snapp_ride", "mean"),
            avg_tapsi_ride=("tapsi_ride", "mean"),
        )
        prev_total = prev_g.sum(axis=1)
        prev_snapp_pct = (prev_g["avg_snapp_ride"] / prev_total * 100).round(1)
        g["snapp_share_WoW"] = (g["snapp_share_pct"] - prev_snapp_pct.reindex(g.index)).round(1)
        g.index.name = "City"
        results[seg_name] = g
    return results


def analysis_navigation_usage(data, week):
    print("\n[#14] Navigation App Usage...")
    df = filter_top_cities(filter_week(data["short_main"], week))
    results = {}
    for col, label in [
        ("tapsi_navigation_type", "Tapsi Nav Type"),
        ("snapp_last_trip_navigation", "Snapp Last Trip Nav"),
    ]:
        valid = df[df[col].notna()]
        if valid.empty:
            continue
        ct = pd.crosstab(valid["city"], valid[col], normalize="index") * 100
        ct = ct.round(1)
        ct["n"] = valid.groupby("city").size()
        ct.index.name = "City"
        results[label] = ct

    lr = filter_top_cities(add_city(filter_week(data["long_rare"], week), data["_lookup"]))
    for q in ["Navigation Familiarity", "Navigation Installed", "Navigation Used"]:
        qdf = lr[lr["question"] == q]
        if qdf.empty:
            continue
        resp = qdf.groupby("city")["recordID"].nunique()
        ans = qdf.groupby(["city", "answer"]).size().unstack(fill_value=0)
        pct = (ans.div(resp, axis=0) * 100).round(1)
        pct["n"] = resp
        pct.index.name = "City"
        results[q] = pct
    return results


def analysis_driver_persona(data, week):
    print("\n[#15] Drivers' Persona...")
    df = filter_top_cities(filter_week(data["short_main"], week))
    results = {}
    for col, label in [
        ("active_time", "Activity Type"),
        ("age_group", "Age Group"),
        ("edu", "Education"),
        ("marr_stat", "Marital Status"),
        ("original_job", "Original Job"),
        ("gender", "Gender"),
    ]:
        valid = df[df[col].notna()]
        if valid.empty:
            continue
        ct = pd.crosstab(valid["city"], valid[col], normalize="index") * 100
        ct = ct.round(1)
        ct["n"] = valid.groupby("city").size()
        ct = add_total_row(ct, len(valid))
        ct.index.name = "City"
        results[label] = ct
    return results


def analysis_referral_plan(data, week):
    print("\n[#16] Referral Plan...")
    df = filter_top_cities(filter_week(data["short_main"], week))
    results = {}
    for col, label in [
        ("snapp_joining_bonus", "Snapp Joining Bonus"),
        ("tapsi_joining_bonus", "Tapsi Joining Bonus"),
    ]:
        ct = crosstab_by_city(df, col, top_cities=False)
        if not ct.empty:
            results[label] = ct
    return results


def analysis_inactivity_before_incentive(data, week):
    print("\n[#17] Inactivity Before Tapsi Incentive...")
    df = filter_top_cities(filter_week(data["short_main"], week))
    return crosstab_by_city(df, "tapsi_inactive_b4_incentive", top_cities=False)


def analysis_commission_free(data, week):
    print("\n[#18] Commission-Free Analysis...")
    df = filter_top_cities(filter_week(data["short_main"], week))
    cols = [
        "snapp_trip_count_commfree", "snapp_trip_count_commfree_discount",
        "snapp_commfree", "snapp_commfree_disc_ride", "snapp_diff_commfree",
        "tapsi_trip_count_commfree", "tapsi_trip_count_commfree_discount",
        "tapsi_commfree", "tapsi_commfree_disc_ride", "tapsi_diff_commfree",
    ]
    available = [c for c in cols if c in df.columns]
    if not available:
        return pd.DataFrame()
    for c in available:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    means = df.groupby("city")[available].mean().round(1)
    means["n"] = df.groupby("city").size()
    means = add_total_row(means, len(df))
    means.index.name = "City"
    return means


def analysis_lucky_wheel(data, week):
    print("\n[#19] Lucky Wheel (Tapsi)...")
    df = filter_top_cities(filter_week(data["short_main"], week))
    if df.empty:
        return pd.DataFrame()
    res = pd.DataFrame(index=sorted(df["city"].unique()))
    if "wheel" in df.columns:
        res["wheel_usage_pct"] = (df.groupby("city")["wheel"].mean() * 100).round(1)
    if "tapsi_magical_window" in df.columns:
        mw = df[df["tapsi_magical_window"].notna()]
        if not mw.empty:
            ct = pd.crosstab(mw["city"], mw["tapsi_magical_window"], normalize="index") * 100
            ct = ct.round(1)
            res = res.join(ct)
    if "tapsi_magical_window_income" in df.columns:
        df["tapsi_magical_window_income"] = pd.to_numeric(df["tapsi_magical_window_income"], errors="coerce")
        res["avg_magical_window_income"] = df.groupby("city")["tapsi_magical_window_income"].mean().round(0)
    res["n"] = df.groupby("city").size()
    res.index.name = "City"
    return res.dropna(how="all", axis=1)


def analysis_request_refusal(data, week):
    print("\n[Extra] Request Refusal Reasons...")
    lr = filter_top_cities(add_city(filter_week(data["long_rare"], week), data["_lookup"]))
    results = {}
    for q in ["Snapp Request Refusal", "Tapsi Request Refusal"]:
        qdf = lr[lr["question"] == q]
        if qdf.empty:
            continue
        resp = qdf.groupby("city")["recordID"].nunique()
        ans = qdf.groupby(["city", "answer"]).size().unstack(fill_value=0)
        pct = (ans.div(resp, axis=0) * 100).round(1)
        pct["n"] = resp
        pct.index.name = "City"
        results[q] = pct
    return results


# ═══════════════════════════════════════════════════════════════════════════
#  NEW ANALYSIS FUNCTIONS — from short_rare / wide_rare / long_rare
# ═══════════════════════════════════════════════════════════════════════════

def analysis_cs_satisfaction(data, week):
    """Customer Support satisfaction scores by city (from short_rare)."""
    print("\n[#CS] Customer Support Satisfaction...")
    sr = add_city(filter_week(data["short_rare"], week), data["_lookup"])
    sr = filter_top_cities(sr)

    results = {}
    for platform, prefix in [("Snapp", "snapp_CS_"), ("Tapsi", "tapsi_CS_")]:
        sat_cols = [c for c in sr.columns
                    if c.startswith(prefix + "satisfaction_")
                    and c != f"{prefix}satisfaction_important_reason"]
        solved_col = f"{prefix.rstrip('_')}_solved" if platform == "Tapsi" else f"{prefix}solved"
        # Fix: tapsi columns use tapsi_CS_solved
        solved_col = f"{prefix}solved"

        if not sat_cols:
            continue

        # Mean satisfaction by city
        df_result = mean_by_city(sr, sat_cols, top_cities=False)

        # CS solved rate
        if solved_col in sr.columns:
            solved_rate = sr.groupby("city")[solved_col].apply(
                lambda x: (x.dropna() == "Yes").mean() * 100 if len(x.dropna()) > 0 else np.nan
            ).round(1).rename("solved_%")
            df_result = df_result.join(solved_rate, how="left")

        # CS contacted rate (snapp_CS or tapsi_CS_)
        contact_col = "snapp_CS" if platform == "Snapp" else "tapsi_CS_"
        if contact_col in sr.columns:
            contact_rate = sr.groupby("city")[contact_col].apply(
                lambda x: (x.dropna() == "Yes").mean() * 100 if len(x.dropna()) > 0 else np.nan
            ).round(1).rename("contacted_%")
            df_result = df_result.join(contact_rate, how="left")

        results[platform] = df_result
    return results


def analysis_cs_categories(data, week):
    """Customer Support category distribution (from long_rare/wide_rare)."""
    print("\n[#CS_Cat] Customer Support Categories...")
    lr = filter_top_cities(add_city(filter_week(data["long_rare"], week), data["_lookup"]))
    results = {}
    for q in ["Snapp Customer Support Category", "Tapsi Customer Support Category"]:
        qdf = lr[lr["question"] == q]
        if qdf.empty:
            continue
        resp = qdf.groupby("city")["recordID"].nunique()
        ans = qdf.groupby(["city", "answer"]).size().unstack(fill_value=0)
        pct = (ans.div(resp, axis=0) * 100).round(1)
        pct["n_contacted"] = resp
        pct.index.name = "City"
        results[q] = pct
    return results


def analysis_cs_important_reason(data, week):
    """Most important CS satisfaction factor (from short_rare)."""
    print("\n[#CS_Reason] CS Most Important Reason...")
    sr = add_city(filter_week(data["short_rare"], week), data["_lookup"])
    sr = filter_top_cities(sr)
    results = {}
    for platform, col in [
        ("Snapp", "snapp_CS_satisfaction_important_reason"),
        ("Tapsi", "tapsi_CS_satisfaction_important_reason"),
    ]:
        ct = crosstab_by_city(sr, col, top_cities=False)
        if not ct.empty:
            results[platform] = ct
    return results


def analysis_recommend(data, week):
    """Recommendation / NPS scores by city (from short_rare)."""
    print("\n[#Reco] Recommendation Scores...")
    sr = add_city(filter_week(data["short_rare"], week), data["_lookup"])
    sr = filter_top_cities(sr)

    # snapp_recommend and snappdriver_tapsi_recommend are likely 0-10 NPS
    reco_cols = ["snapp_recommend", "tapsidriver_tapsi_recommend",
                 "snappdriver_tapsi_recommend"]
    available = [c for c in reco_cols if c in sr.columns]
    if not available:
        return pd.DataFrame()

    for c in available:
        sr[c] = pd.to_numeric(sr[c], errors="coerce")

    result = mean_by_city(sr, available, top_cities=False)
    return result


def analysis_refer_others(data, week):
    """Would you refer others? distribution by city (from short_rare)."""
    print("\n[#Refer] Refer Others...")
    sr = add_city(filter_week(data["short_rare"], week), data["_lookup"])
    sr = filter_top_cities(sr)
    results = {}
    for col, label in [
        ("snapp_refer_others", "Snapp Refer Others"),
        ("tapsi_refer_others", "Tapsi Refer Others"),
    ]:
        ct = crosstab_by_city(sr, col, top_cities=False)
        if not ct.empty:
            results[label] = ct
    return results


def analysis_navigation_recommendations(data, week):
    """Navigation app recommendation scores (from short_rare)."""
    print("\n[#NavReco] Navigation App Recommendations...")
    sr = add_city(filter_week(data["short_rare"], week), data["_lookup"])
    sr = filter_top_cities(sr)

    reco_cols = [
        "recommendation_googlemap", "recommendation_waze",
        "recommendation_neshan", "recommendation_balad",
        "recommendation_googlemap_last3month", "recommendation_waze_last3month",
        "recommendation_neshan_last3month", "recommendation_balad_last3month",
        "snapp_navigation_app_satisfaction",
        "tapsi_in_app_navigation_satisfaction",
    ]
    available = [c for c in reco_cols if c in sr.columns]
    if not available:
        return pd.DataFrame()
    for c in available:
        sr[c] = pd.to_numeric(sr[c], errors="coerce")
    return mean_by_city(sr, available, top_cities=False)


def analysis_registration(data, week):
    """Registration type & motivation distribution by city (from short_rare)."""
    print("\n[#Reg] Registration Info...")
    sr = add_city(filter_week(data["short_rare"], week), data["_lookup"])
    sr = filter_top_cities(sr)
    results = {}
    for col, label in [
        ("snapp_register_type", "Snapp Register Type"),
        ("tapsi_register_type", "Tapsi Register Type"),
        ("snapp_main_reg_reason", "Snapp Main Reg Reason"),
        ("tapsi_main_reg_reason", "Tapsi Main Reg Reason"),
        ("tapsi_invite_to_reg", "Tapsi Invited to Register"),
    ]:
        ct = crosstab_by_city(sr, col, top_cities=False)
        if not ct.empty:
            results[label] = ct
    return results


def analysis_better_income(data, week):
    """Better income platform preference distribution by city (from short_rare)."""
    print("\n[#Income] Better Income Platform...")
    sr = add_city(filter_week(data["short_rare"], week), data["_lookup"])
    sr = filter_top_cities(sr)
    results = {}
    for col, label in [
        ("snapp_better_income", "Snapp Better Income"),
        ("tapsi_better_income", "Tapsi Better Income"),
    ]:
        ct = crosstab_by_city(sr, col, top_cities=False)
        if not ct.empty:
            results[label] = ct
    return results


def analysis_decline_reasons(data, week):
    """Decline/cancel reasons distribution (from long_rare)."""
    print("\n[#Decline] Decline Reasons...")
    lr = filter_top_cities(add_city(filter_week(data["long_rare"], week), data["_lookup"]))
    qdf = lr[lr["question"] == "Decline Reason"]
    if qdf.empty:
        return pd.DataFrame()
    resp = qdf.groupby("city")["recordID"].nunique()
    ans = qdf.groupby(["city", "answer"]).size().unstack(fill_value=0)
    pct = (ans.div(resp, axis=0) * 100).round(1)
    pct["n"] = resp
    pct.index.name = "City"
    return pct


def analysis_snappcarfix_satisfaction(data, week):
    """Snappcarfix satisfaction scores by city (from short_rare)."""
    print("\n[#Carfix] Snappcarfix Satisfaction...")
    sr = add_city(filter_week(data["short_rare"], week), data["_lookup"])
    sr = filter_top_cities(sr)

    sat_cols = [c for c in sr.columns if c.startswith("snappcarfix_satisfaction_")]
    extra = ["snappcarfix_recommend"]
    all_cols = sat_cols + [c for c in extra if c in sr.columns]
    if not all_cols:
        return {}

    results = {}

    # Satisfaction scores
    for c in all_cols:
        sr[c] = pd.to_numeric(sr[c], errors="coerce")
    sat_result = mean_by_city(sr, all_cols, top_cities=False)
    if not sat_result.empty:
        results["Satisfaction"] = sat_result

    # Familiarity & usage rates
    for col, label in [
        ("snappcarfix_familiar", "Familiar"),
        ("snappcarfix_use_ever", "Used Ever"),
        ("snappcarfix_use_lastmo", "Used Last Month"),
    ]:
        ct = crosstab_by_city(sr, col, top_cities=False)
        if not ct.empty:
            results[label] = ct

    return results


def analysis_tapsigarage_satisfaction(data, week):
    """Tapsigarage satisfaction scores by city (from short_rare)."""
    print("\n[#Garage] Tapsigarage Satisfaction...")
    sr = add_city(filter_week(data["short_rare"], week), data["_lookup"])
    sr = filter_top_cities(sr)

    sat_cols = [c for c in sr.columns if c.startswith("tapsigarage_satisfaction_")]
    extra = ["tapsigarage_recommend"]
    all_cols = sat_cols + [c for c in extra if c in sr.columns]
    if not all_cols:
        return {}

    results = {}
    for c in all_cols:
        sr[c] = pd.to_numeric(sr[c], errors="coerce")
    sat_result = mean_by_city(sr, all_cols, top_cities=False)
    if not sat_result.empty:
        results["Satisfaction"] = sat_result

    for col, label in [
        ("tapsigarage_familiar", "Familiar"),
        ("tapsigarage_use_ever", "Used Ever"),
        ("tapsigarage_use_lastmo", "Used Last Month"),
    ]:
        ct = crosstab_by_city(sr, col, top_cities=False)
        if not ct.empty:
            results[label] = ct
    return results


def analysis_demand(data, week):
    """Demand perception metrics by city (from short_rare)."""
    print("\n[#Demand] Demand Perception...")
    sr = add_city(filter_week(data["short_rare"], week), data["_lookup"])
    sr = filter_top_cities(sr)

    cols = ["max_demand", "demand_process", "missed_demand_per_10"]
    available = [c for c in cols if c in sr.columns]
    if not available:
        return pd.DataFrame()
    for c in available:
        sr[c] = pd.to_numeric(sr[c], errors="coerce")
    return mean_by_city(sr, available, top_cities=False)


def analysis_speed_satisfaction(data, week):
    """Speed satisfaction by city (from short_rare)."""
    print("\n[#Speed] Speed Satisfaction...")
    sr = add_city(filter_week(data["short_rare"], week), data["_lookup"])
    sr = filter_top_cities(sr)
    cols = ["snapp_speed_satisfaction", "tapsi_speed_satisfaction"]
    available = [c for c in cols if c in sr.columns]
    if not available:
        return pd.DataFrame()
    for c in available:
        sr[c] = pd.to_numeric(sr[c], errors="coerce")
    return mean_by_city(sr, available, top_cities=False)


def analysis_gps(data, week):
    """GPS problem and fix-location metrics by city (from short_rare)."""
    print("\n[#GPS] GPS Problems...")
    sr = add_city(filter_week(data["short_rare"], week), data["_lookup"])
    sr = filter_top_cities(sr)

    results = {}
    # GPS problem rate
    for col, label in [
        ("gps_problem", "GPS Problem"),
        ("gps_interrupt_impact", "GPS Interrupt Impact"),
        ("fixlocation_familiar", "FixLocation Familiar"),
        ("fixlocation_use", "FixLocation Use"),
        ("snapp_gps_stage", "Snapp GPS Stage"),
        ("tapsi_gps_stage", "Tapsi GPS Stage"),
        ("tapsi_gps_better", "Tapsi GPS Better"),
    ]:
        ct = crosstab_by_city(sr, col, top_cities=False)
        if not ct.empty:
            results[label] = ct

    # Satisfaction score
    if "fixlocation_satisfaction" in sr.columns:
        sr["fixlocation_satisfaction"] = pd.to_numeric(sr["fixlocation_satisfaction"], errors="coerce")
        sat = mean_by_city(sr, ["fixlocation_satisfaction"], top_cities=False)
        if not sat.empty:
            results["FixLoc Satisfaction"] = sat

    return results


def analysis_unpaid_by_passenger(data, week):
    """Unpaid by passenger metrics by city (from short_rare)."""
    print("\n[#Unpaid] Unpaid by Passenger...")
    sr = add_city(filter_week(data["short_rare"], week), data["_lookup"])
    sr = filter_top_cities(sr)

    results = {}
    # Unpaid rate
    ct = crosstab_by_city(sr, "unpaid_by_passenger", top_cities=False)
    if not ct.empty:
        results["Unpaid Rate"] = ct

    # Follow-up satisfaction
    for prefix, label in [("snapp_", "Snapp"), ("tapsi_", "Tapsi")]:
        sat_cols = [f"{prefix}satisfaction_followup_overall",
                    f"{prefix}satisfaction_followup_time"]
        available = [c for c in sat_cols if c in sr.columns]
        if available:
            for c in available:
                sr[c] = pd.to_numeric(sr[c], errors="coerce")
            sat = mean_by_city(sr, available, top_cities=False)
            if not sat.empty:
                results[f"{label} Followup Sat"] = sat

        # Compensated rate
        comp_col = f"{prefix}compensate_unpaid_by_passenger"
        ct2 = crosstab_by_city(sr, comp_col, top_cities=False)
        if not ct2.empty:
            results[f"{label} Compensated"] = ct2

    return results


def analysis_distance_to_origin(data, week):
    """Distance-to-origin time satisfaction (from short_rare)."""
    print("\n[#DistOrigin] Distance to Origin Satisfaction...")
    sr = add_city(filter_week(data["short_rare"], week), data["_lookup"])
    sr = filter_top_cities(sr)
    cols = ["snapp_distancetooring_time_satisfaction",
            "tapsi_distancetooring_time_satisfaction"]
    available = [c for c in cols if c in sr.columns]
    if not available:
        return pd.DataFrame()
    for c in available:
        sr[c] = pd.to_numeric(sr[c], errors="coerce")
    return mean_by_city(sr, available, top_cities=False)


# ─── Main ────────────────────────────────────────────────────────────────────
def run_all(week=None):
    data = load_data()
    if week is None:
        week = get_latest_week(data["short_main"])
    print(f"\nRunning analyses for Week {week}")
    print("=" * 60)

    sheets = {}

    # ── Original analyses (from short_main / wide_main / long_main) ──
    sheets["#1_Snapp_Incentive_Amt"] = analysis_incentive_amounts_snapp(data, week)
    sheets["#2_Tapsi_Incentive_Amt"] = analysis_incentive_amounts_tapsi(data, week)

    for seg, df in analysis_satisfaction_review(data, week).items():
        sheets[f"#3_Sat_{seg[:20]}"] = df

    sheets["#4_Incentive_Duration"] = analysis_incentive_time_limitation(data, week)

    for plat, df in analysis_received_incentive_types(data, week).items():
        sheets[f"#5_6_IncType_{plat}"] = df

    dissat = analysis_incentive_dissatisfaction(data, week)
    summary = dissat.pop("summary", {})
    for plat, df in dissat.items():
        sheets[f"#8_Dissat_{plat}"] = df
    for plat, df in summary.items():
        sheets[f"#9_Dissat_Sum_{plat}"] = df

    sheets["#12_Cities_Overview"] = analysis_all_cities_overview(data, week)

    for seg, df in analysis_ride_share(data, week).items():
        sheets[f"#13_RideShare_{seg[:15]}"] = df

    for label, df in analysis_navigation_usage(data, week).items():
        sheets[f"#14_Nav_{label[:20]}"] = df

    for label, df in analysis_driver_persona(data, week).items():
        sheets[f"#15_Persona_{label[:16]}"] = df

    for label, df in analysis_referral_plan(data, week).items():
        sheets[f"#16_Ref_{label[:20]}"] = df

    sheets["#17_Inactivity"] = analysis_inactivity_before_incentive(data, week)
    sheets["#18_CommFree"] = analysis_commission_free(data, week)
    sheets["#19_LuckyWheel"] = analysis_lucky_wheel(data, week)

    for label, df in analysis_request_refusal(data, week).items():
        safe = label.replace(" ", "_")[:20]
        sheets[f"#20_Refusal_{safe}"] = df

    # ── NEW analyses (from short_rare / wide_rare / long_rare) ──
    for plat, df in analysis_cs_satisfaction(data, week).items():
        sheets[f"#CS_Sat_{plat}"] = df

    for label, df in analysis_cs_categories(data, week).items():
        safe = label.replace(" ", "_")[:18]
        sheets[f"#CS_Cat_{safe}"] = df

    for plat, df in analysis_cs_important_reason(data, week).items():
        sheets[f"#CS_Reason_{plat}"] = df

    reco = analysis_recommend(data, week)
    if isinstance(reco, pd.DataFrame) and not reco.empty:
        sheets["#Reco_NPS"] = reco

    for label, df in analysis_refer_others(data, week).items():
        safe = label.replace(" ", "_")[:18]
        sheets[f"#Refer_{safe}"] = df

    nav_reco = analysis_navigation_recommendations(data, week)
    if isinstance(nav_reco, pd.DataFrame) and not nav_reco.empty:
        sheets["#NavReco_Scores"] = nav_reco

    for label, df in analysis_registration(data, week).items():
        safe = label.replace(" ", "_")[:18]
        sheets[f"#Reg_{safe}"] = df

    for label, df in analysis_better_income(data, week).items():
        safe = label.replace(" ", "_")[:18]
        sheets[f"#Income_{safe}"] = df

    decline = analysis_decline_reasons(data, week)
    if isinstance(decline, pd.DataFrame) and not decline.empty:
        sheets["#Decline_Reasons"] = decline

    for label, df in analysis_snappcarfix_satisfaction(data, week).items():
        sheets[f"#Carfix_{label[:20]}"] = df

    for label, df in analysis_tapsigarage_satisfaction(data, week).items():
        sheets[f"#Garage_{label[:20]}"] = df

    demand = analysis_demand(data, week)
    if isinstance(demand, pd.DataFrame) and not demand.empty:
        sheets["#Demand_Perception"] = demand

    speed = analysis_speed_satisfaction(data, week)
    if isinstance(speed, pd.DataFrame) and not speed.empty:
        sheets["#Speed_Satisfaction"] = speed

    dist = analysis_distance_to_origin(data, week)
    if isinstance(dist, pd.DataFrame) and not dist.empty:
        sheets["#DistOrigin_Sat"] = dist

    for label, df in analysis_gps(data, week).items():
        safe = label.replace(" ", "_")[:18]
        sheets[f"#GPS_{safe}"] = df

    for label, df in analysis_unpaid_by_passenger(data, week).items():
        safe = label.replace(" ", "_")[:18]
        sheets[f"#Unpaid_{safe}"] = df

    # ─── Clean up: drop all-NaN columns & fully-empty sheets ────────────
    cleaned = {}
    for name, df in sheets.items():
        if df is None or (isinstance(df, pd.DataFrame) and df.empty):
            continue
        # Drop columns where every value is NaN (rotating questions not asked this week)
        meta_cols = {"n", "n_dissatisfied", "n_contacted"}
        data_cols = [c for c in df.columns if c not in meta_cols]
        keep_cols = [c for c in data_cols if not df[c].isna().all()]
        keep_cols += [c for c in meta_cols if c in df.columns]
        df = df[keep_cols]

        # If only meta columns remain (all data cols were NaN), skip sheet
        remaining_data = [c for c in df.columns if c not in meta_cols]
        if not remaining_data:
            print(f"  Skipping '{name}': no data for this week (rotating question)")
            continue
        cleaned[name] = df

    # ─── Export with formatting ──────────────────────────────────────────
    output_path = os.path.join(OUTPUT_DIR, f"routine_analysis_week_{week}.xlsx")
    # If file is locked (open in Excel), try an alternate name
    if os.path.exists(output_path):
        try:
            with open(output_path, "a"):
                pass
        except PermissionError:
            alt = os.path.join(OUTPUT_DIR, f"routine_analysis_week_{week}_new.xlsx")
            print(f"\n  File is open in Excel, saving to: {alt}")
            output_path = alt
    print(f"\n{'=' * 60}")
    print(f"Exporting to {output_path}...")

    # Border & header styles
    thin_border = Border(
        left=Side(style="thin"),
        right=Side(style="thin"),
        top=Side(style="thin"),
        bottom=Side(style="thin"),
    )
    header_font = Font(bold=True)
    header_fill = PatternFill(start_color="D9E1F2", end_color="D9E1F2", fill_type="solid")
    header_align = Alignment(horizontal="center", wrap_text=True)

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for name, df in cleaned.items():
            safe_name = name[:31]

            df = sort_cities(df)
            df = reorder_columns(df, name)
            df = convert_pct_to_decimal(df.copy(), name)

            try:
                df.to_excel(writer, sheet_name=safe_name)
            except Exception as e:
                print(f"  Warning: sheet '{safe_name}': {e}")
                continue

            ws = writer.sheets[safe_name]

            # 1) Borders on all cells + header formatting
            max_row = ws.max_row
            max_col = ws.max_column
            for row in ws.iter_rows(min_row=1, max_row=max_row,
                                    min_col=1, max_col=max_col):
                for cell in row:
                    cell.border = thin_border
                    if cell.row == 1:
                        cell.font = header_font
                        cell.fill = header_fill
                        cell.alignment = header_align

            # 2) Conditional formatting (color scales)
            apply_conditional_formatting(ws, df, name)

            # 3) Percentage number format
            format_pct_cells(ws, df, name)

            # 4) Auto-fit column widths
            for col_idx in range(1, max_col + 1):
                col_letter = get_column_letter(col_idx)
                max_len = 0
                for row_idx in range(1, min(max_row + 1, 50)):  # sample first 50 rows
                    val = ws.cell(row=row_idx, column=col_idx).value
                    if val is not None:
                        max_len = max(max_len, len(str(val)))
                ws.column_dimensions[col_letter].width = min(max(max_len + 3, 10), 35)

            # 5) Freeze top row + index column
            ws.freeze_panes = "B2"

    print(f"Done! {len(cleaned)} sheets written to: {output_path}")
    return cleaned


def resolve_week(args, df):
    """
    Parse CLI args to a weeknumber.
    Accepts:  '2025 52'  or  '52'  or  nothing (auto-detect latest).
    When year+week given, maps to the weeknumber in the data.
    """
    if not args:
        return get_latest_week(df)

    if len(args) == 2:
        year, iso_week = int(args[0]), int(args[1])
    elif len(args) == 1:
        val = int(args[0])
        if val > 100:
            # e.g. "202552" → year=2025, week=52
            year, iso_week = divmod(val, 100)
        else:
            return val  # plain weeknumber
    else:
        print("Usage: python survey_routine_analysis.py [YEAR WEEK]")
        print("  e.g.: python survey_routine_analysis.py 2025 52")
        print("        python survey_routine_analysis.py 52")
        raise SystemExit(1)

    # Map year+ISO-week to the weeknumber in the data
    dt = df["datetime"].dropna()
    dt = pd.to_datetime(dt, errors="coerce")
    df_copy = pd.DataFrame({"weeknumber": df["weeknumber"], "dt": dt}).dropna()
    df_copy["iso_year"] = df_copy["dt"].dt.isocalendar().year.astype(int)
    df_copy["iso_week"] = df_copy["dt"].dt.isocalendar().week.astype(int)

    match = df_copy[(df_copy["iso_year"] == year) & (df_copy["iso_week"] == iso_week)]
    if match.empty:
        print(f"No data found for year {year}, week {iso_week}")
        # Show available year-week combos
        available = df_copy.groupby(["iso_year", "iso_week"])["weeknumber"].first()
        print(f"Available: {available.tail(10).to_dict()}")
        raise SystemExit(1)

    resolved = match["weeknumber"].mode().iloc[0]
    print(f"Resolved {year}-W{iso_week:02d} -> weeknumber {resolved}")
    return resolved


if __name__ == "__main__":
    import sys
    # Quick-load just the columns needed for week resolution
    _sm = pd.read_csv(os.path.join(PROCESSED_DIR, "short_survey_main.csv"),
                       usecols=["weeknumber", "datetime"], low_memory=False)
    week = resolve_week(sys.argv[1:], _sm)
    del _sm
    run_all(week)
