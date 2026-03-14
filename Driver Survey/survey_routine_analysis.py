"""
Driver Survey Routine Analysis
Produces conceptually similar outputs to the weekly Excel routine report.
Reads from the 6 processed CSV files and generates analysis tables by city/week.
"""

import pandas as pd
import numpy as np
import os
import warnings

warnings.filterwarnings("ignore")

# ─── Config ──────────────────────────────────────────────────────────────────
PROCESSED_DIR = os.path.join(os.path.dirname(__file__), "processed")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "routine_output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

TOP_CITIES = [
    "Tehran(city)", "Isfahan", "Mashhad", "Shiraz", "Tabriz", "Karaj",
    "Ahwaz", "Qom", "Kerman", "Rasht", "Hamedan", "Arak",
    "Yazd", "Urumieh", "Zanjan", "Gorgan", "Sari", "Kish",
    "Kermanshah", "Bojnurd", "Ardebil", "Ghazvin", "Khorramabad",
]

# Columns to carry from short_main when merging into other frames
MERGE_COLS = ["recordID", "city", "active_time", "cooperation_type",
              "age_group", "edu", "marr_stat", "gender", "original_job"]


# ─── Data Loading ────────────────────────────────────────────────────────────
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

    # Pre-compute a lookup table for merging city/demographics into other frames
    available = [c for c in MERGE_COLS if c in data["short_main"].columns]
    data["_lookup"] = data["short_main"][available].drop_duplicates(subset="recordID")
    return data


def get_latest_week(df, min_respondents=100):
    """Get the latest week with at least min_respondents records."""
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
    """Merge city and demographics into a frame that lacks them."""
    if "city" in df.columns:
        return df
    merge_cols = [c for c in lookup.columns if c not in df.columns or c == "recordID"]
    return df.merge(lookup[merge_cols], on="recordID", how="left")


def add_total_row(ct, n_total=None):
    """Append a Total row that is the weighted average of existing rows (or column mean)."""
    total = ct.select_dtypes(include="number").mean().to_frame().T
    total.index = ["Total"]
    if n_total is not None and "n" in ct.columns:
        total["n"] = n_total
    return pd.concat([ct, total]).round(1)


# ─── Analysis #1: Snapp Incentive Amounts Distribution ───────────────────────
def analysis_incentive_amounts_snapp(data, week):
    print("\n[#1] Snapp Incentive Amounts Distribution...")
    df = filter_top_cities(filter_week(data["short_main"], week))
    col = "snapp_incentive_rial_details"
    valid = df[df[col].notna()]
    if valid.empty:
        return pd.DataFrame()

    ct = pd.crosstab(valid["city"], valid[col], normalize="index") * 100
    ct = ct.round(1)
    ct["n"] = valid.groupby("city").size()
    ct = add_total_row(ct, len(valid))
    ct.index.name = "City"
    return ct


# ─── Analysis #2: Tapsi Incentive Amounts (Joint Drivers) ────────────────────
def analysis_incentive_amounts_tapsi(data, week):
    print("\n[#2] Tapsi Incentive Amounts (Joint Drivers)...")
    df = filter_top_cities(filter_week(data["short_main"], week))
    col = "tapsi_incentive_rial_details"
    joint_types = ["Joint from Snapp", "Joint from Tapsi", "Joint from Both"]
    valid = df[(df["cooperation_type"].isin(joint_types)) & (df[col].notna())]
    if valid.empty:
        return pd.DataFrame()

    ct = pd.crosstab(valid["city"], valid[col], normalize="index") * 100
    ct = ct.round(1)
    ct["n"] = valid.groupby("city").size()
    ct = add_total_row(ct, len(valid))
    ct.index.name = "City"
    return ct


# ─── Analysis #3: Satisfaction Review ────────────────────────────────────────
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

    rare_sat = ["snapp_overall_satisfaction", "tapsi_overall_satisfaction"]
    rare_sat = [c for c in rare_sat if c in sr.columns]

    results = {}
    for seg_name, seg_fn in [
        ("All Drivers", lambda d: d),
        ("Part-Time", lambda d: d[d["active_time"] == "Part Time"]),
        ("Full-Time", lambda d: d[d["active_time"] == "Full Time"]),
    ]:
        # Current & previous week from short_main
        curr = seg_fn(filter_top_cities(filter_week(sm, week)))
        prev = seg_fn(filter_top_cities(filter_week(sm, prev_week)))

        curr_avg = curr.groupby("city")[main_sat].mean().round(2)
        prev_avg = prev.groupby("city")[main_sat].mean().round(2)

        # Rare satisfaction — needs city merged in
        if rare_sat:
            sr_curr = add_city(filter_week(sr, week), lookup)
            sr_prev = add_city(filter_week(sr, prev_week), lookup)
            sr_curr = seg_fn(filter_top_cities(sr_curr))
            sr_prev = seg_fn(filter_top_cities(sr_prev))
            curr_rare_avg = sr_curr.groupby("city")[rare_sat].mean().round(2)
            prev_rare_avg = sr_prev.groupby("city")[rare_sat].mean().round(2)
            curr_avg = curr_avg.join(curr_rare_avg, how="outer")
            prev_avg = prev_avg.join(prev_rare_avg, how="outer")

        # Incentive participation rate
        if "snapp_incentive_message_participation" in curr.columns:
            part = curr.groupby("city")["snapp_incentive_message_participation"].apply(
                lambda x: (x.dropna() == "Yes").mean() * 100 if len(x.dropna()) > 0 else np.nan
            ).round(1).rename("incentive_participation_%")
            curr_avg = curr_avg.join(part)

        # WoW — only compare columns present in both
        num_cols = curr_avg.select_dtypes(include="number").columns
        shared = num_cols.intersection(prev_avg.columns)
        wow = (curr_avg[shared] - prev_avg.reindex(curr_avg.index)[shared]).round(2)
        wow.columns = [f"{c}_WoW" for c in wow.columns]

        # Combine: n + alternating current / WoW
        combined = pd.DataFrame(index=curr_avg.index)
        combined["n"] = curr.groupby("city").size()
        for c in curr_avg.columns:
            combined[c] = curr_avg[c]
            wc = f"{c}_WoW"
            if wc in wow.columns:
                combined[wc] = wow[wc]

        # Total
        total = pd.DataFrame({"n": [len(curr)]}, index=["Total"])
        for c in curr_avg.select_dtypes(include="number").columns:
            total[c] = curr_avg[c].mean()
        combined = pd.concat([combined, total]).round(2)
        combined.index.name = "City"
        results[seg_name] = combined

    return results


# ─── Analysis #4: Incentive Time Limitation ──────────────────────────────────
def analysis_incentive_time_limitation(data, week):
    print("\n[#4] Incentive Time Limitation...")
    df = filter_top_cities(filter_week(data["short_main"], week))
    col = "snapp_incentive_active_duration"
    valid = df[df[col].notna()]
    if valid.empty:
        return pd.DataFrame()
    ct = pd.crosstab(valid["city"], valid[col], normalize="index") * 100
    ct = ct.round(1)
    ct["n"] = valid.groupby("city").size()
    ct = add_total_row(ct, len(valid))
    ct.index.name = "City"
    return ct


# ─── Analysis #5/#6: Received Incentive Types ────────────────────────────────
def analysis_received_incentive_types(data, week):
    print("\n[#5/#6] Received Incentive Types...")
    lm = filter_week(data["long_main"], week)
    lm = add_city(lm, data["_lookup"])
    lm = filter_top_cities(lm)

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


# ─── Analysis #8/#9: Incentive Dissatisfaction Reasons ───────────────────────
def analysis_incentive_dissatisfaction(data, week):
    print("\n[#8/#9] Incentive Dissatisfaction Reasons...")
    sm = filter_week(data["short_main"], week)
    lm = filter_week(data["long_main"], week)
    lm = add_city(lm, data["_lookup"])
    lm = filter_top_cities(lm)

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

    # Summary (no city breakdown)
    summary = {}
    for platform, sat_col, q_list in [
        ("Snapp", "snapp_overall_incentive_satisfaction",
         ["Snapp Incentive Unsatisfaction", "Snapp Last Incentive Unsatisfaction"]),
        ("Tapsi", "tapsi_overall_incentive_satisfaction",
         ["Tapsi Incentive Unsatisfaction", "Tapsi Last Incentive Unsatisfaction"]),
    ]:
        dissat_ids = sm[sm[sat_col].le(3)]["recordID"].unique()
        # Use unfiltered long_main for summary
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


# ─── Analysis #12: All Cities Overview ───────────────────────────────────────
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

    # Cooperation type breakdown
    coop = pd.crosstab(df["city"], df["cooperation_type"], normalize="index") * 100
    coop = coop.round(1).add_prefix("coop_%_")
    overview = overview.join(coop)

    # Total
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


# ─── Analysis #13: Ride Share ────────────────────────────────────────────────
def analysis_ride_share(data, week):
    print("\n[#13] Drivers' Ride Share...")
    sm = data["short_main"]
    prev_week = week - 1

    results = {}
    for seg_name, seg_fn in [
        ("All Drivers", lambda d: d),
        ("Joint Drivers", lambda d: d[d["active_joint"] == 1]),
        ("Exclusive Snapp", lambda d: d[d["cooperation_type"] == "Exclusive Snapp"]),
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

        # WoW
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


# ─── Analysis #14: Navigation App Usage ──────────────────────────────────────
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

    # Navigation questions from long_rare
    lr = filter_week(data["long_rare"], week)
    lr = add_city(lr, data["_lookup"])
    lr = filter_top_cities(lr)
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


# ─── Analysis #15: Drivers' Persona ──────────────────────────────────────────
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


# ─── Analysis #16: Referral Plan ─────────────────────────────────────────────
def analysis_referral_plan(data, week):
    print("\n[#16] Referral Plan...")
    df = filter_top_cities(filter_week(data["short_main"], week))
    results = {}
    for col, label in [
        ("snapp_joining_bonus", "Snapp Joining Bonus"),
        ("tapsi_joining_bonus", "Tapsi Joining Bonus"),
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


# ─── Analysis #17: Inactivity Before Incentive ───────────────────────────────
def analysis_inactivity_before_incentive(data, week):
    print("\n[#17] Inactivity Before Tapsi Incentive...")
    df = filter_top_cities(filter_week(data["short_main"], week))
    col = "tapsi_inactive_b4_incentive"
    valid = df[df[col].notna()]
    if valid.empty:
        return pd.DataFrame()
    ct = pd.crosstab(valid["city"], valid[col], normalize="index") * 100
    ct = ct.round(1)
    ct["n"] = valid.groupby("city").size()
    ct = add_total_row(ct, len(valid))
    ct.index.name = "City"
    return ct


# ─── Analysis #18: Commission-Free Analysis ─────────────────────────────────
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


# ─── Analysis #19: Lucky Wheel / Magical Window ─────────────────────────────
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
        inc = df.groupby("city")["tapsi_magical_window_income"].mean().round(0)
        res["avg_magical_window_income"] = inc

    res["n"] = df.groupby("city").size()
    res.index.name = "City"
    return res.dropna(how="all", axis=1)


# ─── Extra: Request Refusal Reasons ──────────────────────────────────────────
def analysis_request_refusal(data, week):
    print("\n[Extra] Request Refusal Reasons...")
    lr = filter_week(data["long_rare"], week)
    lr = add_city(lr, data["_lookup"])
    lr = filter_top_cities(lr)

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


# ─── Main ────────────────────────────────────────────────────────────────────
def run_all(week=None):
    data = load_data()
    if week is None:
        week = get_latest_week(data["short_main"])
    print(f"\nRunning analyses for Week {week}")
    print("=" * 60)

    sheets = {}

    # #1 / #2
    sheets["#1_Snapp_Incentive_Amt"] = analysis_incentive_amounts_snapp(data, week)
    sheets["#2_Tapsi_Incentive_Amt"] = analysis_incentive_amounts_tapsi(data, week)

    # #3 Satisfaction
    for seg, df in analysis_satisfaction_review(data, week).items():
        sheets[f"#3_Sat_{seg[:20]}"] = df

    # #4
    sheets["#4_Incentive_Duration"] = analysis_incentive_time_limitation(data, week)

    # #5/#6
    for plat, df in analysis_received_incentive_types(data, week).items():
        sheets[f"#5_6_IncType_{plat}"] = df

    # #8/#9
    dissat = analysis_incentive_dissatisfaction(data, week)
    summary = dissat.pop("summary", {})
    for plat, df in dissat.items():
        sheets[f"#8_Dissat_{plat}"] = df
    for plat, df in summary.items():
        sheets[f"#9_Dissat_Sum_{plat}"] = df

    # #12
    sheets["#12_Cities_Overview"] = analysis_all_cities_overview(data, week)

    # #13
    for seg, df in analysis_ride_share(data, week).items():
        sheets[f"#13_RideShare_{seg[:15]}"] = df

    # #14
    for label, df in analysis_navigation_usage(data, week).items():
        sheets[f"#14_Nav_{label[:20]}"] = df

    # #15
    for label, df in analysis_driver_persona(data, week).items():
        sheets[f"#15_Persona_{label[:16]}"] = df

    # #16
    for label, df in analysis_referral_plan(data, week).items():
        sheets[f"#16_Ref_{label[:20]}"] = df

    # #17
    sheets["#17_Inactivity"] = analysis_inactivity_before_incentive(data, week)

    # #18
    sheets["#18_CommFree"] = analysis_commission_free(data, week)

    # #19
    sheets["#19_LuckyWheel"] = analysis_lucky_wheel(data, week)

    # Extra
    for label, df in analysis_request_refusal(data, week).items():
        safe = label.replace(" ", "_")[:20]
        sheets[f"Extra_{safe}"] = df

    # ─── Export ──────────────────────────────────────────────────────────
    output_path = os.path.join(OUTPUT_DIR, f"routine_analysis_week_{week}.xlsx")
    print(f"\n{'=' * 60}")
    print(f"Exporting to {output_path}...")

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for name, df in sheets.items():
            if df is None or (isinstance(df, pd.DataFrame) and df.empty):
                continue
            safe_name = name[:31]
            try:
                df.to_excel(writer, sheet_name=safe_name)
            except Exception as e:
                print(f"  Warning: sheet '{safe_name}': {e}")

    written = sum(1 for v in sheets.values()
                  if v is not None and not (isinstance(v, pd.DataFrame) and v.empty))
    print(f"Done! {written} sheets written to: {output_path}")
    return sheets


if __name__ == "__main__":
    import sys
    week = int(sys.argv[1]) if len(sys.argv) > 1 else None
    run_all(week)
