"""
Driver Survey — Metrics Summary Generator
==========================================
Reads the 6 processed CSV files produced by data_cleaning.py and outputs
a single Markdown file (metrics_summary.md) containing every quantitative
metric, table, and comparison needed for an LLM to generate strategic
interpretations — without the LLM ever needing raw data.

Usage:
    python survey_metrics_summary.py

Output:
    D:/Work/Driver Survey/processed/metrics_summary.md
"""

import os
import warnings
import numpy as np
import pandas as pd
from datetime import datetime

warnings.filterwarnings("ignore")

# ============================================================================
# CONFIGURATION
# ============================================================================
BASE = r"D:\Work\Driver Survey\processed"
SHORT_MAIN = os.path.join(BASE, "short_survey_main.csv")
SHORT_RARE = os.path.join(BASE, "short_survey_rare.csv")
WIDE_MAIN  = os.path.join(BASE, "wide_survey_main.csv")
WIDE_RARE  = os.path.join(BASE, "wide_survey_rare.csv")
LONG_MAIN  = os.path.join(BASE, "long_survey_main.csv")
LONG_RARE  = os.path.join(BASE, "long_survey_rare.csv")
OUTPUT     = os.path.join(BASE, "metrics_summary.md")

MIN_WEEK_RESPONSES = 100
TOP_N_CITIES = 15

# ============================================================================
# HELPERS
# ============================================================================

def safe_load(path):
    """Load CSV, return None if missing."""
    if not os.path.exists(path):
        print(f"[WARN] File not found: {path}")
        return None
    return pd.read_csv(path, encoding="utf-8-sig", low_memory=False)


def safe_col(df, col):
    """Return column as Series, or empty float Series if missing."""
    if df is None or col not in df.columns:
        return pd.Series(dtype=float)
    return df[col]


def safe_numeric(series):
    """Coerce to numeric, dropping non-numeric."""
    return pd.to_numeric(series, errors="coerce")


def safe_mean(series, digits=2):
    """Mean of numeric series, or 'N/A'."""
    s = safe_numeric(series).dropna()
    if len(s) == 0:
        return "N/A"
    return round(s.mean(), digits)


def safe_median(series):
    s = safe_numeric(series).dropna()
    if len(s) == 0:
        return "N/A"
    return s.median()


def nps_score(series):
    """NPS from 0-10 scale. Returns (nps, promoters%, detractors%, n)."""
    s = safe_numeric(series).dropna()
    if len(s) == 0:
        return ("N/A", "N/A", "N/A", 0)
    n = len(s)
    promoters = (s >= 9).sum() / n * 100
    detractors = (s <= 6).sum() / n * 100
    return (round(promoters - detractors, 1),
            round(promoters, 1), round(detractors, 1), n)


def pct(n, total):
    if total == 0:
        return "N/A"
    return f"{n/total*100:.1f}%"


def vc(series, top=None, dropna_flag=True):
    """Value counts as formatted string lines."""
    s = series.dropna() if dropna_flag else series
    counts = s.value_counts()
    if top:
        counts = counts.head(top)
    total = s.shape[0] if dropna_flag else series.shape[0]
    lines = []
    for val, cnt in counts.items():
        lines.append(f"| {val} | {cnt:,} | {pct(cnt, total)} |")
    return lines


def md_table(headers, rows):
    """Build a Markdown table from headers list and row-lists."""
    sep = "| " + " | ".join(["---"] * len(headers)) + " |"
    hdr = "| " + " | ".join(str(h) for h in headers) + " |"
    lines = [hdr, sep]
    for row in rows:
        lines.append("| " + " | ".join(str(c) for c in row) + " |")
    return "\n".join(lines)


def vc_table(series, label="Value", top=None, show_pct=True):
    """Value counts as a markdown table."""
    s = series.dropna()
    if len(s) == 0:
        return "_No data available._\n"
    counts = s.value_counts()
    if top:
        counts = counts.head(top)
    total = len(s)
    headers = [label, "Count", "%"] if show_pct else [label, "Count"]
    rows = []
    for val, cnt in counts.items():
        row = [val, f"{cnt:,}", pct(cnt, total)] if show_pct else [val, f"{cnt:,}"]
        rows.append(row)
    return md_table(headers, rows)


def sat_row(label, series):
    """Return a table row [label, mean, median, dist_1, dist_2, ..., n]."""
    s = safe_numeric(series).dropna()
    if len(s) == 0:
        return [label, "N/A", "N/A", "", "", "", "", "", 0]
    dist = s.value_counts().reindex([1, 2, 3, 4, 5], fill_value=0)
    n = len(s)
    return [label,
            f"{s.mean():.2f}",
            f"{s.median():.0f}",
            f"{dist[1]:,} ({pct(dist[1], n)})",
            f"{dist[2]:,} ({pct(dist[2], n)})",
            f"{dist[3]:,} ({pct(dist[3], n)})",
            f"{dist[4]:,} ({pct(dist[4], n)})",
            f"{dist[5]:,} ({pct(dist[5], n)})",
            f"{n:,}"]


def wide_group_counts(wide_df, prefix, min_count=50):
    """Sum one-hot columns matching prefix, return sorted (reason, count, %)."""
    if wide_df is None:
        return []
    cols = [c for c in wide_df.columns if c.startswith(prefix)]
    if not cols:
        return []
    total_respondents = len(wide_df)
    results = []
    for col in cols:
        reason = col.split("__")[1] if "__" in col else col.replace(prefix, "")
        cnt = int(wide_df[col].sum())
        if cnt >= min_count:
            results.append((reason, cnt, pct(cnt, total_respondents)))
    results.sort(key=lambda x: x[1], reverse=True)
    return results


def long_question_counts(long_df, question_name, top=None):
    """From long-format, get answer value counts for a question."""
    if long_df is None:
        return []
    qdata = long_df[long_df["question"] == question_name]
    if len(qdata) == 0:
        return []
    n_respondents = qdata["recordID"].nunique()
    counts = qdata["answer"].value_counts()
    if top:
        counts = counts.head(top)
    results = []
    for ans, cnt in counts.items():
        results.append((ans, cnt, pct(cnt, n_respondents)))
    return results


def write_wide_or_long(out, title, results, col_label="Reason"):
    """Append a table of (reason, count, %) to output."""
    if not results:
        out.append(f"\n{title}\n\n_No data available._\n")
        return
    out.append(f"\n{title}\n")
    headers = [col_label, "Count", "% of respondents"]
    rows = [[r[0], f"{r[1]:,}", r[2]] for r in results]
    out.append(md_table(headers, rows))
    out.append("")


# ============================================================================
# LOADING
# ============================================================================

def load_all():
    print("Loading data...")
    sm = safe_load(SHORT_MAIN)
    sr = safe_load(SHORT_RARE)
    wm = safe_load(WIDE_MAIN)
    wr = safe_load(WIDE_RARE)
    lm = safe_load(LONG_MAIN)
    lr = safe_load(LONG_RARE)

    # Merge short main + rare on recordID
    if sm is not None and sr is not None:
        # Keep all columns from main, add non-overlapping from rare
        rare_cols = [c for c in sr.columns if c not in sm.columns or c == "recordID"]
        short = sm.merge(sr[rare_cols], on="recordID", how="left")
    else:
        short = sm

    # Concat wide
    if wm is not None and wr is not None:
        # Both share recordID and common cols; merge on recordID
        rare_wcols = [c for c in wr.columns if c not in wm.columns or c == "recordID"]
        wide = wm.merge(wr[rare_wcols], on="recordID", how="left")
    else:
        wide = wm if wm is not None else wr

    # Concat long
    longs = [x for x in [lm, lr] if x is not None]
    long = pd.concat(longs, ignore_index=True) if longs else None

    print(f"  short: {len(short) if short is not None else 0} rows")
    print(f"  wide:  {len(wide) if wide is not None else 0} rows")
    print(f"  long:  {len(long) if long is not None else 0} rows")
    return short, wide, long


# ============================================================================
# SECTION GENERATORS
# ============================================================================

def section_00_overview(short, wide, long):
    out = ["## 0. Dataset Overview\n"]
    n = len(short)
    out.append(f"- **Total respondents:** {n:,}")
    out.append(f"- **Date range:** {short['datetime'].min()} → {short['datetime'].max()}")
    weeks = sorted(short["weeknumber"].dropna().unique())
    out.append(f"- **Survey weeks:** {len(weeks)} weeks ({int(min(weeks))}–{int(max(weeks))})")
    n_cities = short["city"].nunique()
    out.append(f"- **Cities covered:** {n_cities}")

    # Cooperation type
    coop = safe_col(short, "cooperation_type").value_counts()
    for ct, cnt in coop.items():
        out.append(f"- **{ct} drivers:** {cnt:,} ({pct(cnt, n)})")

    # Multi-platform
    joint_signup = (safe_col(short, "joint_by_signup") == 1).sum()
    active_joint = (safe_col(short, "active_joint") == 1).sum()
    snapp_only = ((safe_col(short, "joint_by_signup") == 0)).sum()
    out.append(f"- **Registered on both platforms:** {joint_signup:,} ({pct(joint_signup, n)})")
    out.append(f"- **Active on both platforms:** {active_joint:,} ({pct(active_joint, n)})")
    out.append(f"- **Snapp-exclusive (never registered Tapsi):** {snapp_only:,} ({pct(snapp_only, n)})")

    # Weekly response counts
    out.append("\n### Responses per Week\n")
    wk = short.groupby("weeknumber").size().reset_index(name="n")
    headers = ["Week", "Responses"]
    rows = [[int(r["weeknumber"]), f"{r['n']:,}"] for _, r in wk.iterrows()]
    out.append(md_table(headers, rows))
    out.append("")
    return "\n".join(out)


def section_01_demographics(short):
    out = ["## 1. Driver Demographics\n"]

    out.append("### Gender\n")
    out.append(vc_table(safe_col(short, "gender"), "Gender"))

    out.append("\n### Age Distribution\n")
    out.append(vc_table(safe_col(short, "age"), "Age"))

    out.append("\n### Age Group\n")
    out.append(vc_table(safe_col(short, "age_group"), "Age Group"))

    out.append("\n### Education\n")
    out.append(vc_table(safe_col(short, "education"), "Education"))

    out.append("\n### Marital Status\n")
    out.append(vc_table(safe_col(short, "marital_status"), "Marital Status"))

    out.append("\n### Primary Occupation (Top 15)\n")
    out.append(vc_table(safe_col(short, "original_job"), "Occupation", top=15))

    out.append("\n### Active Time\n")
    out.append(vc_table(safe_col(short, "active_time"), "Active Time"))

    out.append("\n### Cooperation Type\n")
    out.append(vc_table(safe_col(short, "cooperation_type"), "Type"))

    out.append("\n### Top Cities\n")
    out.append(vc_table(safe_col(short, "city"), "City", top=TOP_N_CITIES))

    out.append("")
    return "\n".join(out)


def section_02_tenure(short):
    out = ["## 2. Platform Tenure\n"]

    out.append("### Snapp Tenure\n")
    out.append(vc_table(safe_col(short, "snapp_age"), "Tenure"))

    out.append("\n### Tapsi Tenure\n")
    out.append(vc_table(safe_col(short, "tapsi_age"), "Tenure"))

    out.append("")
    return "\n".join(out)


def section_03_multiplatform(short):
    out = ["## 3. Multi-Platform Behavior\n"]
    n = len(short)

    # Segments
    both_active = short[safe_col(short, "active_joint") == 1]
    snapp_only = short[safe_col(short, "joint_by_signup") == 0]
    reg_both_inactive_tapsi = short[(safe_col(short, "joint_by_signup") == 1) &
                                     (safe_col(short, "active_joint") == 0)]

    out.append(f"| Segment | Count | % |")
    out.append(f"| --- | --- | --- |")
    out.append(f"| Snapp-exclusive (never registered Tapsi) | {len(snapp_only):,} | {pct(len(snapp_only), n)} |")
    out.append(f"| Active on both | {len(both_active):,} | {pct(len(both_active), n)} |")
    out.append(f"| Registered both, inactive on Tapsi | {len(reg_both_inactive_tapsi):,} | {pct(len(reg_both_inactive_tapsi), n)} |")

    # Trip volume comparison
    out.append("\n### Average Weekly Trips\n")
    headers = ["Segment", "Snapp Trips (mean)", "Snapp Trips (median)", "Tapsi Trips (mean)", "Tapsi Trips (median)"]
    rows = []
    for label, subset in [("All drivers", short), ("Both active", both_active), ("Snapp-only", snapp_only)]:
        sr = safe_numeric(safe_col(subset, "snapp_ride")).dropna()
        tr = safe_numeric(safe_col(subset, "tapsi_ride")).dropna()
        rows.append([label,
                     f"{sr.mean():.1f}" if len(sr) else "N/A",
                     f"{sr.median():.1f}" if len(sr) else "N/A",
                     f"{tr.mean():.1f}" if len(tr) else "N/A",
                     f"{tr.median():.1f}" if len(tr) else "N/A"])
    out.append(md_table(headers, rows))

    # Satisfaction comparison by segment
    out.append("\n### Satisfaction by Multi-Platform Segment\n")
    headers = ["Segment", "Snapp Fare Sat", "Snapp Income Sat", "Snapp Req Sat", "n"]
    rows = []
    for label, subset in [("Both active", both_active), ("Snapp-only", snapp_only)]:
        rows.append([label,
                     safe_mean(safe_col(subset, "snapp_fare_satisfaction")),
                     safe_mean(safe_col(subset, "snapp_income_satisfaction")),
                     safe_mean(safe_col(subset, "snapp_req_count_satisfaction")),
                     f"{len(subset):,}"])
    out.append(md_table(headers, rows))

    # LOC
    out.append("\n### Length of Cooperation (LOC, months)\n")
    headers = ["Platform", "Mean LOC", "Median LOC"]
    for plat, col in [("Snapp", "snapp_LOC"), ("Tapsi", "tapsi_LOC")]:
        s = safe_numeric(safe_col(short, col)).dropna()
        if len(s):
            rows.append([plat, f"{s.mean():.1f}", f"{s.median():.1f}"])
    if rows:
        out.append(md_table(["Platform", "Mean LOC", "Median LOC"],
                            [[p, m, md] for p, m, md in rows[-2:]]))

    out.append("")
    return "\n".join(out)


def section_04_satisfaction(short):
    out = ["## 4. Core Satisfaction Scores (1-5 scale)\n"]

    # Main comparison table
    out.append("### Snapp vs Tapsi — Head-to-Head\n")
    headers = ["Metric", "Mean", "Median", "1/5", "2/5", "3/5", "4/5", "5/5", "n"]
    rows = [
        sat_row("Snapp Fare", safe_col(short, "snapp_fare_satisfaction")),
        sat_row("Tapsi Fare", safe_col(short, "tapsi_fare_satisfaction")),
        sat_row("Snapp Request Count", safe_col(short, "snapp_req_count_satisfaction")),
        sat_row("Tapsi Request Count", safe_col(short, "tapsi_req_count_satisfaction")),
        sat_row("Snapp Income", safe_col(short, "snapp_income_satisfaction")),
        sat_row("Tapsi Income", safe_col(short, "tapsi_income_satisfaction")),
    ]
    out.append(md_table(headers, rows))

    # Overall satisfaction
    out.append("\n### Overall Satisfaction\n")
    headers_os = ["Metric", "Mean", "n"]
    rows_os = []
    for label, col in [
        ("Snapp Overall Satisfaction", "snapp_overall_satisfaction_snapp"),
        ("Snapp Overall Satisfaction (rare)", "snapp_overall_satisfaction"),
        ("Tapsi Overall Satisfaction", "tapsi_overall_satisfaction"),
        ("Tapsi Overall Sat (Tapsi drivers)", "tapsi_overall_satisfaction_tapsi"),
        ("Snapp Overall Incentive Sat", "snapp_overall_incentive_satisfaction"),
    ]:
        s = safe_numeric(safe_col(short, col)).dropna()
        if len(s) > 0:
            rows_os.append([label, f"{s.mean():.2f}", f"{len(s):,}"])
    if rows_os:
        out.append(md_table(headers_os, rows_os))

    # By cooperation type
    out.append("\n### Satisfaction by Cooperation Type\n")
    headers_ct = ["Type", "Snapp Fare", "Snapp Req", "Snapp Income", "Avg Snapp Trips", "n"]
    rows_ct = []
    for ctype in ["Full-Time", "Part-Time"]:
        subset = short[safe_col(short, "cooperation_type") == ctype]
        if len(subset) == 0:
            continue
        rows_ct.append([ctype,
                        safe_mean(safe_col(subset, "snapp_fare_satisfaction")),
                        safe_mean(safe_col(subset, "snapp_req_count_satisfaction")),
                        safe_mean(safe_col(subset, "snapp_income_satisfaction")),
                        f"{safe_numeric(safe_col(subset, 'snapp_ride')).dropna().mean():.1f}" if len(safe_numeric(safe_col(subset, 'snapp_ride')).dropna()) else "N/A",
                        f"{len(subset):,}"])
    if rows_ct:
        out.append(md_table(headers_ct, rows_ct))

    # By age group
    out.append("\n### Satisfaction by Age Group\n")
    headers_ag = ["Age Group", "Snapp Fare", "Snapp Req", "Snapp Income", "n"]
    rows_ag = []
    for ag in ["18_to_35", "more_than_35"]:
        subset = short[safe_col(short, "age_group") == ag]
        if len(subset) == 0:
            continue
        rows_ag.append([ag,
                        safe_mean(safe_col(subset, "snapp_fare_satisfaction")),
                        safe_mean(safe_col(subset, "snapp_req_count_satisfaction")),
                        safe_mean(safe_col(subset, "snapp_income_satisfaction")),
                        f"{len(subset):,}"])
    if rows_ag:
        out.append(md_table(headers_ag, rows_ag))

    # By city (top 10)
    out.append("\n### Satisfaction by City (Top 10)\n")
    top_cities = safe_col(short, "city").value_counts().head(10).index
    headers_c = ["City", "Snapp Fare", "Snapp Req", "Snapp Income", "n"]
    rows_c = []
    for city in top_cities:
        subset = short[safe_col(short, "city") == city]
        rows_c.append([city,
                       safe_mean(safe_col(subset, "snapp_fare_satisfaction")),
                       safe_mean(safe_col(subset, "snapp_req_count_satisfaction")),
                       safe_mean(safe_col(subset, "snapp_income_satisfaction")),
                       f"{len(subset):,}"])
    if rows_c:
        out.append(md_table(headers_c, rows_c))

    # Weekly trend
    out.append("\n### Satisfaction Trend by Week\n")
    wk = short.groupby("weeknumber").agg(
        n=("recordID", "count"),
        snapp_fare=("snapp_fare_satisfaction", "mean"),
        snapp_req=("snapp_req_count_satisfaction", "mean"),
        snapp_income=("snapp_income_satisfaction", "mean"),
    ).reset_index()
    wk = wk[wk["n"] >= MIN_WEEK_RESPONSES]
    headers_w = ["Week", "n", "Snapp Fare", "Snapp Req", "Snapp Income"]
    rows_w = []
    for _, r in wk.iterrows():
        rows_w.append([int(r["weeknumber"]), f"{int(r['n']):,}",
                       f"{r['snapp_fare']:.2f}", f"{r['snapp_req']:.2f}",
                       f"{r['snapp_income']:.2f}"])
    if rows_w:
        out.append(md_table(headers_w, rows_w))

    out.append("")
    return "\n".join(out)


def section_05_nps(short):
    out = ["## 5. Net Promoter Score (NPS)\n"]

    nps_data = [
        ("Snapp (drivers rate Snapp)", "snapp_recommend"),
        ("Tapsi (Tapsi drivers rate Tapsi)", "tapsidriver_tapsi_recommend"),
        ("Tapsi (Snapp drivers rate Tapsi)", "snappdriver_tapsi_recommend"),
    ]
    headers = ["Platform", "NPS", "Promoters %", "Detractors %", "n"]
    rows = []
    for label, col in nps_data:
        nps, prom, det, n = nps_score(safe_col(short, col))
        if n > 0:
            rows.append([label, nps, prom, det, f"{n:,}"])
    if rows:
        out.append(md_table(headers, rows))

    # Snapp recommend distribution
    out.append("\n### Snapp Recommend Score Distribution (0-10)\n")
    s = safe_numeric(safe_col(short, "snapp_recommend")).dropna()
    if len(s) > 0:
        dist = s.value_counts().sort_index()
        headers_d = ["Score", "Count", "%"]
        rows_d = [[int(sc), f"{cnt:,}", pct(cnt, len(s))] for sc, cnt in dist.items()]
        out.append(md_table(headers_d, rows_d))

    out.append("")
    return "\n".join(out)


def section_06_incentives(short):
    out = ["## 6. Incentive Programs\n"]

    # Category distribution
    out.append("### Snapp Incentive Category\n")
    out.append(vc_table(safe_col(short, "snapp_incentive_category"), "Category"))

    out.append("\n### Tapsi Incentive Category\n")
    out.append(vc_table(safe_col(short, "tapsi_incentive_category"), "Category"))

    # Incentive satisfaction
    out.append("\n### Incentive Satisfaction\n")
    headers = ["Metric", "Mean", "Median", "1/5", "2/5", "3/5", "4/5", "5/5", "n"]
    rows = [
        sat_row("Snapp Incentive Sat", safe_col(short, "snapp_incentive_satisfaction")),
        sat_row("Tapsi Incentive Sat", safe_col(short, "tapsi_incentive_satisfaction")),
        sat_row("Snapp Overall Incentive Sat", safe_col(short, "snapp_overall_incentive_satisfaction")),
    ]
    out.append(md_table(headers, rows))

    # Incentive type
    out.append("\n### Snapp Incentive Type\n")
    out.append(vc_table(safe_col(short, "snapp_incentive_type"), "Type"))

    # Incentive length/duration
    out.append("\n### Snapp Incentive Duration\n")
    out.append(vc_table(safe_col(short, "snapp_incentive_length"), "Duration"))

    out.append("\n### Tapsi Incentive Duration\n")
    out.append(vc_table(safe_col(short, "tapsi_incentive_length"), "Duration"))

    # Incentive amount
    out.append("\n### Snapp Incentive Amount (Rial)\n")
    out.append(vc_table(safe_col(short, "snapp_incentive_rial_details"), "Amount", top=15))

    # Commission-free trips
    out.append("\n### Snapp Commission-Free Trip Count\n")
    out.append(vc_table(safe_col(short, "snapp_trip_count_commfree"), "Trip Range"))

    # Joining bonus
    out.append("\n### Snapp Joining Bonus\n")
    out.append(vc_table(safe_col(short, "snapp_joining_bonus"), "Received?"))

    # Got message about incentive
    out.append("\n### Received Incentive Message\n")
    headers_msg = ["Platform", "Yes", "No", "n"]
    rows_msg = []
    for label, col in [("Snapp", "snapp_gotmessage_incentive"),
                        ("Tapsi", "tapsi_gotmessage_text_incentive")]:
        s = safe_col(short, col).dropna()
        if len(s):
            y = (s == "Yes").sum()
            no = (s == "No").sum()
            rows_msg.append([label, f"{y:,} ({pct(y, len(s))})",
                             f"{no:,} ({pct(no, len(s))})", f"{len(s):,}"])
    if rows_msg:
        out.append(md_table(headers_msg, rows_msg))

    # Tapsi Magical Window
    out.append("\n### Tapsi Magical Window\n")
    out.append(vc_table(safe_col(short, "tapsi_magical_window"), "Status"))

    out.append("\n### Tapsi Magical Window Income\n")
    out.append(vc_table(safe_col(short, "tapsi_magical_window_income"), "Income Range"))

    out.append("")
    return "\n".join(out)


def section_07_incentive_issues(short, wide, long):
    out = ["## 7. Incentive Dissatisfaction & Bonuses\n"]

    # From wide: incentive unsatisfaction
    out.append("### Snapp Incentive Unsatisfaction Reasons\n")
    for prefix in ["Snapp Last Incentive Unsatisfaction__",
                   "Snapp Incentive Unsatisfaction__"]:
        results = wide_group_counts(wide, prefix, min_count=50)
        if results:
            out.append(f"\n_Prefix: `{prefix.rstrip('__')}`_\n")
            headers = ["Reason", "Count", "% of total"]
            rows = [[r[0], f"{r[1]:,}", r[2]] for r in results]
            out.append(md_table(headers, rows))
            out.append("")

    out.append("\n### Tapsi Incentive Unsatisfaction Reasons\n")
    for prefix in ["Tapsi Last Incentive Unsatisfaction__",
                   "Tapsi Incentive Unsatisfaction__"]:
        results = wide_group_counts(wide, prefix, min_count=50)
        if results:
            out.append(f"\n_Prefix: `{prefix.rstrip('__')}`_\n")
            headers = ["Reason", "Count", "% of total"]
            rows = [[r[0], f"{r[1]:,}", r[2]] for r in results]
            out.append(md_table(headers, rows))
            out.append("")

    # Incentive bonus received
    out.append("\n### Incentive Bonus Received\n")
    for prefix_label, prefix in [("Snapp", "Snapp Incentive GotBonus__"),
                                  ("Tapsi", "Tapsi Incentive GotBonus__")]:
        results = wide_group_counts(wide, prefix, min_count=0)
        if results:
            out.append(f"\n**{prefix_label}:**\n")
            headers = ["Bonus Type", "Count", "% of total"]
            rows = [[r[0], f"{r[1]:,}", r[2]] for r in results]
            out.append(md_table(headers, rows))
            out.append("")

    # Incentive types from wide
    out.append("\n### Incentive Types (from wide format)\n")
    for prefix_label, prefix in [("Snapp", "Snapp Incentive Type__"),
                                  ("Tapsi", "Tapsi Incentive Type__")]:
        results = wide_group_counts(wide, prefix, min_count=50)
        write_wide_or_long(out, f"**{prefix_label} Incentive Types:**", results, "Type")

    # Carpooling refusal
    out.append("\n### Tapsi Carpooling Refusal Reasons\n")
    results = wide_group_counts(wide, "Tapsi Carpooling refusal__", min_count=0)
    if results:
        headers = ["Reason", "Count", "% of total"]
        rows = [[r[0], f"{r[1]:,}", r[2]] for r in results]
        out.append(md_table(headers, rows))

    out.append("")
    return "\n".join(out)


def section_08_refusal(short, wide, long):
    out = ["## 8. Ride Refusal Reasons\n"]

    # From wide_rare
    for title, prefix in [
        ("### Snapp Request Refusal", "Snapp Request Refusal__"),
        ("### Snapp Ride Refusal", "Snapp Ride Refusal Reasons__"),
        ("### Tapsi Request Refusal", "Tapsi Request Refusal__"),
        ("### Tapsi Ride Refusal", "Tapsi Ride Refusal Reasons__"),
    ]:
        results = wide_group_counts(wide, prefix, min_count=50)
        write_wide_or_long(out, title, results)

    # Decline reasons
    out.append("\n### General Decline Reasons\n")
    results = wide_group_counts(wide, "Decline Reason__", min_count=50)
    if results:
        headers = ["Reason", "Count", "% of total"]
        rows = [[r[0], f"{r[1]:,}", r[2]] for r in results]
        out.append(md_table(headers, rows))

    out.append("")
    return "\n".join(out)


def section_09_customer_support(short, wide):
    out = ["## 9. Customer Support\n"]

    # CS Satisfaction
    out.append("### CS Satisfaction Comparison\n")
    headers = ["Metric", "Snapp", "Tapsi"]
    rows = []
    for suffix in ["overall", "waittime", "solution", "behaviour", "relevance"]:
        s_col = f"snapp_CS_satisfaction_{suffix}"
        t_col = f"tapsi_CS_satisfaction_{suffix}"
        rows.append([suffix.capitalize(),
                     safe_mean(safe_col(short, s_col)),
                     safe_mean(safe_col(short, t_col))])
    out.append(md_table(headers, rows))

    # CS Resolved
    out.append("\n### CS Issue Resolved\n")
    out.append("**Snapp:**\n")
    out.append(vc_table(safe_col(short, "snapp_CS_solved"), "Resolved?"))
    out.append("\n**Tapsi:**\n")
    out.append(vc_table(safe_col(short, "tapsi_CS_solved"), "Resolved?"))

    # CS Categories from wide
    out.append("\n### Snapp CS Contact Categories\n")
    results = wide_group_counts(wide, "Snapp Customer Support Category__", min_count=50)
    if results:
        headers = ["Category", "Count", "% of total"]
        rows = [[r[0], f"{r[1]:,}", r[2]] for r in results]
        out.append(md_table(headers, rows))

    out.append("\n### Tapsi CS Contact Categories\n")
    results = wide_group_counts(wide, "Tapsi Customer Support Category__", min_count=50)
    if results:
        headers = ["Category", "Count", "% of total"]
        rows = [[r[0], f"{r[1]:,}", r[2]] for r in results]
        out.append(md_table(headers, rows))

    # CS important reason
    out.append("\n### Most Important CS Factor\n")
    out.append("**Snapp:**\n")
    out.append(vc_table(safe_col(short, "snapp_CS_satisfaction_important_reason"), "Factor", top=10))
    out.append("\n**Tapsi:**\n")
    out.append(vc_table(safe_col(short, "tapsi_CS_satisfaction_important_reason"), "Factor", top=10))

    out.append("")
    return "\n".join(out)


def section_10_navigation(short, wide):
    out = ["## 10. Navigation\n"]

    out.append("### Last Trip Navigation App (Snapp)\n")
    out.append(vc_table(safe_col(short, "snapp_last_trip_navigation"), "App"))

    out.append("\n### Tapsi Navigation Type\n")
    out.append(vc_table(safe_col(short, "tapsi_navigation_type"), "Type"))

    # Navigation satisfaction
    out.append("\n### Navigation Satisfaction\n")
    for label, col in [
        ("Snapp Navigation App Sat", "snapp_navigation_app_satisfaction"),
        ("Tapsi In-App Navigation Sat", "tapsi_in_app_navigation_satisfaction"),
    ]:
        s = safe_numeric(safe_col(short, col)).dropna()
        if len(s):
            out.append(f"- **{label}:** mean={s.mean():.2f}, n={len(s):,}")

    # Navigation recommendations (1-10)
    out.append("\n### Navigation App Recommendation Scores\n")
    headers = ["App", "Mean Score", "n"]
    rows = []
    for app in ["googlemap", "waze", "neshan", "balad"]:
        s = safe_numeric(safe_col(short, f"recommendation_{app}")).dropna()
        if len(s):
            rows.append([app.capitalize(), f"{s.mean():.2f}", f"{len(s):,}"])
    if rows:
        out.append(md_table(headers, rows))

    # Navigation refusal/unsatisfaction from wide
    out.append("\n### Snapp Navigation Refusal Reasons\n")
    results = wide_group_counts(wide, "Snapp Navigation Refusal__", min_count=50)
    if results:
        headers = ["Reason", "Count", "% of total"]
        rows = [[r[0], f"{r[1]:,}", r[2]] for r in results]
        out.append(md_table(headers, rows))

    out.append("\n### Snapp Navigation Unsatisfaction Reasons\n")
    results = wide_group_counts(wide, "Snapp Navigation Unsatisfaction__", min_count=50)
    if results:
        headers = ["Reason", "Count", "% of total"]
        rows = [[r[0], f"{r[1]:,}", r[2]] for r in results]
        out.append(md_table(headers, rows))

    out.append("")
    return "\n".join(out)


def section_11_gps(short, wide):
    out = ["## 11. GPS Issues\n"]

    out.append("### GPS Problem Frequency\n")
    out.append(vc_table(safe_col(short, "gps_problem"), "Frequency"))

    out.append("\n### GPS Glitch Timing\n")
    results = wide_group_counts(wide, "GPS Glitch Time__", min_count=50)
    if results:
        headers = ["Time Slot", "Count", "% of total"]
        rows = [[r[0], f"{r[1]:,}", r[2]] for r in results]
        out.append(md_table(headers, rows))

    out.append("\n### Driver Action During GPS Glitch\n")
    results = wide_group_counts(wide, "GPS Action when Glitch__", min_count=50)
    if results:
        headers = ["Action", "Count", "% of total"]
        rows = [[r[0], f"{r[1]:,}", r[2]] for r in results]
        out.append(md_table(headers, rows))

    out.append("\n### Fix Location Feature\n")
    for label, col in [("Familiar", "fixlocation_familiar"),
                        ("Used", "fixlocation_use"),
                        ("Satisfaction", "fixlocation_satisfaction")]:
        s = safe_col(short, col).dropna()
        if len(s):
            out.append(f"- **{label}:** {s.value_counts().to_dict()} (n={len(s):,})")

    out.append("\n### Tapsi GPS Better?\n")
    out.append(vc_table(safe_col(short, "tapsi_gps_better"), "Response"))

    out.append("")
    return "\n".join(out)


def section_12_registration(short):
    out = ["## 12. Registration & Collaboration Reasons\n"]

    out.append("### Why Drivers Stay with Snapp (Collaboration Reason)\n")
    out.append(vc_table(safe_col(short, "snapp_collab_reason"), "Reason"))

    out.append("\n### Why Drivers Use Tapsi (Collaboration Reason)\n")
    out.append(vc_table(safe_col(short, "tapsi_collab_reason"), "Reason"))

    out.append("\n### Snapp Registration Motivation\n")
    out.append(vc_table(safe_col(short, "snapp_registration_motivation"), "Motivation"))

    out.append("\n### Tapsi Registration Motivation\n")
    out.append(vc_table(safe_col(short, "tapsi_registration_motivation"), "Motivation"))

    out.append("\n### Tapsi Invite to Register\n")
    out.append(vc_table(safe_col(short, "tapsi_invite_to_reg"), "Status"))

    out.append("\n### Referral Behavior\n")
    out.append("**Snapp Refer Others:**\n")
    out.append(vc_table(safe_col(short, "snapp_refer_others"), "Response"))
    out.append("\n**Tapsi Refer Others:**\n")
    out.append(vc_table(safe_col(short, "tapsi_refer_others"), "Response"))

    out.append("")
    return "\n".join(out)


def section_13_commission(short):
    out = ["## 13. Commission & Tax Transparency\n"]

    out.append("### Snapp Commission Knowledge\n")
    out.append(vc_table(safe_col(short, "snapp_comm_info"), "Response", top=10))

    out.append("\n### Tapsi Commission Knowledge\n")
    out.append(vc_table(safe_col(short, "tapsi_comm_info"), "Response", top=10))

    out.append("\n### Snapp Tax Knowledge\n")
    out.append(vc_table(safe_col(short, "snapp_tax_info"), "Response", top=10))

    out.append("\n### Tapsi Tax Knowledge\n")
    out.append(vc_table(safe_col(short, "tapsi_tax_info"), "Response", top=10))

    out.append("")
    return "\n".join(out)


def section_14_unpaid(short):
    out = ["## 14. Unpaid Fares\n"]

    out.append("### Unpaid by Passenger\n")
    out.append(vc_table(safe_col(short, "unpaid_by_passenger"), "Status"))

    out.append("\n### Snapp Compensation for Unpaid\n")
    out.append(vc_table(safe_col(short, "snapp_compensate_unpaid_by_passenger"), "Status"))

    out.append("\n### Follow-Up Satisfaction\n")
    for label, col in [
        ("Snapp Follow-Up Overall", "snapp_satisfaction_followup_overall"),
        ("Snapp Follow-Up Time", "snapp_satisfaction_followup_time"),
        ("Tapsi Follow-Up Overall", "tapsi_satisfaction_followup_overall"),
        ("Tapsi Follow-Up Time", "tapsi_satisfaction_followup_time"),
    ]:
        s = safe_numeric(safe_col(short, col)).dropna()
        if len(s):
            out.append(f"- **{label}:** mean={s.mean():.2f}, n={len(s):,}")

    out.append("")
    return "\n".join(out)


def section_15_ecosystem(short, wide):
    out = ["## 15. Ecosystem & App Features\n"]

    # Driver app features
    out.append("### Driver App Feature Awareness\n")
    for label, col in [
        ("App 'Off' Mode Familiar", "driverapp_off_familiar"),
        ("Campaign Familiar", "driverapp_campagin_familiar"),
        ("Feature Use", "driverapp_feature_use"),
    ]:
        s = safe_col(short, col).dropna()
        if len(s):
            y = (s == "Yes").sum()
            no = (s == "No").sum()
            out.append(f"- **{label}:** Yes={y:,} ({pct(y, len(s))}), No={no:,}, n={len(s):,}")

    # Ecosystem app usage from wide
    out.append("\n### Snapp Ecosystem App Usage\n")
    results = wide_group_counts(wide, "Snapp Usage app__", min_count=50)
    if results:
        headers = ["App", "Count", "% of total"]
        rows = [[r[0], f"{r[1]:,}", r[2]] for r in results]
        out.append(md_table(headers, rows))

    out.append("\n### SnappDriver App Menu Usage\n")
    results = wide_group_counts(wide, "SnappDriver App Menu__", min_count=50)
    if results:
        headers = ["Menu Item", "Count", "% of total"]
        rows = [[r[0], f"{r[1]:,}", r[2]] for r in results]
        out.append(md_table(headers, rows))

    # Communication channels
    out.append("\n### Communication Channel Satisfaction\n")
    headers = ["Channel", "Mean Sat", "n"]
    rows = []
    for ch_label, col in [
        ("SMS", "gotmessage_satisfaction_sms"),
        ("Call", "gotmessage_satisfaction_call"),
        ("App", "gotmessage_satisfaction_app"),
        ("Telegram", "gotmessage_satisfaction_telegram"),
        ("Instagram", "gotmessage_satisfaction_instagram"),
    ]:
        s = safe_numeric(safe_col(short, col)).dropna()
        if len(s) > 50:
            rows.append([ch_label, f"{s.mean():.2f}", f"{len(s):,}"])
    if rows:
        out.append(md_table(headers, rows))

    # Message types from wide
    out.append("\n### Snapp Message Types Received\n")
    results = wide_group_counts(wide, "Snapp Got Message Type__", min_count=50)
    if results:
        headers = ["Type", "Count", "% of total"]
        rows = [[r[0], f"{r[1]:,}", r[2]] for r in results]
        out.append(md_table(headers, rows))

    # Broadcast channels
    out.append("\n### Snapp Broadcast Channels\n")
    results = wide_group_counts(wide, "Snapp Driversapp Broadcast Channel__", min_count=50)
    if results:
        headers = ["Channel", "Count", "% of total"]
        rows = [[r[0], f"{r[1]:,}", r[2]] for r in results]
        out.append(md_table(headers, rows))

    out.append("")
    return "\n".join(out)


def section_16_carpooling(short, wide):
    out = ["## 16. Carpooling\n"]

    out.append("### Tapsi Carpooling Familiarity\n")
    out.append(vc_table(safe_col(short, "tapsi_carpooling_familiar"), "Response"))

    out.append("\n### Tapsi Carpooling Satisfaction\n")
    s = safe_numeric(safe_col(short, "tapsi_carpooling_satisfaction")).dropna()
    if len(s):
        out.append(f"- Mean: {s.mean():.2f}, n={len(s):,}")

    # 15% refusal
    out.append("\n### Carpooling 15% Refusal Reasons\n")
    results = wide_group_counts(wide, "Carpooling 15% Refusal__", min_count=0)
    if results:
        headers = ["Reason", "Count", "% of total"]
        rows = [[r[0], f"{r[1]:,}", r[2]] for r in results]
        out.append(md_table(headers, rows))

    out.append("")
    return "\n".join(out)


def section_17_ecoplus(short, wide):
    out = ["## 17. EcoPlus & Intercity\n"]

    out.append("### Snapp EcoPlus Familiarity\n")
    out.append(vc_table(safe_col(short, "snapp_ecoplus_familiar"), "Response"))

    out.append("\n### Snapp EcoPlus Accepted\n")
    out.append(vc_table(safe_col(short, "snapp_ecoplus_accepted"), "Response"))

    # EcoPlus refusal
    out.append("\n### EcoPlus Refusal Reasons\n")
    results = wide_group_counts(wide, "Snapp Ecoplus Refusal__", min_count=50)
    if results:
        headers = ["Reason", "Count", "% of total"]
        rows = [[r[0], f"{r[1]:,}", r[2]] for r in results]
        out.append(md_table(headers, rows))

    # Intercity
    out.append("\n### Intercity Familiarity\n")
    for label, col in [("Snapp Intercity", "intercitysnapp_familiar"),
                        ("Tapsi Intercity", "intercitytapsi_familiar")]:
        s = safe_col(short, col).dropna()
        if len(s):
            y = (s == "Yes").sum()
            out.append(f"- **{label}:** Yes={y:,} ({pct(y, len(s))}), n={len(s):,}")

    out.append("")
    return "\n".join(out)


def section_18_carfix(short):
    out = ["## 18. SnappCarFix vs TapsiGarage\n"]

    # Awareness funnel
    out.append("### Awareness & Usage Funnel\n")
    headers = ["Metric", "SnappCarFix", "TapsiGarage"]
    metrics = [
        ("Familiar", "snappcarfix_familiar", "tapsigarage_familiar"),
        ("Ever Used", "snappcarfix_use_ever", "tapsigarage_use_ever"),
        ("Used Last Month", "snappcarfix_use_lastmo", "tapsigarage_use_lastmo"),
    ]
    rows = []
    for label, sc, tc in metrics:
        s_yes = (safe_col(short, sc).dropna() == "Yes").sum()
        t_yes = (safe_col(short, tc).dropna() == "Yes").sum()
        rows.append([label, f"{s_yes:,}", f"{t_yes:,}"])
    out.append(md_table(headers, rows))

    # Satisfaction sub-metrics
    out.append("\n### SnappCarFix Satisfaction\n")
    headers_s = ["Metric", "Mean", "n"]
    rows_s = []
    for suffix in ["experience", "productprice", "quality", "variety",
                    "buyingprocess", "deliverytime", "waittime", "behaviour"]:
        col = f"snappcarfix_satisfaction_{suffix}"
        s = safe_numeric(safe_col(short, col)).dropna()
        if len(s) > 0:
            rows_s.append([suffix.capitalize(), f"{s.mean():.2f}", f"{len(s):,}"])
    if rows_s:
        out.append(md_table(headers_s, rows_s))

    # Recommend
    for label, col in [("SnappCarFix Recommend", "snappcarfix_recommend"),
                        ("TapsiGarage Recommend", "tapsigarage_recommend")]:
        s = safe_numeric(safe_col(short, col)).dropna()
        if len(s):
            out.append(f"\n- **{label}:** mean={s.mean():.2f}, n={len(s):,}")

    # Not-use reasons
    out.append("\n### SnappCarFix Not-Use Reasons\n")
    results = wide_group_counts(pd.DataFrame(), "Snappcarfix NotUse Reason__", min_count=0)
    # This needs the wide df — handled below

    out.append("")
    return "\n".join(out)


def section_18_carfix_wide(short, wide):
    """Extended version that also uses wide for not-use reasons."""
    base = section_18_carfix(short)
    out = [base]

    results = wide_group_counts(wide, "Snappcarfix NotUse Reason__", min_count=0)
    if results:
        out.append("\n### SnappCarFix Not-Use Reasons (from wide)\n")
        headers = ["Reason", "Count", "% of total"]
        rows = [[r[0], f"{r[1]:,}", r[2]] for r in results]
        out.append(md_table(headers, rows))

    return "\n".join(out)


def section_19_mixed_incentive(short):
    out = ["## 19. Mixed Incentive Strategy\n"]

    for label, col in [
        ("Mix Incentive", "mixincentive"),
        ("Mix Incentive Activate Familiar", "mixincentive_activate_familiar"),
        ("Mix Incentive Trip Effect", "mixincentive_tripeffect"),
        ("Mix Incentive Only Snapp", "mixincentive_onlysnapp"),
        ("Mix Incentive Choice", "mixincentive_choice"),
        ("Incentive Preference", "incentive_preference"),
        ("Incentive Rules", "incentive_rules"),
    ]:
        s = safe_col(short, col).dropna()
        if len(s) > 50:
            out.append(f"\n### {label}\n")
            out.append(vc_table(s, "Response", top=10))

    out.append("")
    return "\n".join(out)


def section_20_speed_intent(short):
    out = ["## 20. Speed Satisfaction & Intent\n"]

    out.append("### Speed Satisfaction\n")
    headers = ["Platform", "Mean", "n"]
    rows = []
    for label, col in [("Snapp", "snapp_speed_satisfaction"),
                        ("Tapsi", "tapsi_speed_satisfaction")]:
        s = safe_numeric(safe_col(short, col)).dropna()
        if len(s):
            rows.append([label, f"{s.mean():.2f}", f"{len(s):,}"])
    if rows:
        out.append(md_table(headers, rows))

    out.append("\n### Snapp Next-Week Usage Intent\n")
    out.append(vc_table(safe_col(short, "snapp_use_nextweek"), "Intent"))

    out.append("\n### Better Income Perception\n")
    for label, col in [("Snapp Better Income", "snapp_better_income"),
                        ("Tapsi Better Income", "tapsi_better_income")]:
        s = safe_col(short, col).dropna()
        if len(s) > 0:
            out.append(f"\n**{label}:**\n")
            out.append(vc_table(s, "Response", top=10))

    out.append("")
    return "\n".join(out)


def section_21_trends(short):
    out = ["## 21. Weekly Trends Summary\n"]

    # Build weekly aggregation
    wk = short.groupby("weeknumber").agg(
        n=("recordID", "count"),
        joint_rate=("active_joint", "mean"),
        snapp_fare=("snapp_fare_satisfaction", "mean"),
        snapp_req=("snapp_req_count_satisfaction", "mean"),
        snapp_income=("snapp_income_satisfaction", "mean"),
        avg_snapp_rides=("snapp_ride", "mean"),
    ).reset_index()
    wk = wk[wk["n"] >= MIN_WEEK_RESPONSES]

    # Add Tapsi means where available (only for joint drivers)
    joint = short[safe_col(short, "active_joint") == 1]
    if len(joint) > 0:
        twk = joint.groupby("weeknumber").agg(
            tapsi_fare=("tapsi_fare_satisfaction", "mean"),
            tapsi_income=("tapsi_income_satisfaction", "mean"),
            avg_tapsi_rides=("tapsi_ride", "mean"),
        ).reset_index()
        wk = wk.merge(twk, on="weeknumber", how="left")

    headers = ["Week", "n", "Joint%", "S.Fare", "S.Req", "S.Income",
               "S.Rides"]
    extra_h = []
    if "tapsi_fare" in wk.columns:
        extra_h = ["T.Fare", "T.Income", "T.Rides"]
    headers += extra_h

    rows = []
    for _, r in wk.iterrows():
        row = [int(r["weeknumber"]), f"{int(r['n']):,}",
               f"{r['joint_rate']*100:.1f}%",
               f"{r['snapp_fare']:.2f}", f"{r['snapp_req']:.2f}",
               f"{r['snapp_income']:.2f}", f"{r['avg_snapp_rides']:.1f}"]
        if "tapsi_fare" in wk.columns:
            row += [f"{r.get('tapsi_fare', 0):.2f}" if pd.notna(r.get('tapsi_fare')) else "--",
                    f"{r.get('tapsi_income', 0):.2f}" if pd.notna(r.get('tapsi_income')) else "--",
                    f"{r.get('avg_tapsi_rides', 0):.1f}" if pd.notna(r.get('avg_tapsi_rides')) else "--"]
        rows.append(row)
    if rows:
        out.append(md_table(headers + extra_h if not extra_h else headers, rows))

    # Latest vs previous delta
    if len(wk) >= 2:
        latest = wk.iloc[-1]
        prev = wk.iloc[-2]
        out.append(f"\n### Latest Week ({int(latest['weeknumber'])}) vs Previous ({int(prev['weeknumber'])})\n")
        for metric in ["snapp_fare", "snapp_req", "snapp_income"]:
            delta = latest[metric] - prev[metric]
            direction = "+" if delta > 0 else ""
            out.append(f"- **{metric}:** {direction}{delta:.3f}")

    out.append("")
    return "\n".join(out)


# ============================================================================
# MAIN
# ============================================================================

def main():
    short, wide, long = load_all()
    if short is None:
        print("ERROR: Could not load short_survey_main.csv. Aborting.")
        return

    print("Generating metrics summary...")
    sections = []

    # Header
    sections.append(f"# Driver Survey — Metrics Summary\n")
    sections.append(f"_Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}_\n")
    sections.append(f"_This document contains raw metrics and comparisons. "
                    f"Paste it to an LLM for strategic interpretation._\n")
    sections.append("---\n")

    # Generate each section
    sections.append(section_00_overview(short, wide, long))
    sections.append(section_01_demographics(short))
    sections.append(section_02_tenure(short))
    sections.append(section_03_multiplatform(short))
    sections.append(section_04_satisfaction(short))
    sections.append(section_05_nps(short))
    sections.append(section_06_incentives(short))
    sections.append(section_07_incentive_issues(short, wide, long))
    sections.append(section_08_refusal(short, wide, long))
    sections.append(section_09_customer_support(short, wide))
    sections.append(section_10_navigation(short, wide))
    sections.append(section_11_gps(short, wide))
    sections.append(section_12_registration(short))
    sections.append(section_13_commission(short))
    sections.append(section_14_unpaid(short))
    sections.append(section_15_ecosystem(short, wide))
    sections.append(section_16_carpooling(short, wide))
    sections.append(section_17_ecoplus(short, wide))
    sections.append(section_18_carfix_wide(short, wide))
    sections.append(section_19_mixed_incentive(short))
    sections.append(section_20_speed_intent(short))
    sections.append(section_21_trends(short))

    # Write output
    output_text = "\n".join(sections)
    with open(OUTPUT, "w", encoding="utf-8") as f:
        f.write(output_text)

    print(f"\nDone! Output written to: {OUTPUT}")
    print(f"File size: {os.path.getsize(OUTPUT) / 1024:.1f} KB")


if __name__ == "__main__":
    main()
