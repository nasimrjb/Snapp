"""
Driver Survey – Visual Analysis v3
====================================
Generates a multi-page PDF report from short_survey.csv, wide_survey.csv,
and long_survey.csv.

Changes from v2:
  - Uses format="mixed" for datetime parsing (handles both M/D/YYYY and ISO)
  - Filters out records where snapp_age is null/blank
  - Integrates long_survey for multi-choice visualizations (Pages 25-28)
  - long_survey is filtered to only include recordIDs surviving the short filter

Usage:
    python survey_analysis_v3.py
"""

from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.ticker as mticker
import matplotlib.pyplot as plt
import os
import warnings
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")

warnings.filterwarnings("ignore")

# ── Configuration ────────────────────────────────────────────────────

SHORT_PATH = r"D:\Work\Driver Survey\processed\short_survey.csv"
WIDE_PATH = r"D:\Work\Driver Survey\processed\wide_survey.csv"
LONG_PATH = r"D:\Work\Driver Survey\processed\long_survey.csv"
OUTPUT_PDF = r"D:\Work\Driver Survey\processed\driver_survey_analysis.pdf"

SNAPP_COLOR = "#00C853"
TAPSI_COLOR = "#FF6D00"
ACCENT = "#1565C0"
GREY = "#9E9E9E"
BG_COLOR = "#FAFAFA"

PLATFORM_COLORS = {"Snapp": SNAPP_COLOR, "Tapsi": TAPSI_COLOR}

MIN_WEEK_RESPONSES = 100

# ── Load data ────────────────────────────────────────────────────────

short = pd.read_csv(SHORT_PATH, encoding="utf-8-sig")
wide = pd.read_csv(WIDE_PATH,  encoding="utf-8-sig")
long = pd.read_csv(LONG_PATH,  encoding="utf-8-sig")

# ── Filter: remove records without snapp_age ─────────────────────────

before = len(short)
short = short[short["snapp_age"].notna() & (short["snapp_age"] != "")].copy()
dropped_age = before - len(short)
if dropped_age > 0:
    print(f"Dropped {dropped_age} records with missing snapp_age")

# Sync wide and long to only include surviving recordIDs
valid_ids = set(short["recordID"].unique())
wide = wide[wide["recordID"].isin(valid_ids)].copy()
long = long[long["recordID"].isin(valid_ids)].copy()

# ── Compute yearweek ────────────────────────────────────────────────

short["datetime_parsed"] = pd.to_datetime(short["datetime"], format="mixed")
short["year"] = short["datetime_parsed"].dt.year
short["yearweek"] = (short["year"] % 100) * 100 + \
    short["weeknumber"].astype(int)

wide["datetime_parsed"] = pd.to_datetime(wide["datetime"], format="mixed")
wide["year"] = wide["datetime_parsed"].dt.year
wide["yearweek"] = (wide["year"] % 100) * 100 + wide["weeknumber"].astype(int)

long["datetime_parsed"] = pd.to_datetime(long["datetime"], format="mixed")
long["year"] = long["datetime_parsed"].dt.year
long["yearweek"] = (long["year"] % 100) * 100 + long["weeknumber"].astype(int)

# ── Filter weeks with < 100 responses ───────────────────────────────

week_counts_all = short.groupby("yearweek").size()
valid_weeks = week_counts_all[week_counts_all >= MIN_WEEK_RESPONSES].index
dropped_weeks = week_counts_all[week_counts_all < MIN_WEEK_RESPONSES]
if len(dropped_weeks) > 0:
    print(
        f"Dropping {len(dropped_weeks)} week(s) with < {MIN_WEEK_RESPONSES} responses:")
    for yw, cnt in dropped_weeks.items():
        print(f"  yearweek {yw}: {cnt} responses")

short = short[short["yearweek"].isin(valid_weeks)].copy()
wide = wide[wide["yearweek"].isin(valid_weeks)].copy()
long = long[long["yearweek"].isin(valid_weeks)].copy()
print(f"Remaining: {len(short)} short rows, {len(wide)} wide rows, "
      f"{len(long)} long rows across {len(valid_weeks)} weeks")

# ── Compute driver_type ─────────────────────────────────────────────

short["driver_type"] = np.where(
    short["tapsi_ride"] == 0, "Snapp Exclusive", "Joint")
wide["driver_type"] = np.where(
    wide["tapsi_ride"] == 0, "Snapp Exclusive", "Joint")
long["driver_type"] = np.where(
    long["tapsi_ride"] == 0, "Snapp Exclusive", "Joint")

# Sort yearweek for consistent x-axes
short = short.sort_values("yearweek")
wide = wide.sort_values("yearweek")
long = long.sort_values("yearweek")


# ── Satisfaction columns of interest ─────────────────────────────────

SAT_PAIRS = [
    ("snapp_fare_satisfaction",          "tapsi_fare_satisfaction",          "Fare"),
    ("snapp_income_satisfaction",
     "tapsi_income_satisfaction",        "Income"),
    ("snapp_req_count_satisfaction",
     "tapsi_req_count_satisfaction",     "Request Count"),
]

SAT_COLS_SNAPP = [s for s, _, _ in SAT_PAIRS]
SAT_COLS_TAPSI = [t for _, t, _ in SAT_PAIRS]
SAT_LABELS = [l for _, _, l in SAT_PAIRS]


# ── Helpers ──────────────────────────────────────────────────────────

def new_fig(title, figsize=(12, 6)):
    fig, ax = plt.subplots(figsize=figsize, facecolor=BG_COLOR)
    ax.set_facecolor(BG_COLOR)
    fig.suptitle(title, fontsize=15, fontweight="bold", y=0.97)
    return fig, ax


def bar_label(ax, fmt="{:.0f}"):
    for container in ax.containers:
        labels = [fmt.format(v.get_height()) if v.get_height() > 0 else ""
                  for v in container]
        ax.bar_label(container, labels=labels, fontsize=8, padding=2)


def save_fig(pdf, fig):
    fig.tight_layout(rect=[0, 0, 1, 0.94])
    pdf.savefig(fig, facecolor=BG_COLOR)
    plt.close(fig)


def nps_score(series):
    s = series.dropna()
    if len(s) == 0:
        return np.nan
    promoters = (s >= 9).sum() / len(s) * 100
    detractors = (s <= 6).sum() / len(s) * 100
    return promoters - detractors


def plot_sat_by_group(pdf, df, groupcol, title_suffix, figsize=(14, 6),
                      top_n=None, min_group_size=10):
    """
    Plot average of Fare/Income/ReqCount satisfaction per group,
    comparing Snapp vs Tapsi, one subplot per satisfaction type.
    """
    grp_sizes = df.groupby(groupcol).size()
    valid_groups = grp_sizes[grp_sizes >= min_group_size].index
    df_f = df[df[groupcol].isin(valid_groups)]

    if top_n:
        top_groups = df_f[groupcol].value_counts().head(top_n).index
        df_f = df_f[df_f[groupcol].isin(top_groups)]

    groups = sorted(df_f[groupcol].dropna().unique(), key=str)
    if len(groups) == 0:
        return

    fig, axes = plt.subplots(1, 3, figsize=figsize,
                             facecolor=BG_COLOR, sharey=True)
    fig.suptitle(f"Avg Satisfaction by {title_suffix}",
                 fontsize=15, fontweight="bold", y=0.99)

    x = np.arange(len(groups))
    w = 0.35

    for ax, (scol, tcol, label) in zip(axes, SAT_PAIRS):
        grp = df_f.groupby(groupcol).agg(
            snapp=(scol, "mean"),
            tapsi=(tcol, "mean")
        ).reindex(groups)

        bars_s = ax.bar(x - w/2, grp["snapp"], w,
                        color=SNAPP_COLOR, label="Snapp")
        bars_t = ax.bar(x + w/2, grp["tapsi"], w,
                        color=TAPSI_COLOR, label="Tapsi")

        for bar in bars_s:
            if not np.isnan(bar.get_height()):
                ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.05,
                        f"{bar.get_height():.2f}", ha="center", fontsize=7, fontweight="bold")
        for bar in bars_t:
            if not np.isnan(bar.get_height()):
                ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.05,
                        f"{bar.get_height():.2f}", ha="center", fontsize=7, fontweight="bold")

        ax.set_xticks(x)
        ax.set_xticklabels([str(g) for g in groups], fontsize=8,
                           rotation=45 if len(groups) > 5 else 0,
                           ha="right" if len(groups) > 5 else "center")
        ax.set_title(label, fontsize=11)
        ax.set_ylim(0, 5.5)
        ax.legend(frameon=False, fontsize=8)
        ax.set_facecolor(BG_COLOR)
        ax.spines[["top", "right"]].set_visible(False)

    axes[0].set_ylabel("Mean Satisfaction (1–5)")
    save_fig(pdf, fig)


def plot_long_multichoice(pdf, long_df, question_name, title,
                          figsize=(12, 6), color=ACCENT, by_group=None):
    """
    Plot multi-choice answers from long_survey for a given question.
    If by_group is provided, shows grouped bars (e.g. by yearweek or driver_type).
    """
    qdata = long_df[long_df["question"] == question_name].copy()
    if len(qdata) == 0:
        return

    if by_group is None:
        # Simple bar chart of answer counts
        answer_counts = qdata["answer"].value_counts(
        ).sort_values(ascending=True)
        fig, ax = new_fig(title, figsize=figsize)
        ax.barh(answer_counts.index, answer_counts.values,
                color=color, edgecolor="white")
        total = answer_counts.sum()
        for i, (ans, v) in enumerate(answer_counts.items()):
            ax.text(v + 1, i, f"{v} ({v/total*100:.0f}%)",
                    va="center", fontsize=9)
        ax.set_xlabel("Count")
        ax.spines[["top", "right"]].set_visible(False)
        save_fig(pdf, fig)
    else:
        # Grouped bar chart
        pivot = qdata.groupby([by_group, "answer"]
                              ).size().unstack(fill_value=0)
        answers = pivot.columns.tolist()
        groups = pivot.index.tolist()

        fig, ax = new_fig(title, figsize=figsize)
        x = np.arange(len(answers))
        n_groups = len(groups)
        total_w = 0.8
        w = total_w / n_groups

        colors = plt.cm.Set2(np.linspace(0, 1, n_groups))
        for i, grp in enumerate(groups):
            vals = [pivot.loc[grp, a]
                    if a in pivot.columns else 0 for a in answers]
            ax.bar(x + i * w - total_w/2, vals, w, label=str(grp),
                   color=colors[i], edgecolor="white")

        ax.set_xticks(x)
        ax.set_xticklabels(answers, fontsize=8, rotation=30, ha="right")
        ax.legend(frameon=False, fontsize=8, title=by_group)
        ax.set_ylabel("Count")
        ax.spines[["top", "right"]].set_visible(False)
        save_fig(pdf, fig)


def plot_long_snapp_vs_tapsi(pdf, long_df, snapp_question, tapsi_question,
                             title, figsize=(14, 6)):
    """
    Side-by-side bar charts for Snapp vs Tapsi from long_survey,
    showing the same question for both platforms.
    """
    fig, axes = plt.subplots(1, 2, figsize=figsize,
                             facecolor=BG_COLOR, sharey=True)
    fig.suptitle(title, fontsize=15, fontweight="bold", y=0.99)

    for ax, q, color, label in [
        (axes[0], snapp_question, SNAPP_COLOR, "Snapp"),
        (axes[1], tapsi_question, TAPSI_COLOR, "Tapsi"),
    ]:
        qdata = long_df[long_df["question"] == q]
        if len(qdata) == 0:
            ax.set_title(f"{label}  (no data)", fontsize=11)
            ax.set_facecolor(BG_COLOR)
            continue

        answer_counts = qdata["answer"].value_counts(
        ).sort_values(ascending=True)
        total = answer_counts.sum()
        ax.barh(answer_counts.index, answer_counts.values,
                color=color, edgecolor="white")
        for i, (ans, v) in enumerate(answer_counts.items()):
            ax.text(v + 0.5, i, f"{v} ({v/total*100:.0f}%)",
                    va="center", fontsize=9)
        ax.set_title(f"{label}  (n={total})", fontsize=11)
        ax.set_facecolor(BG_COLOR)
        ax.spines[["top", "right"]].set_visible(False)

    axes[0].set_xlabel("Count")
    axes[1].set_xlabel("Count")
    save_fig(pdf, fig)


# ── Build PDF ────────────────────────────────────────────────────────

with PdfPages(OUTPUT_PDF) as pdf:

    # ================================================================
    # PAGE 1 – RESPONSE COUNT BY YEARWEEK
    # ================================================================
    week_counts = short.groupby("yearweek").size()
    fig, ax = new_fig("Weekly Response Count (by Year-Week)")
    ax.bar(week_counts.index.astype(str), week_counts.values,
           color=ACCENT, edgecolor="white", linewidth=0.5)
    bar_label(ax)
    ax.set_xlabel("Year-Week")
    ax.set_ylabel("Responses")
    ax.spines[["top", "right"]].set_visible(False)
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 2 – DEMOGRAPHICS OVERVIEW (2x2 grid)
    # ================================================================
    fig, axes = plt.subplots(2, 2, figsize=(12, 8), facecolor=BG_COLOR)
    fig.suptitle("Demographics Overview", fontsize=15,
                 fontweight="bold", y=0.97)

    ax = axes[0, 0]
    ag = short["age_group"].value_counts()
    ax.barh(ag.index, ag.values, color=[ACCENT, GREY])
    ax.set_title("Age Group", fontsize=11)
    for i, v in enumerate(ag.values):
        ax.text(v + 5, i, f"{v} ({v/len(short)*100:.0f}%)",
                va="center", fontsize=9)

    ax = axes[0, 1]
    ed = short["edu"].value_counts().sort_index()
    labels = ["High School\nor Below", "College+"]
    ax.barh(labels, ed.values, color=[GREY, ACCENT])
    ax.set_title("Education", fontsize=11)
    for i, v in enumerate(ed.values):
        ax.text(v + 5, i, f"{v} ({v/len(short)*100:.0f}%)",
                va="center", fontsize=9)

    ax = axes[1, 0]
    ct = short["cooperation_type"].value_counts()
    ax.barh(ct.index, ct.values, color=[SNAPP_COLOR, TAPSI_COLOR])
    ax.set_title("Cooperation Type", fontsize=11)
    for i, v in enumerate(ct.values):
        ax.text(v + 5, i, f"{v} ({v/len(short)*100:.0f}%)",
                va="center", fontsize=9)

    ax = axes[1, 1]
    ms = short["marr_stat"].value_counts().sort_index()
    labels = ["Single", "Married"]
    ax.barh(labels, ms.values, color=[GREY, ACCENT])
    ax.set_title("Marital Status", fontsize=11)
    for i, v in enumerate(ms.values):
        ax.text(v + 5, i, f"{v} ({v/len(short)*100:.0f}%)",
                va="center", fontsize=9)

    for row in axes:
        for ax in row:
            ax.set_facecolor(BG_COLOR)
            ax.spines[["top", "right"]].set_visible(False)
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 3 – ACTIVE JOINT RATE BY YEARWEEK
    # ================================================================
    weekly_joint = short.groupby("yearweek").agg(
        total=("active_joint", "size"),
        active=("active_joint", "sum")
    )
    weekly_joint["rate"] = weekly_joint["active"] / weekly_joint["total"] * 100

    fig, ax = new_fig("Active Joint (Tapsi) Rate by Year-Week")
    ax.plot(weekly_joint.index.astype(str), weekly_joint["rate"],
            marker="o", color=TAPSI_COLOR, linewidth=2.5, markersize=8)
    for i, (idx, row) in enumerate(weekly_joint.iterrows()):
        ax.annotate(f"{row['rate']:.0f}%", (str(idx), row["rate"]),
                    textcoords="offset points", xytext=(0, 10),
                    ha="center", fontsize=9)
    ax.set_xlabel("Year-Week")
    ax.set_ylabel("Active Joint Rate (%)")
    ax.set_ylim(0, 100)
    ax.spines[["top", "right"]].set_visible(False)
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 4 – AVERAGE RIDE COUNTS
    # ================================================================
    weekly_rides = short.groupby("yearweek").agg(
        snapp_ride=("snapp_ride", "mean"),
        tapsi_ride=("tapsi_ride", "mean")
    )
    fig, ax = new_fig("Average Weekly Ride Count – Snapp vs Tapsi")
    ax.plot(weekly_rides.index.astype(str), weekly_rides["snapp_ride"],
            marker="o", color=SNAPP_COLOR, linewidth=2.5, label="Snapp")
    ax.plot(weekly_rides.index.astype(str), weekly_rides["tapsi_ride"],
            marker="s", color=TAPSI_COLOR, linewidth=2.5, label="Tapsi")
    ax.legend(frameon=False, fontsize=10)
    ax.set_xlabel("Year-Week")
    ax.set_ylabel("Avg Rides (midpoint)")
    ax.spines[["top", "right"]].set_visible(False)
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 5 – SATISFACTION COMPARISON (Fare, Income, Request Count)
    # ================================================================
    fig, axes = plt.subplots(1, 3, figsize=(
        14, 5), facecolor=BG_COLOR, sharey=True)
    fig.suptitle("Satisfaction Comparison (1–5 scale): Snapp vs Tapsi",
                 fontsize=15, fontweight="bold", y=0.99)

    for ax, (scol, tcol, label) in zip(axes, SAT_PAIRS):
        snapp_mean = short[scol].dropna().mean()
        tapsi_mean = short[tcol].dropna().mean()
        bars = ax.bar(["Snapp", "Tapsi"], [snapp_mean, tapsi_mean],
                      color=[SNAPP_COLOR, TAPSI_COLOR], width=0.5,
                      edgecolor="white", linewidth=0.5)
        ax.set_title(label, fontsize=11)
        ax.set_ylim(0, 5.5)
        ax.set_facecolor(BG_COLOR)
        ax.spines[["top", "right"]].set_visible(False)
        for bar in bars:
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.08,
                    f"{bar.get_height():.2f}", ha="center", fontsize=10,
                    fontweight="bold")
    axes[0].set_ylabel("Mean Satisfaction")
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 6 – OVERALL SATISFACTION DISTRIBUTION
    # ================================================================
    fig, axes = plt.subplots(1, 2, figsize=(
        12, 5), facecolor=BG_COLOR, sharey=True)
    fig.suptitle("Overall Satisfaction Distribution (1–5 scale)",
                 fontsize=15, fontweight="bold", y=0.99)

    for ax, col, color, label in [
        (axes[0], "snapp_overall_satisfaction", SNAPP_COLOR, "Snapp"),
        (axes[1], "tapsi_overall_satisfaction", TAPSI_COLOR, "Tapsi"),
    ]:
        data = short[col].dropna()
        counts = data.value_counts().sort_index()
        ax.bar(counts.index.astype(int).astype(str), counts.values,
               color=color, edgecolor="white")
        total = counts.sum()
        for x, y in zip(counts.index.astype(int).astype(str), counts.values):
            ax.text(x, y + 1, f"{y}\n({y/total*100:.0f}%)",
                    ha="center", fontsize=8)
        ax.set_title(f"{label}  (n={int(total)})", fontsize=11)
        ax.set_xlabel("Rating")
        ax.set_facecolor(BG_COLOR)
        ax.spines[["top", "right"]].set_visible(False)
    axes[0].set_ylabel("Count")
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 7 – NPS BY YEARWEEK
    # ================================================================
    nps_weekly = short.groupby("yearweek").agg(
        snapp_nps=("snapp_recommend", nps_score),
        tapsi_nps=("tapsi_recommend", nps_score)
    ).dropna(how="all")

    fig, ax = new_fig("NPS (Net Promoter Score) by Year-Week – Snapp vs Tapsi")
    if not nps_weekly["snapp_nps"].isna().all():
        ax.plot(nps_weekly.index.astype(str), nps_weekly["snapp_nps"],
                marker="o", color=SNAPP_COLOR, linewidth=2.5, label="Snapp")
    if not nps_weekly["tapsi_nps"].isna().all():
        ax.plot(nps_weekly.index.astype(str), nps_weekly["tapsi_nps"],
                marker="s", color=TAPSI_COLOR, linewidth=2.5, label="Tapsi")
    ax.axhline(0, color=GREY, linestyle="--", linewidth=0.8)
    ax.legend(frameon=False, fontsize=10)
    ax.set_xlabel("Year-Week")
    ax.set_ylabel("NPS")
    ax.spines[["top", "right"]].set_visible(False)
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 8 – INCENTIVE CATEGORY BREAKDOWN
    # ================================================================
    fig, axes = plt.subplots(1, 2, figsize=(12, 5), facecolor=BG_COLOR)
    fig.suptitle("Incentive Category Distribution",
                 fontsize=15, fontweight="bold", y=0.99)

    for ax, col, color, label in [
        (axes[0], "snapp_incentive_category", SNAPP_COLOR, "Snapp"),
        (axes[1], "tapsi_incentive_category", TAPSI_COLOR, "Tapsi"),
    ]:
        data = short[col].dropna().value_counts()
        ax.barh(data.index, data.values, color=color, edgecolor="white")
        total = data.sum()
        for i, v in enumerate(data.values):
            ax.text(v + 2, i, f"{v} ({v/total*100:.0f}%)",
                    va="center", fontsize=9)
        ax.set_title(f"{label}  (n={total})", fontsize=11)
        ax.set_facecolor(BG_COLOR)
        ax.spines[["top", "right"]].set_visible(False)
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 9 – INCENTIVE TYPE USAGE (wide_survey)
    # ================================================================
    incentive_types = {
        "Snapp": {
            "Pay After Ride":          "Snapp Incentive Type__Pay After Ride",
            "Ride-Based Comm-free":    "Snapp Incentive Type__Ride-Based Commission-free",
            "Earning-Based Comm-free": "Snapp Incentive Type__Earning-based Commission-free",
            "Income Guarantee":        "Snapp Incentive Type__Income Guarantee",
            "Pay After Income":        "Snapp Incentive Type__Pay After Income",
        },
        "Tapsi": {
            "Pay After Ride":          "Tapsi Incentive Type__Pay After Ride",
            "Ride-Based Comm-free":    "Tapsi Incentive Type__Ride-Based Commission-free",
            "Earning-Based Comm-free": "Tapsi Incentive Type__Earning-based Commission-free",
            "Income Guarantee":        "Tapsi Incentive Type__Income Guarantee",
            "Pay After Income":        "Tapsi Incentive Type__Pay After Income",
        },
    }

    fig, axes = plt.subplots(1, 2, figsize=(
        14, 6), facecolor=BG_COLOR, sharey=True)
    fig.suptitle("Incentive Type Usage – wide_survey (multi-choice)",
                 fontsize=15, fontweight="bold", y=0.99)

    for ax, (platform, cols) in zip(axes, incentive_types.items()):
        labels = list(cols.keys())
        values = [wide[c].sum() if c in wide.columns else 0
                  for c in cols.values()]
        color = PLATFORM_COLORS[platform]
        y_pos = range(len(labels))
        ax.barh(y_pos, values, color=color, edgecolor="white")
        ax.set_yticks(y_pos)
        ax.set_yticklabels(labels)
        for i, v in enumerate(values):
            ax.text(v + 5, i, str(int(v)), va="center", fontsize=9)
        ax.set_title(platform, fontsize=12)
        ax.set_facecolor(BG_COLOR)
        ax.spines[["top", "right"]].set_visible(False)
    axes[0].invert_yaxis()
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 10 – INCENTIVE UNSATISFACTION REASONS (wide_survey)
    # ================================================================
    unsat_types = {
        "Snapp": {
            "Improper Amount":  "Snapp Incentive Unsatisfaction__Improper Amount",
            "Difficult":        "Snapp Incentive Unsatisfaction__difficult",
            "No Time":          "Snapp Incentive Unsatisfaction__No Time todo",
            "Not Available":    "Snapp Incentive Unsatisfaction__No Available Time",
            "Non Payment":      "Snapp Incentive Unsatisfaction__Non Payment",
        },
        "Tapsi": {
            "Improper Amount":  "Tapsi Incentive Unsatisfaction__Improper Amount",
            "Difficult":        "Tapsi Incentive Unsatisfaction__difficult",
            "Not Available":    "Tapsi Incentive Unsatisfaction__Not Available",
            "No Time":          "Tapsi Incentive Unsatisfaction__No Time todo",
            "Non Payment":      "Tapsi Incentive Unsatisfaction__Non Payment",
        },
    }

    fig, axes = plt.subplots(1, 2, figsize=(
        14, 5), facecolor=BG_COLOR, sharey=True)
    fig.suptitle("Incentive Unsatisfaction Reasons – wide_survey",
                 fontsize=15, fontweight="bold", y=0.99)

    for ax, (platform, cols) in zip(axes, unsat_types.items()):
        labels = list(cols.keys())
        values = [wide[c].sum() if c in wide.columns else 0
                  for c in cols.values()]
        color = PLATFORM_COLORS[platform]
        y_pos = range(len(labels))
        ax.barh(y_pos, values, color=color, edgecolor="white")
        ax.set_yticks(y_pos)
        ax.set_yticklabels(labels)
        for i, v in enumerate(values):
            ax.text(v + 2, i, str(int(v)), va="center", fontsize=9)
        ax.set_title(platform, fontsize=12)
        ax.set_facecolor(BG_COLOR)
        ax.spines[["top", "right"]].set_visible(False)
    axes[0].invert_yaxis()
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 11 – AVERAGE INCENTIVE (RIALS) BY YEARWEEK
    # ================================================================
    weekly_inc = short.groupby("yearweek").agg(
        snapp=("snapp_incentive", "mean"),
        tapsi=("tapsi_incentive", "mean")
    )

    fig, ax = new_fig("Average Monetary Incentive by Year-Week (Rials)")
    ax.plot(weekly_inc.index.astype(str), weekly_inc["snapp"] / 1e6,
            marker="o", color=SNAPP_COLOR, linewidth=2.5, label="Snapp")
    ax.plot(weekly_inc.index.astype(str), weekly_inc["tapsi"] / 1e6,
            marker="s", color=TAPSI_COLOR, linewidth=2.5, label="Tapsi")
    ax.legend(frameon=False, fontsize=10)
    ax.set_xlabel("Year-Week")
    ax.set_ylabel("Avg Incentive (Million Rials)")
    ax.spines[["top", "right"]].set_visible(False)
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 12 – LOC DISTRIBUTION
    # ================================================================
    fig, axes = plt.subplots(1, 2, figsize=(
        12, 5), facecolor=BG_COLOR, sharey=True)
    fig.suptitle("Length of Cooperation Distribution (months)",
                 fontsize=15, fontweight="bold", y=0.99)

    for ax, col, color, label in [
        (axes[0], "snapp_LOC", SNAPP_COLOR, "Snapp"),
        (axes[1], "tapsi_LOC", TAPSI_COLOR, "Tapsi"),
    ]:
        data = short[col].dropna()
        ax.hist(data, bins=20, color=color, edgecolor="white", alpha=0.85)
        ax.axvline(data.mean(), color="black", linestyle="--", linewidth=1.2)
        ax.text(data.mean() + 1, ax.get_ylim()[1] * 0.9,
                f"Mean: {data.mean():.1f}", fontsize=9)
        ax.set_title(f"{label}  (n={len(data)})", fontsize=11)
        ax.set_xlabel("Months")
        ax.set_facecolor(BG_COLOR)
        ax.spines[["top", "right"]].set_visible(False)
    axes[0].set_ylabel("Count")
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 13 – RIDE REFUSAL REASONS (wide_survey only)
    # ================================================================
    refusal_labels = [
        "Insufficient Fare", "Distance to Origin", "Wait for Better",
        "Long Trip", "Target Destination", "Traffic",
        "Short Accept Time", "Unfamiliar Route", "Internet Problems",
        "App Problems", "Not Realized",
    ]
    snapp_refusal_cols = [
        "Snapp Ride Refusal Reasons__Insufficient Fare",
        "Snapp Ride Refusal Reasons__Distance to origin was Long",
        "Snapp Ride Refusal Reasons__Wait for better Offer",
        "Snapp Ride Refusal Reasons__Long Trip Duration",
        "Snapp Ride Refusal Reasons__Had a Target Destination",
        "Snapp Ride Refusal Reasons__Traffic",
        "Snapp Ride Refusal Reasons__Short Accept Time",
        "Snapp Ride Refusal Reasons__Unfamiliar Route",
        "Snapp Ride Refusal Reasons__Internet Problems",
        "Snapp Ride Refusal Reasons__App Problems",
        "Snapp Ride Refusal Reasons__Not Realized There's Request",
    ]
    tapsi_refusal_cols = [c.replace("Snapp", "Tapsi")
                          for c in snapp_refusal_cols]

    snapp_vals = [wide[c].sum() if c in wide.columns else 0
                  for c in snapp_refusal_cols]
    tapsi_vals = [wide[c].sum() if c in wide.columns else 0
                  for c in tapsi_refusal_cols]

    fig, ax = new_fig("Ride Refusal Reasons – Snapp vs Tapsi (wide_survey)",
                      figsize=(14, 6))
    y = np.arange(len(refusal_labels))
    h = 0.35
    ax.barh(y - h/2, snapp_vals, h, color=SNAPP_COLOR,
            label="Snapp", edgecolor="white")
    ax.barh(y + h/2, tapsi_vals, h, color=TAPSI_COLOR,
            label="Tapsi", edgecolor="white")
    ax.set_yticks(y)
    ax.set_yticklabels(refusal_labels)
    ax.invert_yaxis()
    ax.legend(frameon=False, fontsize=10)
    ax.set_xlabel("Count")
    ax.spines[["top", "right"]].set_visible(False)
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 14 – CUSTOMER SUPPORT CATEGORY (wide_survey)
    # ================================================================
    cs_labels = ["Fare", "Cancelling", "Trip Problems", "Petrol",
                 "Technical", "Settlement", "Incentive",
                 "Location Change", "Drivers Club", "Registration"]
    snapp_cs = [f"Snapp Customer Support Category__{l}" for l in cs_labels]
    tapsi_cs = [f"Tapsi Customer Support Category__{l}" for l in
                ["Fare", "Cancelling", "Trip Problems", "Petrol",
                 "Technical", "Settlement", "Incentive",
                 "Loc Change", "Drivers Club", "Registration"]]

    snapp_cs_vals = [wide[c].sum() if c in wide.columns else 0
                     for c in snapp_cs]
    tapsi_cs_vals = [wide[c].sum() if c in wide.columns else 0
                     for c in tapsi_cs]

    fig, ax = new_fig("Customer Support Ticket Categories – wide_survey",
                      figsize=(14, 6))
    y = np.arange(len(cs_labels))
    h = 0.35
    ax.barh(y - h/2, snapp_cs_vals, h, color=SNAPP_COLOR,
            label="Snapp", edgecolor="white")
    ax.barh(y + h/2, tapsi_cs_vals, h, color=TAPSI_COLOR,
            label="Tapsi", edgecolor="white")
    ax.set_yticks(y)
    ax.set_yticklabels(cs_labels)
    ax.invert_yaxis()
    ax.legend(frameon=False, fontsize=10)
    ax.set_xlabel("Count")
    ax.spines[["top", "right"]].set_visible(False)
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 15 – NAVIGATION APP USAGE (wide_survey only)
    # ================================================================
    nav_apps = ["Google Map", "Waze", "Neshan", "Balad"]

    fig, axes = plt.subplots(1, 3, figsize=(
        15, 5), facecolor=BG_COLOR, sharey=True)
    fig.suptitle("Navigation App Adoption Funnel – wide_survey",
                 fontsize=15, fontweight="bold", y=0.99)

    for ax, stage in zip(axes, ["Familiarity", "Installed", "Used"]):
        cols = [f"Navigation {stage}__{app}" for app in nav_apps]
        vals = [wide[c].sum() if c in wide.columns else 0 for c in cols]
        bars = ax.barh(nav_apps, vals,
                       color=[ACCENT, "#FFA726", "#66BB6A", "#AB47BC"],
                       edgecolor="white")
        for i, v in enumerate(vals):
            ax.text(v + 10, i, str(int(v)), va="center", fontsize=9)
        ax.set_title(stage, fontsize=12)
        ax.set_facecolor(BG_COLOR)
        ax.spines[["top", "right"]].set_visible(False)
    axes[0].invert_yaxis()
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 16 – SATISFACTION BY COOPERATION TYPE
    # ================================================================
    sat_by_coop = short.groupby("cooperation_type").agg(
        snapp_fare=("snapp_fare_satisfaction", "mean"),
        tapsi_fare=("tapsi_fare_satisfaction", "mean"),
        snapp_income=("snapp_income_satisfaction", "mean"),
        tapsi_income=("tapsi_income_satisfaction", "mean"),
    ).reindex(["Part-Time", "Full-Time"])

    fig, axes = plt.subplots(1, 2, figsize=(
        12, 5), facecolor=BG_COLOR, sharey=True)
    fig.suptitle("Satisfaction by Cooperation Type",
                 fontsize=15, fontweight="bold", y=0.99)

    x = np.arange(2)
    w = 0.35

    ax = axes[0]
    ax.bar(x - w/2, sat_by_coop["snapp_fare"], w,
           color=SNAPP_COLOR, label="Snapp")
    ax.bar(x + w/2, sat_by_coop["tapsi_fare"], w,
           color=TAPSI_COLOR, label="Tapsi")
    ax.set_xticks(x)
    ax.set_xticklabels(sat_by_coop.index)
    ax.set_title("Fare Satisfaction", fontsize=11)
    ax.set_ylabel("Mean (1–5)")
    ax.set_ylim(0, 5.5)
    ax.legend(frameon=False, fontsize=9)
    ax.set_facecolor(BG_COLOR)
    ax.spines[["top", "right"]].set_visible(False)

    ax = axes[1]
    ax.bar(x - w/2, sat_by_coop["snapp_income"], w,
           color=SNAPP_COLOR, label="Snapp")
    ax.bar(x + w/2, sat_by_coop["tapsi_income"], w,
           color=TAPSI_COLOR, label="Tapsi")
    ax.set_xticks(x)
    ax.set_xticklabels(sat_by_coop.index)
    ax.set_title("Income Satisfaction", fontsize=11)
    ax.set_ylim(0, 5.5)
    ax.legend(frameon=False, fontsize=9)
    ax.set_facecolor(BG_COLOR)
    ax.spines[["top", "right"]].set_visible(False)
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 17 – GPS GLITCH ACTIONS (wide_survey only)
    # ================================================================
    gps_labels = ["Called Passenger", "Accepted Familiar Trips",
                  "Passenger Help", "Decided to Stop",
                  "Cancelled Trip", "Changed Location", "Switched to Tapsi"]
    gps_cols = [
        "GPS Action when Glitch__Called Passenger",
        "GPS Action when Glitch__Accepted familiar trips",
        "GPS Action when Glitch__Passenger Help for route",
        "GPS Action when Glitch__Decided to stop working",
        "GPS Action when Glitch__Cancelled Trip",
        "GPS Action when Glitch__Changed Location",
        "GPS Action when Glitch__Switched to Tapsi",
    ]
    gps_vals = [wide[c].sum() if c in wide.columns else 0 for c in gps_cols]

    fig, ax = new_fig("Driver Actions During GPS Glitch – wide_survey")
    ax.barh(gps_labels, gps_vals, color=ACCENT, edgecolor="white")
    for i, v in enumerate(gps_vals):
        ax.text(v + 5, i, str(int(v)), va="center", fontsize=9)
    ax.invert_yaxis()
    ax.set_xlabel("Count")
    ax.spines[["top", "right"]].set_visible(False)
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 18 – TOP 15 CITIES
    # ================================================================
    city_counts = short["city"].value_counts().head(15)
    fig, ax = new_fig("Top 15 Cities by Response Count")
    ax.barh(city_counts.index[::-1], city_counts.values[::-1],
            color=ACCENT, edgecolor="white")
    for i, v in enumerate(city_counts.values[::-1]):
        ax.text(v + 1, i, str(v), va="center", fontsize=9)
    ax.set_xlabel("Responses")
    ax.spines[["top", "right"]].set_visible(False)
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 19 – INCENTIVE SATISFACTION DISTRIBUTION
    # ================================================================
    fig, axes = plt.subplots(1, 2, figsize=(
        12, 5), facecolor=BG_COLOR, sharey=True)
    fig.suptitle("Incentive Satisfaction Distribution (1–5 scale)",
                 fontsize=15, fontweight="bold", y=0.99)

    for ax, col, color, label in [
        (axes[0], "snapp_overall_incentive_satisfaction", SNAPP_COLOR, "Snapp"),
        (axes[1], "tapsi_overall_incentive_satisfaction", TAPSI_COLOR, "Tapsi"),
    ]:
        data = short[col].dropna()
        counts = data.value_counts().sort_index()
        ax.bar(counts.index.astype(int).astype(str), counts.values,
               color=color, edgecolor="white")
        total = counts.sum()
        for x, y in zip(counts.index.astype(int).astype(str), counts.values):
            ax.text(x, y + 1, f"{y}\n({y/total*100:.0f}%)",
                    ha="center", fontsize=8)
        ax.set_title(
            f"{label}  (n={int(total)},  mean={data.mean():.2f})", fontsize=11)
        ax.set_xlabel("Rating")
        ax.set_facecolor(BG_COLOR)
        ax.spines[["top", "right"]].set_visible(False)
    axes[0].set_ylabel("Count")
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 20 – COMMISSION-FREE RIDES VS TOTAL RIDES
    # ================================================================
    fig, axes = plt.subplots(1, 2, figsize=(12, 5), facecolor=BG_COLOR)
    fig.suptitle("Commission-Free Rides vs Total Rides",
                 fontsize=15, fontweight="bold", y=0.99)

    for ax, ride_col, cf_col, color, label in [
        (axes[0], "snapp_ride", "snapp_commfree", SNAPP_COLOR, "Snapp"),
        (axes[1], "tapsi_ride", "tapsi_commfree", TAPSI_COLOR, "Tapsi"),
    ]:
        mask = short[cf_col].notna()
        ax.scatter(short.loc[mask, ride_col], short.loc[mask, cf_col],
                   alpha=0.4, color=color, edgecolors="white",
                   linewidth=0.3, s=40)
        lims = [0, 85]
        ax.plot(lims, lims, "--", color=GREY, linewidth=0.8, label="y = x")
        ax.set_xlim(lims)
        ax.set_ylim(lims)
        ax.set_xlabel("Total Rides")
        ax.set_title(f"{label}  (n={mask.sum()})", fontsize=11)
        ax.legend(frameon=False, fontsize=9)
        ax.set_facecolor(BG_COLOR)
        ax.spines[["top", "right"]].set_visible(False)
    axes[0].set_ylabel("Commission-Free Rides")
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 21 – SATISFACTION BY YEARWEEK (line charts)
    # ================================================================
    fig, axes = plt.subplots(1, 3, figsize=(
        16, 5), facecolor=BG_COLOR, sharey=True)
    fig.suptitle("Avg Satisfaction by Year-Week: Snapp vs Tapsi",
                 fontsize=15, fontweight="bold", y=0.99)

    for ax, (scol, tcol, label) in zip(axes, SAT_PAIRS):
        weekly_sat = short.groupby("yearweek").agg(
            snapp=(scol, "mean"),
            tapsi=(tcol, "mean")
        )
        ax.plot(weekly_sat.index.astype(str), weekly_sat["snapp"],
                marker="o", color=SNAPP_COLOR, linewidth=2, label="Snapp")
        ax.plot(weekly_sat.index.astype(str), weekly_sat["tapsi"],
                marker="s", color=TAPSI_COLOR, linewidth=2, label="Tapsi")
        for idx, row in weekly_sat.iterrows():
            if not np.isnan(row["snapp"]):
                ax.annotate(f"{row['snapp']:.2f}", (str(idx), row["snapp"]),
                            textcoords="offset points", xytext=(0, 8),
                            ha="center", fontsize=7, color=SNAPP_COLOR)
            if not np.isnan(row["tapsi"]):
                ax.annotate(f"{row['tapsi']:.2f}", (str(idx), row["tapsi"]),
                            textcoords="offset points", xytext=(0, -12),
                            ha="center", fontsize=7, color=TAPSI_COLOR)
        ax.set_title(label, fontsize=11)
        ax.set_ylim(0, 5.5)
        ax.set_xlabel("Year-Week")
        ax.legend(frameon=False, fontsize=8)
        ax.set_facecolor(BG_COLOR)
        ax.spines[["top", "right"]].set_visible(False)
        ax.tick_params(axis="x", rotation=45)
    axes[0].set_ylabel("Mean Satisfaction (1–5)")
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 22 – SATISFACTION BY COOPERATION TYPE (grouped bar)
    # ================================================================
    plot_sat_by_group(pdf, short, "cooperation_type", "Cooperation Type")

    # ================================================================
    # PAGE 23 – SATISFACTION BY CITY (top 10 cities)
    # ================================================================
    plot_sat_by_group(pdf, short, "city", "City (Top 10)",
                      top_n=10, min_group_size=20)

    # ================================================================
    # PAGE 24 – SATISFACTION BY DRIVER TYPE
    # ================================================================
    plot_sat_by_group(pdf, short, "driver_type",
                      "Driver Type (Snapp Exclusive vs Joint)")

    # ================================================================
    # PAGE 25 – ★ LONG_SURVEY: INCENTIVE TYPE (Snapp vs Tapsi)
    # ================================================================
    plot_long_snapp_vs_tapsi(
        pdf, long,
        "Snapp Incentive Type", "Tapsi Incentive Type",
        "Incentive Type – long_survey"
    )

    # ================================================================
    # PAGE 26 – ★ LONG_SURVEY: INCENTIVE GOT BONUS (Snapp vs Tapsi)
    # ================================================================
    plot_long_snapp_vs_tapsi(
        pdf, long,
        "Snapp Incentive GotBonus", "Tapsi Incentive GotBonus",
        "Incentive Got Bonus – long_survey"
    )

    # ================================================================
    # PAGE 27 – ★ LONG_SURVEY: CUSTOMER SUPPORT (Snapp vs Tapsi)
    # ================================================================
    plot_long_snapp_vs_tapsi(
        pdf, long,
        "Snapp Customer Support Category", "Tapsi Customer Support Category",
        "Customer Support Categories – long_survey"
    )

    # ================================================================
    # PAGE 28 – ★ LONG_SURVEY: TAPSI-ONLY QUESTIONS
    # ================================================================
    fig, axes = plt.subplots(1, 2, figsize=(14, 5), facecolor=BG_COLOR)
    fig.suptitle("Tapsi-Only Questions – long_survey",
                 fontsize=15, fontweight="bold", y=0.99)

    # Tapsi Incentive Unsatisfaction
    ax = axes[0]
    qdata = long[long["question"] == "Tapsi Incentive Unsatisfaction"]
    if len(qdata) > 0:
        ac = qdata["answer"].value_counts().sort_values(ascending=True)
        ax.barh(ac.index, ac.values, color=TAPSI_COLOR, edgecolor="white")
        total = ac.sum()
        for i, (ans, v) in enumerate(ac.items()):
            ax.text(v + 0.3, i, f"{v} ({v/total*100:.0f}%)",
                    va="center", fontsize=9)
    ax.set_title(f"Incentive Unsatisfaction (n={len(qdata)})", fontsize=11)
    ax.set_xlabel("Count")
    ax.set_facecolor(BG_COLOR)
    ax.spines[["top", "right"]].set_visible(False)

    # Tapsi Carpooling Refusion
    ax = axes[1]
    qdata = long[long["question"] == "Tapsi Carpooling Refusion"]
    if len(qdata) > 0:
        ac = qdata["answer"].value_counts().sort_values(ascending=True)
        ax.barh(ac.index, ac.values, color=TAPSI_COLOR, edgecolor="white")
        total = ac.sum()
        for i, (ans, v) in enumerate(ac.items()):
            ax.text(v + 0.2, i, f"{v} ({v/total*100:.0f}%)",
                    va="center", fontsize=9)
    ax.set_title(f"Carpooling Refusion (n={len(qdata)})", fontsize=11)
    ax.set_xlabel("Count")
    ax.set_facecolor(BG_COLOR)
    ax.spines[["top", "right"]].set_visible(False)
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 29 – ★ LONG_SURVEY: MULTI-CHOICE BY DRIVER TYPE
    # ================================================================
    # Show customer support categories split by driver type
    for q_snapp, q_tapsi, title in [
        ("Snapp Customer Support Category",
         "Tapsi Customer Support Category",
         "Customer Support by Driver Type – long_survey"),
    ]:
        for q, platform, color in [
            (q_snapp, "Snapp", SNAPP_COLOR),
            (q_tapsi, "Tapsi", TAPSI_COLOR),
        ]:
            qdata = long[long["question"] == q]
            if len(qdata) == 0:
                continue

            pivot = qdata.groupby(["driver_type", "answer"]).size().unstack(
                fill_value=0)
            answers = pivot.columns.tolist()
            groups = pivot.index.tolist()

            fig, ax = new_fig(f"{platform} Customer Support by Driver Type",
                              figsize=(14, 6))
            y = np.arange(len(answers))
            n_g = len(groups)
            total_w = 0.7
            w = total_w / n_g

            grp_colors = ["#42A5F5", "#EF5350"]  # Blue=Joint, Red=Exclusive
            for i, grp in enumerate(groups):
                vals = pivot.loc[grp].values
                ax.barh(y + i * w - total_w/2, vals, w,
                        label=grp, color=grp_colors[i % len(grp_colors)],
                        edgecolor="white")

            ax.set_yticks(y)
            ax.set_yticklabels(answers)
            ax.legend(frameon=False, fontsize=9)
            ax.set_xlabel("Count")
            ax.spines[["top", "right"]].set_visible(False)
            save_fig(pdf, fig)


print(f"\nReport saved to {OUTPUT_PDF}")
print(f"Total pages: 31")
