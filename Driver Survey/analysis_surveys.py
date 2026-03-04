"""
Driver Survey – Visual Analysis
================================
Generates a multi-page PDF report from short_survey.csv and wide_survey.csv.
Each page is a self-contained chart with title and annotations.

Usage:
    python survey_analysis.py
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
OUTPUT_PDF = r"D:\Work\Driver Survey\processed\driver_survey_analysis.pdf"

SNAPP_COLOR = "#00C853"   # Snapp green
TAPSI_COLOR = "#FF6D00"   # Tapsi orange
ACCENT = "#1565C0"   # Blue accent
GREY = "#9E9E9E"
BG_COLOR = "#FAFAFA"

PLATFORM_COLORS = {"Snapp": SNAPP_COLOR, "Tapsi": TAPSI_COLOR}

# ── Load data ────────────────────────────────────────────────────────

short = pd.read_csv(SHORT_PATH, encoding="utf-8-sig")
wide = pd.read_csv(WIDE_PATH,  encoding="utf-8-sig")

# Ensure weeknumber is sorted properly
short["weeknumber"] = short["weeknumber"].astype(int)
wide["weeknumber"] = wide["weeknumber"].astype(int)


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


def pct_bar_label(ax, total):
    for container in ax.containers:
        labels = [f"{v.get_height()/total*100:.0f}%" if v.get_height() > 0 else ""
                  for v in container]
        ax.bar_label(container, labels=labels, fontsize=8, padding=2)


def save_fig(pdf, fig):
    fig.tight_layout(rect=[0, 0, 1, 0.94])
    pdf.savefig(fig, facecolor=BG_COLOR)
    plt.close(fig)


def nps_score(series):
    """Compute NPS from a 0-10 scale series."""
    s = series.dropna()
    if len(s) == 0:
        return np.nan
    promoters = (s >= 9).sum() / len(s) * 100
    detractors = (s <= 6).sum() / len(s) * 100
    return promoters - detractors


# ── Build PDF ────────────────────────────────────────────────────────

with PdfPages(OUTPUT_PDF) as pdf:

    # ================================================================
    # PAGE 1 – RESPONSE COUNT BY WEEK
    # ================================================================
    week_counts = short.groupby("weeknumber").size()
    fig, ax = new_fig("Weekly Response Count")
    ax.bar(week_counts.index.astype(str), week_counts.values,
           color=ACCENT, edgecolor="white", linewidth=0.5)
    bar_label(ax)
    ax.set_xlabel("Week Number")
    ax.set_ylabel("Responses")
    ax.spines[["top", "right"]].set_visible(False)
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 2 – DEMOGRAPHICS OVERVIEW (2x2 grid)
    # ================================================================
    fig, axes = plt.subplots(2, 2, figsize=(12, 8), facecolor=BG_COLOR)
    fig.suptitle("Demographics Overview", fontsize=15,
                 fontweight="bold", y=0.97)

    # Age group
    ax = axes[0, 0]
    ag = short["age_group"].value_counts()
    ax.barh(ag.index, ag.values, color=[ACCENT, GREY])
    ax.set_title("Age Group", fontsize=11)
    for i, v in enumerate(ag.values):
        ax.text(v + 5, i, f"{v} ({v/len(short)*100:.0f}%)",
                va="center", fontsize=9)

    # Education
    ax = axes[0, 1]
    ed = short["edu"].value_counts().sort_index()
    labels = ["High School\nor Below", "College+"]
    ax.barh(labels, ed.values, color=[GREY, ACCENT])
    ax.set_title("Education", fontsize=11)
    for i, v in enumerate(ed.values):
        ax.text(v + 5, i, f"{v} ({v/len(short)*100:.0f}%)",
                va="center", fontsize=9)

    # Cooperation type
    ax = axes[1, 0]
    ct = short["cooperation_type"].value_counts()
    ax.barh(ct.index, ct.values, color=[SNAPP_COLOR, TAPSI_COLOR])
    ax.set_title("Cooperation Type", fontsize=11)
    for i, v in enumerate(ct.values):
        ax.text(v + 5, i, f"{v} ({v/len(short)*100:.0f}%)",
                va="center", fontsize=9)

    # Marital status
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
    # PAGE 3 – ACTIVE JOINT RATE BY WEEK
    # ================================================================
    weekly_joint = short.groupby("weeknumber").agg(
        total=("active_joint", "size"),
        active=("active_joint", "sum")
    )
    weekly_joint["rate"] = weekly_joint["active"] / weekly_joint["total"] * 100

    fig, ax = new_fig("Active Joint (Tapsi) Rate by Week")
    ax.plot(weekly_joint.index.astype(str), weekly_joint["rate"],
            marker="o", color=TAPSI_COLOR, linewidth=2.5, markersize=8)
    for i, (idx, row) in enumerate(weekly_joint.iterrows()):
        ax.annotate(f"{row['rate']:.0f}%", (str(idx), row["rate"]),
                    textcoords="offset points", xytext=(0, 10),
                    ha="center", fontsize=9)
    ax.set_xlabel("Week Number")
    ax.set_ylabel("Active Joint Rate (%)")
    ax.set_ylim(0, 100)
    ax.spines[["top", "right"]].set_visible(False)
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 4 – AVERAGE RIDE COUNTS: SNAPP vs TAPSI
    # ================================================================
    weekly_rides = short.groupby("weeknumber").agg(
        snapp_ride=("snapp_ride", "mean"),
        tapsi_ride=("tapsi_ride", "mean")
    )
    fig, ax = new_fig("Average Weekly Ride Count – Snapp vs Tapsi")
    ax.plot(weekly_rides.index.astype(str), weekly_rides["snapp_ride"],
            marker="o", color=SNAPP_COLOR, linewidth=2.5, label="Snapp")
    ax.plot(weekly_rides.index.astype(str), weekly_rides["tapsi_ride"],
            marker="s", color=TAPSI_COLOR, linewidth=2.5, label="Tapsi")
    ax.legend(frameon=False, fontsize=10)
    ax.set_xlabel("Week Number")
    ax.set_ylabel("Avg Rides (midpoint)")
    ax.spines[["top", "right"]].set_visible(False)
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 5 – SATISFACTION COMPARISON (Fare, Income, Request Count)
    # ================================================================
    sat_pairs = [
        ("snapp_fare_satisfaction",       "tapsi_fare_satisfaction",       "Fare"),
        ("snapp_income_satisfaction",      "tapsi_income_satisfaction",     "Income"),
        ("snapp_req_count_satisfaction",
         "tapsi_req_count_satisfaction",  "Request Count"),
    ]

    fig, axes = plt.subplots(1, 3, figsize=(
        14, 5), facecolor=BG_COLOR, sharey=True)
    fig.suptitle("Satisfaction Comparison (1–5 scale): Snapp vs Tapsi",
                 fontsize=15, fontweight="bold", y=0.99)

    for ax, (scol, tcol, label) in zip(axes, sat_pairs):
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
                    f"{bar.get_height():.2f}", ha="center", fontsize=10, fontweight="bold")
    axes[0].set_ylabel("Mean Satisfaction")
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 6 – OVERALL SATISFACTION DISTRIBUTION (histogram side by side)
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
    # PAGE 7 – NPS (NET PROMOTER SCORE) BY WEEK
    # ================================================================
    nps_weekly = short.groupby("weeknumber").agg(
        snapp_nps=("snapp_recommend", nps_score),
        tapsi_nps=("tapsi_recommend", nps_score)
    ).dropna(how="all")

    fig, ax = new_fig("NPS (Net Promoter Score) by Week – Snapp vs Tapsi")
    if not nps_weekly["snapp_nps"].isna().all():
        ax.plot(nps_weekly.index.astype(str), nps_weekly["snapp_nps"],
                marker="o", color=SNAPP_COLOR, linewidth=2.5, label="Snapp")
    if not nps_weekly["tapsi_nps"].isna().all():
        ax.plot(nps_weekly.index.astype(str), nps_weekly["tapsi_nps"],
                marker="s", color=TAPSI_COLOR, linewidth=2.5, label="Tapsi")
    ax.axhline(0, color=GREY, linestyle="--", linewidth=0.8)
    ax.legend(frameon=False, fontsize=10)
    ax.set_xlabel("Week Number")
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
    # PAGE 9 – INCENTIVE TYPE USAGE (multi-choice from wide_df)
    # ================================================================
    incentive_types = {
        "Snapp": {
            "Pay After Ride":              "Snapp Incentive Type__Pay After Ride",
            "Ride-Based Comm-free":        "Snapp Incentive Type__Ride-Based Commission-free",
            "Earning-Based Comm-free":     "Snapp Incentive Type__Earning-based Commission-free",
            "Income Guarantee":            "Snapp Incentive Type__Income Guarantee",
            "Pay After Income":            "Snapp Incentive Type__Pay After Income",
        },
        "Tapsi": {
            "Pay After Ride":              "Tapsi Incentive Type__Pay After Ride",
            "Ride-Based Comm-free":        "Tapsi Incentive Type__Ride-Based Commission-free",
            "Earning-Based Comm-free":     "Tapsi Incentive Type__Earning-based Commission-free",
            "Income Guarantee":            "Tapsi Incentive Type__Income Guarantee",
            "Pay After Income":            "Tapsi Incentive Type__Pay After Income",
        },
    }

    fig, axes = plt.subplots(1, 2, figsize=(
        14, 6), facecolor=BG_COLOR, sharey=True)
    fig.suptitle("Incentive Type Usage (multi-choice)",
                 fontsize=15, fontweight="bold", y=0.99)

    for ax, (platform, cols) in zip(axes, incentive_types.items()):
        labels = list(cols.keys())
        values = [wide[c].sum() if c in wide.columns else 0 for c in cols.values()]
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
    # PAGE 10 – INCENTIVE UNSATISFACTION REASONS
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
    fig.suptitle("Incentive Unsatisfaction Reasons",
                 fontsize=15, fontweight="bold", y=0.99)

    for ax, (platform, cols) in zip(axes, unsat_types.items()):
        labels = list(cols.keys())
        values = [wide[c].sum() if c in wide.columns else 0 for c in cols.values()]
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
    # PAGE 11 – AVERAGE INCENTIVE (RIALS) BY WEEK
    # ================================================================
    weekly_inc = short.groupby("weeknumber").agg(
        snapp=("snapp_incentive", "mean"),
        tapsi=("tapsi_incentive", "mean")
    )

    fig, ax = new_fig("Average Monetary Incentive by Week (Rials)")
    ax.plot(weekly_inc.index.astype(str), weekly_inc["snapp"] / 1e6,
            marker="o", color=SNAPP_COLOR, linewidth=2.5, label="Snapp")
    ax.plot(weekly_inc.index.astype(str), weekly_inc["tapsi"] / 1e6,
            marker="s", color=TAPSI_COLOR, linewidth=2.5, label="Tapsi")
    ax.legend(frameon=False, fontsize=10)
    ax.set_xlabel("Week Number")
    ax.set_ylabel("Avg Incentive (Million Rials)")
    ax.spines[["top", "right"]].set_visible(False)
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 12 – LOC (LENGTH OF COOPERATION) DISTRIBUTION
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
    # PAGE 13 – RIDE REFUSAL REASONS (multi-choice)
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
    tapsi_refusal_cols = [c.replace("Snapp", "Tapsi").replace("with Tapsi", "with Tapsi")
                          for c in snapp_refusal_cols]

    snapp_vals = [
        wide[c].sum() if c in wide.columns else 0 for c in snapp_refusal_cols]
    tapsi_vals = [
        wide[c].sum() if c in wide.columns else 0 for c in tapsi_refusal_cols]

    fig, ax = new_fig("Ride Refusal Reasons – Snapp vs Tapsi", figsize=(14, 6))
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
    # PAGE 14 – CUSTOMER SUPPORT CATEGORY (multi-choice)
    # ================================================================
    cs_labels = ["Fare", "Cancelling", "Trip Problems", "Petrol",
                 "Technical", "Settlement", "Incentive",
                 "Location Change", "Drivers Club", "Registration"]
    snapp_cs = [f"Snapp Customer Support Category__{l}" for l in cs_labels]
    tapsi_cs = [f"Tapsi Customer Support Category__{l}" for l in
                ["Fare", "Cancelling", "Trip Problems", "Petrol",
                 "Technical", "Settlement", "Incentive",
                 "Loc Change", "Drivers Club", "Registration"]]

    snapp_cs_vals = [
        wide[c].sum() if c in wide.columns else 0 for c in snapp_cs]
    tapsi_cs_vals = [
        wide[c].sum() if c in wide.columns else 0 for c in tapsi_cs]

    fig, ax = new_fig("Customer Support Ticket Categories", figsize=(14, 6))
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
    # PAGE 15 – NAVIGATION APP USAGE (multi-choice)
    # ================================================================
    nav_apps = ["Google Map", "Waze", "Neshan", "Balad"]

    fig, axes = plt.subplots(1, 3, figsize=(
        15, 5), facecolor=BG_COLOR, sharey=True)
    fig.suptitle("Navigation App Adoption Funnel",
                 fontsize=15, fontweight="bold", y=0.99)

    for ax, stage in zip(axes, ["Familiarity", "Installed", "Used"]):
        cols = [f"Navigation {stage}__{app}" for app in nav_apps]
        vals = [wide[c].sum() if c in wide.columns else 0 for c in cols]
        bars = ax.barh(nav_apps, vals, color=[ACCENT, "#FFA726", "#66BB6A", "#AB47BC"],
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

    # Fare
    ax = axes[0]
    ax.bar(x - w/2, sat_by_coop["snapp_fare"],
           w, color=SNAPP_COLOR, label="Snapp")
    ax.bar(x + w/2, sat_by_coop["tapsi_fare"],
           w, color=TAPSI_COLOR, label="Tapsi")
    ax.set_xticks(x)
    ax.set_xticklabels(sat_by_coop.index)
    ax.set_title("Fare Satisfaction", fontsize=11)
    ax.set_ylabel("Mean (1–5)")
    ax.set_ylim(0, 5.5)
    ax.legend(frameon=False, fontsize=9)
    ax.set_facecolor(BG_COLOR)
    ax.spines[["top", "right"]].set_visible(False)

    # Income
    ax = axes[1]
    ax.bar(x - w/2, sat_by_coop["snapp_income"],
           w, color=SNAPP_COLOR, label="Snapp")
    ax.bar(x + w/2, sat_by_coop["tapsi_income"],
           w, color=TAPSI_COLOR, label="Tapsi")
    ax.set_xticks(x)
    ax.set_xticklabels(sat_by_coop.index)
    ax.set_title("Income Satisfaction", fontsize=11)
    ax.set_ylim(0, 5.5)
    ax.legend(frameon=False, fontsize=9)
    ax.set_facecolor(BG_COLOR)
    ax.spines[["top", "right"]].set_visible(False)
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 17 – GPS GLITCH ACTIONS (multi-choice)
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

    fig, ax = new_fig("Driver Actions During GPS Glitch")
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
    # PAGE 20 – COMMISSION-FREE RIDES vs TOTAL RIDES (scatter)
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
                   alpha=0.4, color=color, edgecolors="white", linewidth=0.3, s=40)
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

print(f"Report saved to {OUTPUT_PDF}")
