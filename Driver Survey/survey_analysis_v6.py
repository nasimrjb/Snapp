"""
Driver Survey -- Visual Analysis v6
====================================
This is Step 3 (the final step) of the Driver Survey ETL pipeline:

    1. generate_mapping.py   --> generates column_rename_mapping.json from raw Excel
    2. data_cleaning.py      --> reads raw data, produces 6 processed CSV files
    3. survey_analysis_v6.py --> (THIS FILE) reads those 6 CSVs, produces a PDF report

Purpose:
    Generates a 74-page PDF report containing bar charts, line trends, pie charts,
    scatter plots, funnels, and cross-tabs that visualize driver satisfaction survey
    results for the Snapp and Tapsi rideshare platforms (Iran market).

Input Files (all in processed/ directory):
    The 6 CSVs come in (main, rare) pairs for three "shapes" of data:
    - short_survey_main.csv  -- one row per driver, always/often asked single-choice questions
    - short_survey_rare.csv  -- one row per driver, rarely asked single-choice questions
    - wide_survey_main.csv   -- one row per driver, always/often multi-choice as binary columns
    - wide_survey_rare.csv   -- one row per driver, rarely asked multi-choice as binary columns
    - long_survey_main.csv   -- melted rows (question, answer), always/often multi-choice
    - long_survey_rare.csv   -- melted rows (question, answer), rarely asked multi-choice

    "short" = single-choice answers stored as one column per question.
    "wide"  = multi-choice answers one-hot-encoded (each choice becomes a 0/1 column).
    "long"  = multi-choice answers melted into (question, answer) rows for easy groupby.

    At load time, main+rare pairs are merged (short, wide) or concatenated (long)
    so that all columns are available in a single DataFrame per shape.

Output:
    driver_survey_analysis_v6.pdf -- multi-page PDF containing 74 chart pages.

Key Libraries Used:
    - pandas (pd)           : data loading, grouping, aggregation, and manipulation
    - numpy (np)            : numeric operations (NaN handling, array math)
    - matplotlib (plt)      : all chart rendering (bar, line, pie, scatter, hist)
    - matplotlib.backends.backend_pdf.PdfPages : writes multiple figures into one PDF
    - matplotlib.gridspec   : advanced subplot layouts (e.g., uneven grid on page 69)
    - matplotlib.patches    : custom legend entries (colored rectangles)
    - matplotlib.ticker     : axis tick formatting
    - matplotlib.patheffects: text outline effects (imported but not currently used)

Pages
-----
PAGE  1: Cover / Key KPI Summary
PAGE  2: Weekly Response Count (bar chart of responses per year-week)
PAGE  3: Demographics Overview (age, education, cooperation, marital status)
PAGE  4: Primary Occupation Breakdown (top 15 jobs)
PAGE  5: Active Joint (Tapsi) Rate by Year-Week
PAGE  6: Average Weekly Ride Count (Snapp vs Tapsi trend)
PAGE  7: Satisfaction Comparison (Fare / Income / Request Count)
PAGE  8: Overall Satisfaction Distribution (histograms)
PAGE  9: NPS by Year-Week (Net Promoter Score trend)
PAGE 10: Incentive Category Distribution
PAGE 11: Incentive Type Usage (wide binary columns)
PAGE 12: Incentive Unsatisfaction Reasons (wide binary columns)
PAGE 13: Average Monetary Incentive by Year-Week (trend)
PAGE 14: Incentive Satisfaction Distribution (1-5 scale)
PAGE 15: Length of Cooperation Distribution (months histogram)
PAGE 16: Ride Refusal Reasons (Snapp vs Tapsi, wide binary)
PAGE 17: Customer Support Ticket Categories (wide binary)
PAGE 18: Navigation App Adoption Funnel (long survey)
PAGE 19: Navigation App Ratings (0-10 scale)
PAGE 20: GPS Failure Stage Distribution
PAGE 21: GPS Glitch Time of Day (wide binary)
PAGE 22: GPS Glitch Driver Actions (wide binary)
PAGE 23: Commission & Tax Transparency
PAGE 24: Unpaid Fares -- Incident Rate & Compensation
PAGE 25: Customer Support Channel Usage
PAGE 26: Customer Support Quality Deep Dive
PAGE 27: Collaboration Reasons
PAGE 28: Income Source Preference
PAGE 29: Satisfaction by Year-Week (trend lines)
PAGE 30: Satisfaction by Cooperation Type
PAGE 31: Satisfaction by City (top 10)
PAGE 32: Satisfaction by Driver Type (Joint vs Exclusive)
PAGE 33: Honeymoon Effect (satisfaction decline with tenure)
PAGE 34: Satisfaction by Age Group
PAGE 35: Satisfaction by Driver Engagement Level (active time)
PAGE 36: Satisfaction by Primary Occupation
PAGE 37: Tapsi Carpooling Deep-Dive
PAGE 38: Feature Adoption (EcoPlus & Magical Window)
PAGE 39: Driver Privacy & Participation Attitudes
PAGE 40: Demand & Supply Metrics
PAGE 41: Commission-Free Rides vs Total Rides (scatter)
PAGE 42: Top 15 Cities by Response Count
PAGE 43: City-Level Satisfaction Comparison
PAGE 44: Registration & Referral Analysis
PAGE 45-47: Long Survey Multi-Choice Pages (Incentive Type, GotBonus, CS Category)
PAGE 48: Snapp Navigation Issues (long survey)
PAGE 49: Decline Reason & App Menu Usage (long survey)
PAGE 50: Tapsi-Only Questions (long survey)
PAGE 51-52: CS Category by Driver Type (long survey, Snapp & Tapsi)
PAGE 53: App Usage & EcoPlus Refusal (long survey)
PAGE 54: Joint vs Snapp Exclusive Key Metrics
PAGE 55: Incentive Full Funnel (notification -> participation)
PAGE 56: Incentive Active Duration
PAGE 57: Tapsi Re-activation Timing
PAGE 58: App NPS vs Platform NPS
PAGE 59: Commission Knowledge x Satisfaction cross-tab
PAGE 60: Unpaid Fare Follow-up Satisfaction
PAGE 61: Trip Length Preference
PAGE 62: Navigation Used in Last Trip
PAGE 63: Joining Bonus & Registration Origin
PAGE 64: Tapsi Navigation Deep-Dive
PAGE 65: Tapsi GPS Performance & Magical Window
PAGE 66: Speed Satisfaction (Snapp vs Tapsi)
PAGE 67: Snapp CarFix Deep-Dive (funnel, satisfaction, recommendation)
PAGE 68: Tapsi Garage Deep-Dive (funnel, satisfaction, recommendation)
PAGE 69: Mixed Incentive Strategy (awareness, activation, preferences)
PAGE 70: Request Refusal Reasons (wide binary)
PAGE 71: App Notification Channels (wide binary)
PAGE 72: Fix Location Feature & OS Distribution
PAGE 73: Incentive Rules Awareness & Preference
PAGE 74: Next-Week Usage Intent & Rate Passenger Feature

Usage:
    python survey_analysis_v6.py
"""

# ============================================================================
# IMPORTS
# ============================================================================
# PdfPages lets us write multiple matplotlib figures into a single PDF file,
# where each figure becomes one page.  This is how we build the 74-page report.
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.ticker as mticker         # Custom axis tick formatting (e.g., percentage labels)
import matplotlib.gridspec as gridspec       # Flexible grid layouts for complex multi-panel pages
import matplotlib.pyplot as plt              # Core plotting API for creating figures and axes
import matplotlib.patches as mpatches        # Manual legend entries (colored patches)
import matplotlib.patheffects as pe          # Text outline/shadow effects (imported for potential use)
import os
import sys
import warnings
import numpy as np                           # Numeric operations: NaN, arange, where, etc.
import pandas as pd                          # Data loading (read_csv), groupby, merge, value_counts

# "Agg" is a non-interactive backend -- it renders to files (PNG, PDF) without
# opening a window.  This is essential for server/CI environments or when we
# just want to write a PDF and exit.
matplotlib.use("Agg")

# Suppress matplotlib deprecation warnings and pandas SettingWithCopyWarning
# to keep console output clean.  Data issues are handled via explicit checks.
warnings.filterwarnings("ignore")

# ============================================================================
# CONFIGURATION -- File paths, color palette, and thresholds
# ============================================================================
# BASE points to the "processed/" folder where data_cleaning.py writes its
# output CSVs.  All 6 input files and the output PDF live here.
BASE = r"D:\Work\Driver Survey\processed"
SHORT_MAIN = os.path.join(BASE, "short_survey_main.csv")
SHORT_RARE = os.path.join(BASE, "short_survey_rare.csv")
WIDE_MAIN  = os.path.join(BASE, "wide_survey_main.csv")
WIDE_RARE  = os.path.join(BASE, "wide_survey_rare.csv")
LONG_MAIN  = os.path.join(BASE, "long_survey_main.csv")
LONG_RARE  = os.path.join(BASE, "long_survey_rare.csv")
OUTPUT_PDF = os.path.join(BASE, "driver_survey_analysis_v6.pdf")

# Color palette: Snapp = green, Tapsi = orange -- these are the official brand
# colors for Iran's two main rideshare platforms.  ACCENT/ACCENT2 are used for
# neutral or third-party data (e.g., navigation apps).  BG_COLOR gives every
# page a consistent light-grey background instead of pure white.
SNAPP_COLOR = "#00C853"          # Snapp brand green
TAPSI_COLOR = "#FF6D00"          # Tapsi brand orange
ACCENT = "#1565C0"               # Blue -- used for neutral / third-party data
ACCENT2 = "#7B1FA2"              # Purple -- secondary accent
GREY = "#9E9E9E"                 # Medium grey -- text labels and axis elements
LGREY = "#E0E0E0"               # Light grey -- "no data" or least important bars
BG_COLOR = "#FAFAFA"             # Near-white background for all figures
PLATFORM_COLORS = {"Snapp": SNAPP_COLOR, "Tapsi": TAPSI_COLOR}

# Minimum number of survey responses a week must have to be included in the
# analysis.  Weeks with fewer responses are dropped because small samples
# produce noisy, misleading averages in the weekly trend charts.
MIN_WEEK_RESPONSES = 100

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================
# These small utility functions are called many times throughout the 74-page
# PDF generation.  They reduce code duplication for common tasks like creating
# a figure, labeling bars, saving a page, and styling axes.
# ============================================================================


# Create a new matplotlib Figure + single Axes with a consistent background
# color and bold title.  Most single-chart pages start with this call.
# Returns (fig, ax) so the caller can draw on the axes and then call save_fig.
def new_fig(title, figsize=(12, 6)):
    fig, ax = plt.subplots(figsize=figsize, facecolor=BG_COLOR)
    ax.set_facecolor(BG_COLOR)
    fig.suptitle(title, fontsize=15, fontweight="bold", y=0.97)
    return fig, ax


# Add numeric labels on top of every bar in a bar chart.
# Iterates over all bar "containers" (one per call to ax.bar) and places
# the bar height value as text above each bar.  Skips zero-height bars.
def bar_label(ax, fmt="{:.0f}"):
    for container in ax.containers:
        labels = [fmt.format(v.get_height()) if v.get_height()
                  > 0 else "" for v in container]
        ax.bar_label(container, labels=labels, fontsize=8, padding=2)


# Finalize a figure and write it as the next page in the PDF.
# tight_layout(rect=...) adjusts spacing so the suptitle (y=0.97) does not
# overlap the subplots.  plt.close(fig) frees memory -- without this,
# hundreds of open figures would accumulate and crash the script.
def save_fig(pdf, fig):
    fig.tight_layout(rect=[0, 0, 1, 0.94])
    pdf.savefig(fig, facecolor=BG_COLOR)
    plt.close(fig)


# Calculate the Net Promoter Score (NPS) for a series of 0-10 ratings.
# NPS = % Promoters (score 9-10) minus % Detractors (score 0-6).
# Scores of 7-8 are "Passives" and do not count either way.
# NPS ranges from -100 (all detractors) to +100 (all promoters).
# A positive NPS means more people would recommend than not.
def nps_score(series):
    s = series.dropna()
    if len(s) == 0:
        return np.nan
    return (s >= 9).sum() / len(s) * 100 - (s <= 6).sum() / len(s) * 100


# Apply a clean, minimal style to an axes: light background and remove the
# top/right spines (border lines).  Called on virtually every axes in the
# report for visual consistency.
def style_ax(ax):
    ax.set_facecolor(BG_COLOR)
    ax.spines[["top", "right"]].set_visible(False)


# Reusable charting function: creates a 3-panel side-by-side bar chart page
# comparing Snapp vs Tapsi mean satisfaction across groups of drivers.
# The 3 panels correspond to the 3 SAT_PAIRS: Fare, Income, and Request Count.
#
# Parameters:
#   groupcol       - column to group by (e.g., "city", "cooperation_type")
#   top_n          - if set, only keep the N groups with the most responses
#   min_group_size - drop groups with fewer than this many responses
#   order          - if given, display groups in this specific order
#
# This function is called for Pages 30-32 and 34 (by cooperation type, city,
# driver type, and age group) to avoid repeating ~30 lines of code each time.
def plot_sat_by_group(pdf, df, groupcol, title_suffix, figsize=(14, 6),
                      top_n=None, min_group_size=10, order=None):
    grp_sizes = df.groupby(groupcol).size()
    valid_groups = grp_sizes[grp_sizes >= min_group_size].index
    df_f = df[df[groupcol].isin(valid_groups)]
    if top_n:
        top_groups = df_f[groupcol].value_counts().head(top_n).index
        df_f = df_f[df_f[groupcol].isin(top_groups)]
    if order:
        groups = [g for g in order if g in df_f[groupcol].unique()]
    else:
        groups = sorted(df_f[groupcol].dropna().unique(), key=str)
    if len(groups) == 0:
        return
    fig, axes = plt.subplots(1, 3, figsize=figsize,
                             facecolor=BG_COLOR, sharey=True)
    fig.suptitle(
        f"Avg Satisfaction by {title_suffix}", fontsize=15, fontweight="bold", y=0.99)
    x = np.arange(len(groups))
    w = 0.35
    for ax, (scol, tcol, label) in zip(axes, SAT_PAIRS):
        grp = df_f.groupby(groupcol).agg(
            snapp=(scol, "mean"), tapsi=(tcol, "mean")).reindex(groups)
        bars_s = ax.bar(x - w/2, grp["snapp"], w,
                        color=SNAPP_COLOR, label="Snapp")
        bars_t = ax.bar(x + w/2, grp["tapsi"], w,
                        color=TAPSI_COLOR, label="Tapsi")
        for bar in [*bars_s, *bars_t]:
            if not np.isnan(bar.get_height()) and bar.get_height() > 0:
                ax.text(bar.get_x()+bar.get_width()/2, bar.get_height()+0.05,
                        f"{bar.get_height():.2f}", ha="center", fontsize=7, fontweight="bold")
        ax.set_xticks(x)
        ax.set_xticklabels([str(g) for g in groups], fontsize=8,
                           rotation=40 if len(groups) > 5 else 0,
                           ha="right" if len(groups) > 5 else "center")
        ax.set_title(label, fontsize=11)
        ax.set_ylim(0, 5.5)
        ax.legend(frameon=False, fontsize=8)
        style_ax(ax)
    axes[0].set_ylabel("Mean Satisfaction (1-5)")
    save_fig(pdf, fig)


# Reusable charting function for long-format survey data: creates a 2-panel
# horizontal bar chart comparing answer distributions for a Snapp question
# vs its Tapsi equivalent.  Filters long_df where question == snapp_question
# or tapsi_question, then counts each unique answer value.
# Used for Pages 45-47 (Incentive Type, GotBonus, CS Category from long survey).
def plot_long_snapp_vs_tapsi(pdf, long_df, snapp_question, tapsi_question, title, figsize=(14, 6)):
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
            style_ax(ax)
            continue
        answer_counts = qdata["answer"].value_counts(
        ).sort_values(ascending=True)
        total = answer_counts.sum()
        ax.barh(answer_counts.index, answer_counts.values,
                color=color, edgecolor="white")
        for i, (ans, v) in enumerate(answer_counts.items()):
            ax.text(v+0.5, i, f"{v} ({v/total*100:.0f}%)",
                    va="center", fontsize=9)
        ax.set_title(f"{label}  (n={total})", fontsize=11)
        style_ax(ax)
    axes[0].set_xlabel("Count")
    axes[1].set_xlabel("Count")
    save_fig(pdf, fig)


# Create a placeholder PDF page when data for a chart is unavailable.
# Shows a centered grey warning box with the reason text.  This keeps the
# page numbering consistent even when certain survey questions were not asked.
def placeholder_page(pdf, title, reason="Data not available"):
    """Create a page with centered warning text when data is missing."""
    fig, ax = plt.subplots(figsize=(12, 6), facecolor=BG_COLOR)
    ax.set_facecolor(BG_COLOR)
    fig.suptitle(title, fontsize=15, fontweight="bold", y=0.97)
    ax.text(0.5, 0.5, reason, transform=ax.transAxes,
            fontsize=18, color=GREY, ha="center", va="center",
            fontweight="bold",
            bbox=dict(boxstyle="round,pad=1", facecolor=LGREY, edgecolor=GREY, alpha=0.5))
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")
    save_fig(pdf, fig)


# Safely load a CSV file, returning None (instead of crashing) if the file
# is missing.  Uses utf-8-sig encoding because data_cleaning.py writes CSVs
# with a BOM (byte order mark), which utf-8-sig strips automatically.
def safe_load(path):
    """Return pd.read_csv(path) or None if the file does not exist."""
    if not os.path.isfile(path):
        print(f"[WARN] File not found, skipping: {path}")
        return None
    return pd.read_csv(path, encoding="utf-8-sig", low_memory=False)


# Merge a "main" DataFrame with its corresponding "rare" DataFrame using a
# left join on recordID.  Only brings in columns from rare that are NOT
# already in main (to avoid duplicate column name conflicts).
# This is how we combine always-asked and rarely-asked questions into one
# unified DataFrame for each shape (short, wide).
def merge_main_rare(main_df, rare_df, key="recordID"):
    """Left-join rare columns into main, keeping only non-duplicate columns from rare."""
    if main_df is None:
        return rare_df
    if rare_df is None:
        return main_df
    # Only bring in columns from rare that are not already in main (plus the key)
    main_cols = set(main_df.columns)
    rare_extra = [c for c in rare_df.columns if c not in main_cols or c == key]
    return main_df.merge(rare_df[rare_extra], on=key, how="left")


# ============================================================================
# DATA LOADING
# ============================================================================
# Load all 6 CSV files.  Each may be None if the file is missing (safe_load
# prints a warning and returns None instead of crashing).
print("Loading data files...")
short_main = safe_load(SHORT_MAIN)
short_rare = safe_load(SHORT_RARE)
wide_main  = safe_load(WIDE_MAIN)
wide_rare  = safe_load(WIDE_RARE)
long_main  = safe_load(LONG_MAIN)
long_rare  = safe_load(LONG_RARE)

# --- Merge main + rare DataFrames ---
# For "short" and "wide" shapes, main and rare share the same recordID but
# have different columns.  We LEFT JOIN rare onto main so every driver row
# gets both the always-asked and rarely-asked columns in one DataFrame.
short = merge_main_rare(short_main, short_rare)
wide  = merge_main_rare(wide_main, wide_rare)

# For "long" format, main and rare have the same columns (recordID, question,
# answer) but different rows.  We simply stack (concatenate) them vertically.
if long_main is not None and long_rare is not None:
    long = pd.concat([long_main, long_rare], ignore_index=True)
elif long_main is not None:
    long = long_main.copy()
elif long_rare is not None:
    long = long_rare.copy()
else:
    long = None

# If short ended up None, create empty DataFrame
if short is None:
    short = pd.DataFrame()
    print("[WARN] No short data available -- report will be mostly empty.")

# Rename columns for backward compatibility.
# data_cleaning.py uses "snapp_CS" / "tapsi_CS_" as column names, but the
# charting code below references "snapp_customer_support" / "tapsi_customer_support".
# This rename bridges the gap without modifying either script.
rename_map = {}
if "snapp_CS" in short.columns:
    rename_map["snapp_CS"] = "snapp_customer_support"
if "tapsi_CS_" in short.columns:
    rename_map["tapsi_CS_"] = "tapsi_customer_support"
if rename_map:
    short.rename(columns=rename_map, inplace=True)

if wide is not None:
    wide_rename = {}
    if "snapp_CS" in wide.columns:
        wide_rename["snapp_CS"] = "snapp_customer_support"
    if "tapsi_CS_" in wide.columns:
        wide_rename["tapsi_CS_"] = "tapsi_customer_support"
    if wide_rename:
        wide.rename(columns=wide_rename, inplace=True)

# --- Ensure critical columns exist (add as NaN if missing) ---
# The charting code below references ~100+ column names.  If a column is
# missing (e.g., a question was not asked in this survey wave), the code
# would crash with a KeyError.  Instead, we pre-create every expected column
# filled with NaN so that charts simply show "no data" gracefully.
# This list covers satisfaction scores, CS metrics, GPS stages, navigation,
# incentives, CarFix/Garage, mixed incentive, and other feature columns.
_ensure = [
    "snapp_overall_satisfaction", "tapsi_overall_satisfaction",
    "snapp_recommend", "tapsidriver_tapsi_recommend",
    "snapp_CS_satisfaction_overall", "tapsi_CS_satisfaction_overall",
    "snapp_CS_satisfaction_waittime", "tapsi_CS_satisfaction_waittime",
    "snapp_CS_satisfaction_solution", "tapsi_CS_satisfaction_solution",
    "snapp_CS_satisfaction_behaviour", "tapsi_CS_satisfaction_behaviour",
    "snapp_CS_satisfaction_relevance", "tapsi_CS_satisfaction_relevance",
    "snapp_CS_satisfaction_important_reason", "tapsi_CS_satisfaction_important_reason",
    "snapp_CS_solved", "tapsi_CS_solved",
    "snapp_customer_support", "tapsi_customer_support",
    "snapp_collab_reason", "tapsi_collab_reason",
    "snapp_better_income", "tapsi_better_income",
    "snapp_comm_info", "tapsi_comm_info", "snapp_tax_info", "tapsi_tax_info",
    "snapp_gps_stage", "tapsi_gps_stage",
    "snapp_unpaid_by_passenger_followup", "tapsi_unpaid_by_passenger_followup",
    "snapp_compensate_unpaid_by_passenger", "tapsi_compensate_unpaid_by_passenger",
    "snapp_register_type", "tapsi_register_type",
    "snapp_main_reg_reason", "tapsi_main_reg_reason",
    "snapp_refer_others", "tapsi_refer_others",
    "snapp_ecoplus_familiar", "snapp_ecoplus_access_usage",
    "snapp_participate_feeling", "snapp_not_talking_reason",
    "demand_process", "missed_demand_per_10", "max_demand",
    "recommendation_googlemap", "recommendation_waze",
    "recommendation_neshan", "recommendation_balad",
    "snapp_accepted_trip_length", "tapsi_accepted_trip_length",
    "snapp_satisfaction_followup_overall", "tapsi_satisfaction_followup_overall",
    "snapp_satisfaction_followup_time", "tapsi_satisfaction_followup_time",
    "tapsi_in_app_navigation_usage", "tapsi_in_app_navigation_satisfaction",
    "tapsi_offline_navigation_familiar", "tapsi_offline_navigation_usage",
    "tapsi_gps_better", "snapp_navigation_app_satisfaction",
    "snapp_unsatisfaction_app_support", "tapsi_unsatisfaction_app_support",
    "snapp_speed_satisfaction", "tapsi_speed_satisfaction",
    "snappcarfix_familiar", "snappcarfix_use_ever", "snappcarfix_recommend",
    "snappcarfix_satisfaction_overall", "snappcarfix_satisfaction_experience",
    "snappcarfix_satisfaction_productprice", "snappcarfix_satisfaction_quality",
    "snappcarfix_satisfaction_variety", "snappcarfix_satisfaction_buyingprocess",
    "snappcarfix_satisfaction_deliverytime", "snappcarfix_satisfaction_waittime",
    "snappcarfix_satisfaction_behaviour", "snappcarfix_use_lastmo",
    "snappcarfix_satisfaction_quality_lastm", "snappcarfix_satisfaction_price_lastm",
    "snappcarfix_satisfaction_variety_lastm", "snappcarfix_satisfaction_easyusage",
    "snappcarfix_satisfaction_ontimedelivery", "snappcarfix_satisfaction_CS",
    "tapsigarage_familiar", "tapsigarage_use_ever", "tapsigarage_recommend",
    "tapsigarage_satisfaction_overall", "tapsigarage_satisfaction_experience",
    "tapsigarage_satisfaction_productprice",
    "tapsigarage_satisfaction_quality_experience",
    "tapsigarage_satisfaction_variety_experience",
    "tapsigarage_satisfaction_buyingprocess", "tapsigarage_satisfaction_deliverytime",
    "tapsigarage_satisfaction_waittime", "tapsigarage_satisfaction_behaviour",
    "tapsigarage_use_lastmo", "tapsigarage_satisfaction_quality",
    "tapsigarage_satisfaction_price", "tapsigarage_satisfaction_variety",
    "tapsigarage_satisfaction_easyusage", "tapsigarage_satisfaction_ontimedelivery",
    "tapsigarage_satisfaction_CS",
    "mixincentive", "mixincentive_activate_familiar",
    "mixincentive_tripeffect", "mixincentive_onlysnapp", "mixincentive_choice",
    "incentive_preference", "incentive_rules",
    "fixlocation_familiar", "fixlocation_use", "fixlocation_satisfaction",
    "OS", "snapp_use_nextweek", "ratepassenger_familiar_use",
]
for _c in _ensure:
    if _c not in short.columns:
        short[_c] = np.nan

# --- Filter: drop rows missing snapp_age ---
# snapp_age (tenure bucket like "<3 months", "1-3 years") is used as a key
# segmentation variable in many charts.  Rows without it are unusable.
if "snapp_age" in short.columns:
    before = len(short)
    short = short[short["snapp_age"].notna() & (short["snapp_age"] != "")].copy()
    dropped_age = before - len(short)
    if dropped_age > 0:
        print(f"Dropped {dropped_age} records with missing snapp_age")

# --- Sync wide and long to valid recordIDs ---
# After filtering short (dropping missing snapp_age), some recordIDs have been
# removed.  We filter wide and long to match, so all three DataFrames describe
# the exact same set of survey respondents.
valid_ids = set(short["recordID"].unique()) if "recordID" in short.columns else set()
if wide is not None and "recordID" in wide.columns:
    wide = wide[wide["recordID"].isin(valid_ids)].copy()
if long is not None and "recordID" in long.columns:
    long = long[long["recordID"].isin(valid_ids)].copy()

# ============================================================================
# DATETIME PARSING & YEARWEEK CONSTRUCTION
# ============================================================================
# "yearweek" is a compact numeric identifier for each survey week, formatted
# as YYWW (e.g., 2507 = year 2025, week 7).  It is used as the x-axis in
# all weekly trend charts.  We build it from year and weeknumber columns.
dfs_to_parse = [short]
if wide is not None:
    dfs_to_parse.append(wide)
if long is not None:
    dfs_to_parse.append(long)

for df in dfs_to_parse:
    df["datetime_parsed"] = pd.to_datetime(df["datetime"], errors="coerce")
    df["year"] = df["datetime_parsed"].dt.year
    df["weeknumber"] = pd.to_numeric(df["weeknumber"], errors="coerce")
    df["yearweek"] = (
        (df["year"] % 100) * 100 + df["weeknumber"]
    ).where(df["weeknumber"].notna() & df["year"].notna()).astype("Int64")

# --- Drop weeks with too few responses ---
# Weeks with < MIN_WEEK_RESPONSES (default: 100) are unreliable for computing
# averages.  Small samples cause extreme swings in weekly trend lines,
# which misleads stakeholders.  We drop them from ALL three DataFrames.
week_counts_all = short.groupby("yearweek").size()
valid_weeks = week_counts_all[week_counts_all >= MIN_WEEK_RESPONSES].index
dropped_weeks = week_counts_all[week_counts_all < MIN_WEEK_RESPONSES]
if len(dropped_weeks) > 0:
    print(
        f"Dropping {len(dropped_weeks)} week(s) with <{MIN_WEEK_RESPONSES} responses")

short = short[short["yearweek"].isin(valid_weeks)].copy()
if wide is not None:
    wide = wide[wide["yearweek"].isin(valid_weeks)].copy()
if long is not None:
    long = long[long["yearweek"].isin(valid_weeks)].copy()

# --- Build driver_type column ---
# Drivers who have zero Tapsi rides (tapsi_ride == 0) work exclusively for
# Snapp ("Snapp Exclusive").  All others are "Joint" drivers who work for
# both platforms.  This segmentation is central to understanding competitive
# dynamics -- do joint drivers rate Snapp lower because they have a benchmark?
if "tapsi_ride" in short.columns:
    short["driver_type"] = np.where(
        short["tapsi_ride"] == 0, "Snapp Exclusive", "Joint")
if wide is not None and "tapsi_ride" in wide.columns:
    wide["driver_type"] = np.where(
        wide["tapsi_ride"] == 0, "Snapp Exclusive", "Joint")
if long is not None and "tapsi_ride" in long.columns:
    long["driver_type"] = np.where(
        long["tapsi_ride"] == 0, "Snapp Exclusive", "Joint")

# Sort all DataFrames by yearweek so weekly trend charts plot in chronological order.
short.sort_values("yearweek", inplace=True)
if wide is not None:
    wide.sort_values("yearweek", inplace=True)
if long is not None:
    long.sort_values("yearweek", inplace=True)

# Ordered list of Snapp tenure buckets (snapp_age column values).
# TENURE_LABELS are shorter versions for chart axis labels.
TENURE_ORDER = ["less_than_3_months", "3_to_6_months", "6_months_to_1_year",
                "1_to_3_years", "3_to_5_years", "5_to_7_years", "more_than_7_years"]
TENURE_LABELS = ["<3 m", "3-6 m", "6m-1y", "1-3 y", "3-5 y", "5-7 y", ">7 y"]

# The 3 core satisfaction dimensions compared throughout the report.
# Each tuple is (snapp_column, tapsi_column, display_label).
# These drive the 3-panel charts in plot_sat_by_group and several other pages.
SAT_PAIRS = [
    ("snapp_fare_satisfaction",      "tapsi_fare_satisfaction",      "Fare"),
    ("snapp_income_satisfaction",    "tapsi_income_satisfaction",    "Income"),
    ("snapp_req_count_satisfaction", "tapsi_req_count_satisfaction", "Request Count"),
]

# Boolean flags for quick checks before attempting to build a chart.
# If HAVE_WIDE is False, pages that rely on binary multi-choice columns are skipped.
HAVE_SHORT = len(short) > 0
HAVE_WIDE = wide is not None and len(wide) > 0
HAVE_LONG = long is not None and len(long) > 0

print(
    f"Remaining: {len(short)} short, "
    f"{len(wide) if wide is not None else 0} wide, "
    f"{len(long) if long is not None else 0} long, "
    f"{short['yearweek'].nunique() if HAVE_SHORT else 0} weeks")
print(f"HAVE_SHORT={HAVE_SHORT}, HAVE_WIDE={HAVE_WIDE}, HAVE_LONG={HAVE_LONG}")

# ============================================================================
# PDF REPORT GENERATION
# ============================================================================
# The entire report is built inside a single `with PdfPages(...) as pdf:` block.
# Each "PAGE" section below creates one matplotlib Figure, draws charts on it,
# then calls save_fig(pdf, fig) which writes that figure as the next page in
# the PDF and closes the figure to free memory.
#
# The recurring pattern for most pages is:
#   1. Create figure & axes  (new_fig, plt.subplots, or plt.figure)
#   2. Prepare data          (groupby, value_counts, filtering)
#   3. Draw chart            (ax.bar, ax.barh, ax.plot, ax.pie, ax.scatter)
#   4. Add labels            (ax.text, bar_label, ax.annotate)
#   5. Style & save          (style_ax, save_fig)
#
# If required data is missing, placeholder_page() creates a "no data" page
# so that page numbering stays consistent across different survey waves.
# ============================================================================
with PdfPages(OUTPUT_PDF) as pdf:


    # ================================================================
    # PAGE 1 – COVER / KEY KPI SUMMARY
    # ================================================================
    # Executive dashboard showing high-level KPIs at a glance.
    # Business value: gives stakeholders instant context (sample size,
    # satisfaction scores, NPS, incentive spend) before diving into details.
    #
    # CHARTING PATTERN -- "manual axes placement" (fig.add_axes):
    # Instead of plt.subplots, we use fig.add_axes([left, bottom, width, height])
    # to place axes at exact positions.  This is useful for complex dashboard
    # layouts where subplots don't give enough control over spacing.
    # Coordinates are in figure-fraction units: (0,0)=bottom-left, (1,1)=top-right.
    n_total = len(short)
    n_weeks = short["yearweek"].nunique()
    n_cities = short["city"].nunique()
    n_joint_pct = (short["driver_type"] == "Joint").mean() * 100
    n_fulltime_pct = (short["cooperation_type"] == "Full-Time").mean() * 100
    snapp_sat_mean = short["snapp_overall_satisfaction"].mean()
    tapsi_sat_mean = short["tapsi_overall_satisfaction"].mean()
    snapp_nps_val = nps_score(short["snapp_recommend"])
    tapsi_nps_val = nps_score(short["tapsidriver_tapsi_recommend"])
    snapp_inc_mean = short["snapp_incentive"].mean() / 1e6
    tapsi_inc_mean = short["tapsi_incentive"].mean() / 1e6

    fig = plt.figure(figsize=(12, 8), facecolor=BG_COLOR)
    fig.suptitle("Driver Survey – Key Performance Indicators",
                 fontsize=18, fontweight="bold", y=0.97)
    ax_banner = fig.add_axes([0.05, 0.82, 0.9, 0.1])
    ax_banner.set_xlim(0, 10)
    ax_banner.set_ylim(0, 1)
    ax_banner.axis("off")
    kpis = [(f"{n_total:,}", "Responses"), (f"{n_weeks}", "Survey Weeks"),
            (f"{n_cities}", "Cities"), (f"{n_joint_pct:.0f}%", "Joint Drivers"),
            (f"{n_fulltime_pct:.0f}%", "Full-Time")]
    for i, (val, lbl) in enumerate(kpis):
        cx = 1 + i*2
        ax_banner.text(cx, 0.7, val, ha="center", fontsize=20,
                       fontweight="bold", color=ACCENT)
        ax_banner.text(cx, 0.1, lbl, ha="center", fontsize=10, color=GREY)

    ax_sat = fig.add_axes([0.05, 0.52, 0.42, 0.26])
    cats = ["Overall Sat.", "Fare Sat.", "Income Sat.", "Req-Count Sat."]
    s_vals = [short["snapp_overall_satisfaction"].mean(), short["snapp_fare_satisfaction"].mean(),
              short["snapp_income_satisfaction"].mean(), short["snapp_req_count_satisfaction"].mean()]
    t_vals = [short["tapsi_overall_satisfaction"].mean(), short["tapsi_fare_satisfaction"].mean(),
              short["tapsi_income_satisfaction"].mean(), short["tapsi_req_count_satisfaction"].mean()]
    x = np.arange(len(cats))
    w = 0.35
    ax_sat.bar(x-w/2, s_vals, w, color=SNAPP_COLOR, label="Snapp")
    ax_sat.bar(x+w/2, t_vals, w, color=TAPSI_COLOR, label="Tapsi")
    for xi, (sv, tv) in enumerate(zip(s_vals, t_vals)):
        ax_sat.text(xi-w/2, sv+0.05, f"{sv:.2f}",
                    ha="center", fontsize=8, fontweight="bold")
        ax_sat.text(xi+w/2, tv+0.05, f"{tv:.2f}",
                    ha="center", fontsize=8, fontweight="bold")
    ax_sat.set_xticks(x)
    ax_sat.set_xticklabels(cats, fontsize=9, rotation=15, ha="right")
    ax_sat.set_ylim(0, 5.5)
    ax_sat.set_title("Mean Satisfaction (1–5)", fontsize=11)
    ax_sat.legend(frameon=False, fontsize=9)
    style_ax(ax_sat)

    ax_nps = fig.add_axes([0.55, 0.52, 0.4, 0.26])
    metrics = ["NPS", "Avg Incentive\n(M Rials)", "CS Satisfaction\n(Overall)"]
    s_m = [snapp_nps_val, snapp_inc_mean,
           short["snapp_CS_satisfaction_overall"].mean()]
    t_m = [tapsi_nps_val, tapsi_inc_mean,
           short["tapsi_CS_satisfaction_overall"].mean()]
    x2 = np.arange(len(metrics))
    ax_nps.bar(x2-w/2, s_m, w, color=SNAPP_COLOR, label="Snapp")
    ax_nps.bar(x2+w/2, t_m, w, color=TAPSI_COLOR, label="Tapsi")
    for xi, (sv, tv) in enumerate(zip(s_m, t_m)):
        ax_nps.text(xi-w/2, max(sv, 0)+0.2,
                    f"{sv:.1f}", ha="center", fontsize=8, fontweight="bold")
        ax_nps.text(xi+w/2, max(tv, 0)+0.2,
                    f"{tv:.1f}", ha="center", fontsize=8, fontweight="bold")
    ax_nps.set_xticks(x2)
    ax_nps.set_xticklabels(metrics, fontsize=9)
    ax_nps.axhline(0, color=GREY, linewidth=0.8, linestyle="--")
    ax_nps.set_title("NPS, Incentive & CS Overview", fontsize=11)
    ax_nps.legend(frameon=False, fontsize=9)
    style_ax(ax_nps)

    ax_demo = fig.add_axes([0.05, 0.28, 0.9, 0.18])
    style_ax(ax_demo)
    ax_demo.set_xlim(0, 10)
    ax_demo.set_ylim(0, 1)
    ax_demo.axis("off")
    gender_m = (short["gender"] == "Male").mean() * 100
    age_u35 = (short["age"].isin(["18_25", "26_35", "<18"])).mean() * 100
    part_time = (short["cooperation_type"] == "Part-Time").mean() * 100
    top_job = short["original_job"].value_counts().index[0]
    top_job_pct = short["original_job"].value_counts().iloc[0] / \
        len(short) * 100
    top_city = short["city"].value_counts().index[0]
    top_city_pct = short["city"].value_counts().iloc[0] / len(short) * 100
    demo_items = [(f"{gender_m:.0f}%", "Male Drivers"), (f"{age_u35:.0f}%", "Under 35"),
                  (f"{part_time:.0f}%", "Part-Time"), (f"{top_job_pct:.0f}%",
                                                       f"#1 Job:\n{top_job}"),
                  (f"{top_city_pct:.0f}%", f"Top City:\n{top_city}")]
    for i, (val, lbl) in enumerate(demo_items):
        cx = 1+i*2
        ax_demo.text(cx, 0.75, val, ha="center", fontsize=16,
                     fontweight="bold", color=ACCENT2)
        ax_demo.text(cx, 0.1, lbl, ha="center", fontsize=9, color=GREY)
    ax_demo.set_title("Demographics Snapshot",
                      fontsize=11, x=0.5, y=1.0, pad=0)

    ax_ins = fig.add_axes([0.05, 0.04, 0.9, 0.2])
    style_ax(ax_ins)
    ax_ins.axis("off")
    insights = [
        "• Tapsi CS resolution rate ~60% vs Snapp ~29% — large satisfaction gap (3.52 vs 2.56)",
        "• Honeymoon effect: new Snapp drivers (<3 months) rate satisfaction 3.25 vs 2.65 for veterans",
        "• Full compensation for unpaid fares: Tapsi 63% vs Snapp 40% — significant driver trust gap",
        "• Navigation: Neshan used by 68% in last Snapp trip; Tapsi drivers use in-app nav at 23%",
        "• Snapp App NPS vs Platform NPS — see page 58 for app vs platform recommendation split",
        "• Tapsi re-activation: 13% of incentive-responding drivers were inactive >6 months — page 57",
    ]
    for i, ins in enumerate(insights):
        ax_ins.text(0.01, 0.92-i*0.17, ins, fontsize=9.5, transform=ax_ins.transAxes,
                    color="#333333", va="top")
    pdf.savefig(fig, facecolor=BG_COLOR)
    plt.close(fig)

    # ================================================================
    # PAGE 2 – RESPONSE COUNT BY YEARWEEK
    # ================================================================
    # Shows how many survey responses were collected each week.
    # Business value: reveals survey cadence and any weeks with low coverage.
    #
    # CHARTING PATTERN -- "simple vertical bar chart":
    # ax.bar(x_labels, heights, color=...) creates a vertical bar chart.
    # bar_label(ax) adds the numeric height as text above each bar.
    # This is the simplest charting pattern used throughout the report.
    week_counts = short.groupby("yearweek").size()
    fig, ax = new_fig("Weekly Response Count (by Year-Week)")
    ax.bar(week_counts.index.astype(str), week_counts.values,
           color=ACCENT, edgecolor="white", linewidth=0.5)
    bar_label(ax)
    ax.set_xlabel("Year-Week")
    ax.set_ylabel("Responses")
    ax.tick_params(axis="x", rotation=45)
    style_ax(ax)
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 3 – DEMOGRAPHICS OVERVIEW
    # ================================================================
    # 4-panel overview of who the surveyed drivers are (age, education,
    # cooperation type, marital status).
    # Business value: ensures the sample is representative before drawing
    # conclusions from satisfaction data.
    #
    # CHARTING PATTERN -- "2x2 subplots with horizontal bars":
    # plt.subplots(2, 2) creates a 2-row, 2-column grid of axes.
    # We loop over axes and draw ax.barh() (horizontal bar) on each panel,
    # then annotate each bar with "count (percentage%)" text.
    fig, axes = plt.subplots(2, 2, figsize=(12, 8), facecolor=BG_COLOR)
    fig.suptitle("Demographics Overview", fontsize=15,
                 fontweight="bold", y=0.97)
    ax = axes[0, 0]
    ag = short["age_group"].value_counts()
    ax.barh(ag.index, ag.values, color=[ACCENT, GREY])
    ax.set_title("Age Group", fontsize=11)
    for i, v in enumerate(ag.values):
        ax.text(v+5, i, f"{v} ({v/len(short)*100:.0f}%)",
                va="center", fontsize=9)
    ax = axes[0, 1]
    ed = short["edu"].value_counts().sort_index()
    ax.barh(["High School\nor Below", "College+"],
            ed.values, color=[GREY, ACCENT])
    ax.set_title("Education", fontsize=11)
    for i, v in enumerate(ed.values):
        ax.text(v+5, i, f"{v} ({v/len(short)*100:.0f}%)",
                va="center", fontsize=9)
    ax = axes[1, 0]
    ct = short["cooperation_type"].value_counts()
    ax.barh(ct.index, ct.values, color=[SNAPP_COLOR, TAPSI_COLOR])
    ax.set_title("Cooperation Type", fontsize=11)
    for i, v in enumerate(ct.values):
        ax.text(v+5, i, f"{v} ({v/len(short)*100:.0f}%)",
                va="center", fontsize=9)
    ax = axes[1, 1]
    ms = short["marr_stat"].value_counts().sort_index()
    ax.barh(["Single", "Married"], ms.values, color=[GREY, ACCENT])
    ax.set_title("Marital Status", fontsize=11)
    for i, v in enumerate(ms.values):
        ax.text(v+5, i, f"{v} ({v/len(short)*100:.0f}%)",
                va="center", fontsize=9)
    for row in axes:
        for ax in row:
            style_ax(ax)
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 4 – OCCUPATION BREAKDOWN
    # ================================================================
    # Top 15 primary occupations of surveyed drivers.
    # Business value: shows whether drivers are career drivers or side-giggers,
    # which affects how they perceive fare and incentive adequacy.
    job_counts = short["original_job"].value_counts().head(15)
    fig, ax = new_fig(
        "Primary Occupation of Surveyed Drivers (Top 15)", figsize=(12, 7))
    colors = [ACCENT if i < 5 else LGREY for i in range(len(job_counts))]
    ax.barh(job_counts.index[::-1], job_counts.values[::-1],
            color=colors[::-1], edgecolor="white")
    total = len(short)
    for i, v in enumerate(job_counts.values[::-1]):
        ax.text(v+30, i, f"{v:,} ({v/total*100:.1f}%)",
                va="center", fontsize=9)
    ax.set_xlabel("Response Count")
    style_ax(ax)
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 5 – ACTIVE JOINT RATE BY YEARWEEK
    # ================================================================
    # Percentage of surveyed drivers who also drive for Tapsi, plotted over time.
    # Business value: tracks competitive pressure -- a rising joint rate means
    # more drivers are hedging across platforms.
    #
    # CHARTING PATTERN -- "line trend chart":
    # ax.plot(x, y, marker="o") draws a line with circle markers.
    # ax.annotate() places the numeric value near each data point.
    # Used for all weekly trend pages (rides, NPS, incentive, satisfaction).
    weekly_joint = short.groupby("yearweek").agg(
        total=("active_joint", "size"), active=("active_joint", "sum"))
    weekly_joint["rate"] = weekly_joint["active"] / weekly_joint["total"] * 100
    fig, ax = new_fig("Active Joint (Tapsi) Rate by Year-Week")
    ax.plot(weekly_joint.index.astype(
        str), weekly_joint["rate"], marker="o", color=TAPSI_COLOR, linewidth=2.5, markersize=8)
    for idx, row in weekly_joint.iterrows():
        ax.annotate(f"{row['rate']:.0f}%", (str(idx), row["rate"]),
                    textcoords="offset points", xytext=(0, 10), ha="center", fontsize=9)
    ax.set_xlabel("Year-Week")
    ax.set_ylabel("Active Joint Rate (%)")
    ax.set_ylim(0, 100)
    ax.tick_params(axis="x", rotation=45)
    style_ax(ax)
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 6 – AVERAGE RIDE COUNTS
    # ================================================================
    # Average weekly rides per driver on Snapp vs Tapsi over time.
    # Business value: reveals whether drivers are doing more rides on one
    # platform -- a leading indicator of platform preference and earnings.
    # Same line-trend pattern as PAGE 5.
    weekly_rides = short.groupby("yearweek").agg(snapp_ride=(
        "snapp_ride", "mean"), tapsi_ride=("tapsi_ride", "mean"))
    fig, ax = new_fig("Average Weekly Ride Count – Snapp vs Tapsi")
    ax.plot(weekly_rides.index.astype(
        str), weekly_rides["snapp_ride"], marker="o", color=SNAPP_COLOR, linewidth=2.5, label="Snapp")
    ax.plot(weekly_rides.index.astype(
        str), weekly_rides["tapsi_ride"], marker="s", color=TAPSI_COLOR, linewidth=2.5, label="Tapsi")
    ax.legend(frameon=False, fontsize=10)
    ax.set_xlabel("Year-Week")
    ax.set_ylabel("Avg Rides")
    ax.tick_params(axis="x", rotation=45)
    style_ax(ax)
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 7 – SATISFACTION COMPARISON
    # ================================================================
    # Side-by-side bar chart comparing Snapp vs Tapsi mean satisfaction
    # across the 3 core dimensions (Fare, Income, Request Count).
    # Business value: the most direct competitive comparison -- which
    # platform are drivers more satisfied with, and in which areas?
    #
    # CHARTING PATTERN -- "side-by-side (grouped) bar chart":
    # Uses offset x-positions: Snapp bars at (x - width/2), Tapsi at (x + width/2).
    # This is the standard matplotlib technique for grouped bars.
    # ax.bar(x - w/2, snapp_vals, w)  draws the left group (Snapp).
    # ax.bar(x + w/2, tapsi_vals, w)  draws the right group (Tapsi).
    # Then loop over bars to place value labels on top of each bar.
    fig, axes = plt.subplots(1, 3, figsize=(
        14, 5), facecolor=BG_COLOR, sharey=True)
    fig.suptitle("Satisfaction Comparison (1–5 scale): Snapp vs Tapsi",
                 fontsize=15, fontweight="bold", y=0.99)
    for ax, (scol, tcol, label) in zip(axes, SAT_PAIRS):
        snapp_mean = short[scol].dropna().mean()
        tapsi_mean = short[tcol].dropna().mean()
        bars = ax.bar(["Snapp", "Tapsi"], [snapp_mean, tapsi_mean], color=[
                      SNAPP_COLOR, TAPSI_COLOR], width=0.5, edgecolor="white")
        ax.set_title(label, fontsize=11)
        ax.set_ylim(0, 5.5)
        style_ax(ax)
        for bar in bars:
            ax.text(bar.get_x()+bar.get_width()/2, bar.get_height()+0.08,
                    f"{bar.get_height():.2f}", ha="center", fontsize=10, fontweight="bold")
    axes[0].set_ylabel("Mean Satisfaction")
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 8 – OVERALL SATISFACTION DISTRIBUTION
    # ================================================================
    # Histogram of 1-5 satisfaction ratings for Snapp and Tapsi side by side.
    # Business value: shows whether satisfaction is skewed (e.g., mostly 2s and 3s)
    # or bimodal (many 1s and 5s), which has different implications than just the mean.
    #
    # CHARTING PATTERN -- "paired histograms (1x2 subplots)":
    # plt.subplots(1, 2, sharey=True) creates two panels sharing the y-axis.
    # We loop: for each platform, count occurrences of each rating (1-5) using
    # value_counts().sort_index(), then draw ax.bar() with those counts.
    fig, axes = plt.subplots(1, 2, figsize=(
        12, 5), facecolor=BG_COLOR, sharey=True)
    fig.suptitle("Overall Satisfaction Distribution (1–5 scale)",
                 fontsize=15, fontweight="bold", y=0.99)
    for ax, col, color, label in [(axes[0], "snapp_overall_satisfaction", SNAPP_COLOR, "Snapp"),
                                  (axes[1], "tapsi_overall_satisfaction", TAPSI_COLOR, "Tapsi")]:
        data = short[col].dropna()
        counts = data.value_counts().sort_index()
        ax.bar(counts.index.astype(int).astype(str),
               counts.values, color=color, edgecolor="white")
        total = counts.sum()
        for x, y in zip(counts.index.astype(int).astype(str), counts.values):
            ax.text(x, y+1, f"{y}\n({y/total*100:.0f}%)",
                    ha="center", fontsize=8)
        ax.set_title(
            f"{label}  (n={int(total)}, mean={data.mean():.2f})", fontsize=11)
        ax.set_xlabel("Rating")
        style_ax(ax)
    axes[0].set_ylabel("Count")
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 9 – NPS BY YEARWEEK
    # ================================================================
    # Net Promoter Score trend over time for both platforms.
    # Business value: NPS is a widely-used loyalty metric.  Tracking it weekly
    # reveals whether recent changes (fare adjustments, app updates) improved
    # or hurt driver willingness to recommend the platform.
    # Same line-trend pattern as PAGE 5.
    nps_weekly = short.groupby("yearweek").agg(
        snapp_nps=("snapp_recommend", nps_score), tapsi_nps=("tapsidriver_tapsi_recommend", nps_score)).dropna(how="all")
    fig, ax = new_fig("NPS (Net Promoter Score) by Year-Week – Snapp vs Tapsi")
    if not nps_weekly["snapp_nps"].isna().all():
        ax.plot(nps_weekly.index.astype(
            str), nps_weekly["snapp_nps"], marker="o", color=SNAPP_COLOR, linewidth=2.5, label="Snapp")
    if not nps_weekly["tapsi_nps"].isna().all():
        ax.plot(nps_weekly.index.astype(
            str), nps_weekly["tapsi_nps"], marker="s", color=TAPSI_COLOR, linewidth=2.5, label="Tapsi")
    ax.axhline(0, color=GREY, linestyle="--", linewidth=0.8)
    ax.legend(frameon=False, fontsize=10)
    ax.set_xlabel("Year-Week")
    ax.set_ylabel("NPS")
    ax.tick_params(axis="x", rotation=45)
    style_ax(ax)
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 10 – INCENTIVE CATEGORY BREAKDOWN
    # ================================================================
    # Distribution of incentive categories (e.g., commission-free, pay-after-ride).
    # Business value: shows which incentive types each platform relies on most.
    # Same paired-horizontal-bar pattern as PAGE 8 but with barh instead of bar.
    fig, axes = plt.subplots(1, 2, figsize=(12, 5), facecolor=BG_COLOR)
    fig.suptitle("Incentive Category Distribution",
                 fontsize=15, fontweight="bold", y=0.99)
    for ax, col, color, label in [(axes[0], "snapp_incentive_category", SNAPP_COLOR, "Snapp"),
                                  (axes[1], "tapsi_incentive_category", TAPSI_COLOR, "Tapsi")]:
        data = short[col].dropna().value_counts()
        ax.barh(data.index, data.values, color=color, edgecolor="white")
        total = data.sum()
        for i, v in enumerate(data.values):
            ax.text(v+2, i, f"{v} ({v/total*100:.0f}%)",
                    va="center", fontsize=9)
        ax.set_title(f"{label}  (n={total})", fontsize=11)
        style_ax(ax)
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 11 – INCENTIVE TYPE USAGE (wide_survey)
    # ================================================================
    # Counts how many drivers selected each incentive type (multi-choice).
    # Data comes from wide_survey binary columns (1 = driver chose this type).
    # Business value: reveals which incentive structures are most popular.
    #
    # CHARTING PATTERN -- "wide binary column aggregation":
    # For multi-choice questions, data_cleaning.py creates one binary column
    # per answer option (e.g., "Snapp Incentive Type__Pay After Ride").
    # We sum each column (wide[col].sum()) to get the total count of drivers
    # who selected that option, then plot as horizontal bars.
    # This pattern recurs on pages 12, 16, 17, 21, 22, 33-34, 70-71.
    incentive_types = {
        "Snapp": {"Pay After Ride": "Snapp Incentive Type__Pay After Ride",
                  "Ride-Based Comm-free": "Snapp Incentive Type__Ride-Based Commission-free",
                  "Earning-Based Comm-free": "Snapp Incentive Type__Earning-based Commission-free",
                  "Income Guarantee": "Snapp Incentive Type__Income Guarantee",
                  "Pay After Income": "Snapp Incentive Type__Pay After Income"},
        "Tapsi": {"Pay After Ride": "Tapsi Incentive Type__Pay After Ride",
                  "Ride-Based Comm-free": "Tapsi Incentive Type__Ride-Based Commission-free",
                  "Earning-Based Comm-free": "Tapsi Incentive Type__Earning-based Commission-free",
                  "Income Guarantee": "Tapsi Incentive Type__Income Guarantee",
                  "Pay After Income": "Tapsi Incentive Type__Pay After Income"},
    }
    fig, axes = plt.subplots(1, 2, figsize=(
        14, 6), facecolor=BG_COLOR, sharey=True)
    fig.suptitle("Incentive Type Usage – wide_survey (multi-choice)",
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
            ax.text(v+5, i, str(int(v)), va="center", fontsize=9)
        ax.set_title(platform, fontsize=12)
        style_ax(ax)
    axes[0].invert_yaxis()
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 12 – INCENTIVE UNSATISFACTION REASONS
    # ================================================================
    # Why drivers are unsatisfied with incentives (multi-choice from wide survey).
    # Business value: actionable feedback for incentive program redesign.
    # Same wide-binary-column pattern as PAGE 11.
    unsat_types = {
        "Snapp": {"Improper Amount": "Snapp Incentive Unsatisfaction__Improper Amount",
                  "Difficult": "Snapp Incentive Unsatisfaction__difficult",
                  "No Time": "Snapp Incentive Unsatisfaction__No Time todo",
                  "Not Available": "Snapp Incentive Unsatisfaction__No Available Time",
                  "Non Payment": "Snapp Incentive Unsatisfaction__Non Payment"},
        "Tapsi": {"Improper Amount": "Tapsi Incentive Unsatisfaction__Improper Amount",
                  "Difficult": "Tapsi Incentive Unsatisfaction__difficult",
                  "No Time": "Tapsi Incentive Unsatisfaction__No Time todo",
                  "Not Available": "Tapsi Incentive Unsatisfaction__Not Available",
                  "Non Payment": "Tapsi Incentive Unsatisfaction__Non Payment"},
    }
    fig, axes = plt.subplots(1, 2, figsize=(
        14, 5), facecolor=BG_COLOR, sharey=True)
    fig.suptitle("Incentive Unsatisfaction Reasons – wide_survey",
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
            ax.text(v+2, i, str(int(v)), va="center", fontsize=9)
        ax.set_title(platform, fontsize=12)
        style_ax(ax)
    axes[0].invert_yaxis()
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 13 – AVERAGE INCENTIVE (RIALS) BY YEARWEEK
    # ================================================================
    # Weekly trend of average monetary incentive paid per driver, in million Rials.
    # Business value: tracks incentive spend efficiency over time.
    # Same line-trend pattern as PAGE 5.
    weekly_inc = short.groupby("yearweek").agg(
        snapp=("snapp_incentive", "mean"), tapsi=("tapsi_incentive", "mean"))
    fig, ax = new_fig("Average Monetary Incentive by Year-Week (Rials)")
    ax.plot(weekly_inc.index.astype(
        str), weekly_inc["snapp"]/1e6, marker="o", color=SNAPP_COLOR, linewidth=2.5, label="Snapp")
    ax.plot(weekly_inc.index.astype(
        str), weekly_inc["tapsi"]/1e6, marker="s", color=TAPSI_COLOR, linewidth=2.5, label="Tapsi")
    ax.legend(frameon=False, fontsize=10)
    ax.set_xlabel("Year-Week")
    ax.set_ylabel("Avg Incentive (Million Rials)")
    ax.tick_params(axis="x", rotation=45)
    style_ax(ax)
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 14 – INCENTIVE SATISFACTION DISTRIBUTION
    # ================================================================
    # Histogram of incentive satisfaction ratings (1-5) for each platform.
    # Business value: shows how drivers feel about the incentive amounts they receive.
    # Same paired-histogram pattern as PAGE 8.
    fig, axes = plt.subplots(1, 2, figsize=(
        12, 5), facecolor=BG_COLOR, sharey=True)
    fig.suptitle("Incentive Satisfaction Distribution (1–5 scale)",
                 fontsize=15, fontweight="bold", y=0.99)
    for ax, col, color, label in [(axes[0], "snapp_overall_incentive_satisfaction", SNAPP_COLOR, "Snapp"),
                                  (axes[1], "tapsi_overall_incentive_satisfaction", TAPSI_COLOR, "Tapsi")]:
        data = short[col].dropna()
        counts = data.value_counts().sort_index()
        ax.bar(counts.index.astype(int).astype(str),
               counts.values, color=color, edgecolor="white")
        total = counts.sum()
        for x, y in zip(counts.index.astype(int).astype(str), counts.values):
            ax.text(x, y+1, f"{y}\n({y/total*100:.0f}%)",
                    ha="center", fontsize=8)
        ax.set_title(
            f"{label}  (n={int(total)}, mean={data.mean():.2f})", fontsize=11)
        ax.set_xlabel("Rating")
        style_ax(ax)
    axes[0].set_ylabel("Count")
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 15 – LOC DISTRIBUTION
    # ================================================================
    # Histogram of how many months each driver has cooperated with each platform.
    # Business value: reveals driver retention -- a long tail means good retention,
    # while a spike at low months means high churn.
    #
    # CHARTING PATTERN -- "histogram":
    # ax.hist(data, bins=20) creates a frequency histogram.
    # ax.axvline(data.mean()) draws a vertical dashed line at the mean.
    fig, axes = plt.subplots(1, 2, figsize=(
        12, 5), facecolor=BG_COLOR, sharey=True)
    fig.suptitle("Length of Cooperation Distribution (months)",
                 fontsize=15, fontweight="bold", y=0.99)
    for ax, col, color, label in [(axes[0], "snapp_LOC", SNAPP_COLOR, "Snapp"),
                                  (axes[1], "tapsi_LOC", TAPSI_COLOR, "Tapsi")]:
        data = short[col].dropna()
        ax.hist(data, bins=20, color=color, edgecolor="white", alpha=0.85)
        ax.axvline(data.mean(), color="black", linestyle="--", linewidth=1.2)
        ax.text(data.mean()+1, ax.get_ylim()
                [1]*0.9, f"Mean: {data.mean():.1f}", fontsize=9)
        ax.set_title(f"{label}  (n={len(data)})", fontsize=11)
        ax.set_xlabel("Months")
        style_ax(ax)
    axes[0].set_ylabel("Count")
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 16 – RIDE REFUSAL REASONS
    # ================================================================
    # Why drivers refuse ride requests, compared side-by-side for both platforms.
    # Data comes from wide survey binary columns.
    # Business value: "Insufficient Fare" being #1 is actionable pricing feedback.
    #
    # CHARTING PATTERN -- "paired horizontal bar chart (dodge)":
    # Two sets of barh at offset y-positions: y - h/2 (Snapp) and y + h/2 (Tapsi).
    # This is the horizontal equivalent of the side-by-side bar pattern from PAGE 7.
    # ax.invert_yaxis() puts the first category at the top (more natural reading).
    refusal_labels = ["Insufficient Fare", "Distance to Origin", "Wait for Better", "Long Trip",
                      "Target Destination", "Traffic", "Short Accept Time", "Unfamiliar Route",
                      "Internet Problems", "App Problems", "App was Unfamiliar", "Was Working w/ Other"]
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
        "Snapp Ride Refusal Reasons__App was Unfamiliar",
        "Snapp Ride Refusal Reasons__Was Working with Tapsi",
    ]
    tapsi_refusal_cols = [c.replace("Snapp", "Tapsi")
                          for c in snapp_refusal_cols]
    snapp_vals = [
        wide[c].sum() if c in wide.columns else 0 for c in snapp_refusal_cols]
    tapsi_vals = [
        wide[c].sum() if c in wide.columns else 0 for c in tapsi_refusal_cols]
    fig, ax = new_fig(
        "Ride Refusal Reasons – Snapp vs Tapsi (wide_survey)", figsize=(14, 7))
    y = np.arange(len(refusal_labels))
    h = 0.35
    ax.barh(y-h/2, snapp_vals, h, color=SNAPP_COLOR,
            label="Snapp", edgecolor="white")
    ax.barh(y+h/2, tapsi_vals, h, color=TAPSI_COLOR,
            label="Tapsi", edgecolor="white")
    ax.set_yticks(y)
    ax.set_yticklabels(refusal_labels)
    ax.invert_yaxis()
    ax.legend(frameon=False, fontsize=10)
    ax.set_xlabel("Count")
    style_ax(ax)
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 17 – CUSTOMER SUPPORT CATEGORY
    # ================================================================
    # Which categories (fare, cancelling, technical, etc.) drivers contact CS about.
    # Business value: helps CS teams prioritize staffing for the most common issues.
    # Same paired-horizontal-bar (dodge) pattern as PAGE 16.
    cs_labels = ["Fare", "Cancelling", "Trip Problems", "Petrol", "Technical",
                 "Settlement", "Incentive", "Location Change", "Drivers Club", "Registration"]
    snapp_cs = [f"Snapp Customer Support Category__{l}" for l in cs_labels]
    tapsi_cs = [
        f"Tapsi Customer Support Category__{l.replace('Location Change', 'Loc Change')}" for l in cs_labels]
    snapp_cs_vals = [
        wide[c].sum() if c in wide.columns else 0 for c in snapp_cs]
    tapsi_cs_vals = [
        wide[c].sum() if c in wide.columns else 0 for c in tapsi_cs]
    fig, ax = new_fig(
        "Customer Support Ticket Categories – wide_survey", figsize=(14, 6))
    y = np.arange(len(cs_labels))
    h = 0.35
    ax.barh(y-h/2, snapp_cs_vals, h, color=SNAPP_COLOR,
            label="Snapp", edgecolor="white")
    ax.barh(y+h/2, tapsi_cs_vals, h, color=TAPSI_COLOR,
            label="Tapsi", edgecolor="white")
    ax.set_yticks(y)
    ax.set_yticklabels(cs_labels)
    ax.invert_yaxis()
    ax.legend(frameon=False, fontsize=10)
    ax.set_xlabel("Count")
    style_ax(ax)
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 18 – NAVIGATION APP ADOPTION FUNNEL (long_survey)
    # ================================================================
    # 3-stage funnel (Familiar -> Installed -> Used) for 4 navigation apps.
    # Data comes from the long survey where question = "Navigation Familiarity" etc.
    # Business value: shows which nav apps are adopted vs just known, informing
    # potential partnerships.
    #
    # CHARTING PATTERN -- "long-survey question filter":
    # long_df[long_df["question"] == stage] filters to rows for one question,
    # then we count how many rows have each answer value.
    # This is the standard way to extract data from the melted long format.
    nav_apps = ["Google Map", "Waze", "Neshan", "Balad"]
    nav_colors = [ACCENT, "#FFA726", "#66BB6A", "#AB47BC"]
    stages = ["Navigation Familiarity",
              "Navigation Installed", "Navigation Used"]
    stage_labels = ["Familiarity", "Installed", "Used"]
    fig, axes = plt.subplots(1, 3, figsize=(
        15, 5), facecolor=BG_COLOR, sharey=True)
    fig.suptitle("Navigation App Adoption Funnel – long_survey",
                 fontsize=15, fontweight="bold", y=0.99)
    for ax, stage, slbl in zip(axes, stages, stage_labels):
        qdata = long[long["question"] == stage]
        vals = [len(qdata[qdata["answer"] == app]) for app in nav_apps]
        ax.barh(nav_apps, vals, color=nav_colors, edgecolor="white")
        for i, v in enumerate(vals):
            ax.text(v+10, i, str(int(v)), va="center", fontsize=9)
        ax.set_title(slbl, fontsize=12)
        style_ax(ax)
    axes[0].invert_yaxis()
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 19 – NAVIGATION APP RATINGS (0–10 scale)
    # ================================================================
    # Mean recommendation score (0-10) for each nav app, plus a distribution
    # comparison of Neshan vs Balad (the two Iranian-built apps).
    # Business value: quantifies driver preference among navigation tools.
    nav_rating_cols = {"Google Maps": "recommendation_googlemap", "Waze": "recommendation_waze",
                       "Neshan": "recommendation_neshan", "Balad": "recommendation_balad"}
    fig, axes = plt.subplots(1, 2, figsize=(14, 6), facecolor=BG_COLOR)
    fig.suptitle("Navigation App Ratings by Drivers (0–10 scale)",
                 fontsize=15, fontweight="bold", y=0.99)
    ax = axes[0]
    apps = list(nav_rating_cols.keys())
    means = [short[c].dropna().mean() for c in nav_rating_cols.values()]
    ns = [short[c].notna().sum() for c in nav_rating_cols.values()]
    colors_ = [ACCENT, "#FFA726", "#66BB6A", "#AB47BC"]
    bars = ax.bar(apps, means, color=colors_, edgecolor="white", width=0.6)
    for bar, mean, n in zip(bars, means, ns):
        ax.text(bar.get_x()+bar.get_width()/2, mean+0.1,
                f"{mean:.2f}\n(n={n:,})", ha="center", fontsize=9, fontweight="bold")
    ax.set_ylim(0, 11)
    ax.set_ylabel("Mean Rating (0–10)")
    ax.set_title("Mean Recommendation Score", fontsize=11)
    style_ax(ax)
    ax = axes[1]
    for col, color, label in [("recommendation_neshan", "#66BB6A", "Neshan"),
                              ("recommendation_balad", "#AB47BC", "Balad")]:
        data = short[col].dropna()
        vals = data.value_counts().sort_index()
        ax.plot(vals.index, vals.values, marker="o", color=color,
                linewidth=2, label=f"{label} (n={len(data):,})")
    ax.set_xlabel("Rating (0–10)")
    ax.set_ylabel("Count")
    ax.set_title("Rating Distribution: Neshan vs Balad", fontsize=11)
    ax.legend(frameon=False, fontsize=9)
    style_ax(ax)
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 20 – GPS FAILURE STAGE DISTRIBUTION
    # ================================================================
    # At which stage of a ride does GPS typically fail (offer, en route to
    # passenger, origin-to-destination, or all stages)?
    # Business value: helps engineering teams focus GPS fixes on the right stage.
    gps_stages = ["No problem", "Offer", "in route to passenger",
                  "Origin to Destination route", "All stages"]
    fig, axes = plt.subplots(1, 2, figsize=(
        12, 5), facecolor=BG_COLOR, sharey=True)
    fig.suptitle("GPS Failure Stage Distribution – Snapp vs Tapsi",
                 fontsize=15, fontweight="bold", y=0.99)
    for ax, col, color, label in [(axes[0], "snapp_gps_stage", SNAPP_COLOR, "Snapp"),
                                  (axes[1], "tapsi_gps_stage", TAPSI_COLOR, "Tapsi")]:
        data = short[col].dropna().value_counts()
        total = data.sum()
        ordered = [data.get(s, 0) for s in gps_stages]
        ax.barh(gps_stages, ordered, color=color, edgecolor="white")
        for i, v in enumerate(ordered):
            if v > 0:
                ax.text(v+5, i, f"{v} ({v/total*100:.0f}%)",
                        va="center", fontsize=9)
        ax.set_title(f"{label}  (n={total})", fontsize=11)
        style_ax(ax)
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 21 – GPS GLITCH TIME OF DAY
    # ================================================================
    # When during the day GPS glitches are most common (wide binary columns).
    # Business value: if glitches concentrate during traffic hours, it may
    # indicate network congestion rather than a pure GPS issue.
    # Same wide-binary-column pattern as PAGE 11.
    gps_time_cols = {"Morning (4–9 AM)": "GPS Glitch Time__Morning(4-9AM)",
                     "Before Noon (9–12 PM)": "GPS Glitch Time__Before Noon(9-12PM)",
                     "Afternoon (12–4 PM)": "GPS Glitch Time__Afternoon(12-4PM)",
                     "Traffic Hours (4–8 PM)": "GPS Glitch Time__Traffic(4-8PM)",
                     "Night (8 PM–12 AM)": "GPS Glitch Time__Night(8-12AM)",
                     "Late Night": "GPS Glitch Time__Late Night"}
    time_labels = list(gps_time_cols.keys())
    time_vals = [
        wide[c].sum() if c in wide.columns else 0 for c in gps_time_cols.values()]
    fig, ax = new_fig(
        "When GPS Glitches Occur – Time of Day (wide_survey)", figsize=(12, 5))
    bars = ax.bar(time_labels, time_vals, color=ACCENT, edgecolor="white")
    for bar in bars:
        ax.text(bar.get_x()+bar.get_width()/2, bar.get_height()+5,
                str(int(bar.get_height())), ha="center", fontsize=9)
    ax.set_ylabel("Count")
    ax.tick_params(axis="x", rotation=15)
    style_ax(ax)
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 22 – GPS GLITCH ACTIONS
    # ================================================================
    # What drivers do when GPS fails (call passenger, accept familiar trips,
    # cancel trip, switch to Tapsi, etc.).
    # Business value: "Switched to Tapsi" directly quantifies churn risk from GPS issues.
    # Same wide-binary-column pattern as PAGE 11.
    gps_action_map = {"Called Passenger": "GPS Action when Glitch__Called Passenger",
                      "Accepted Familiar Trips": "GPS Action when Glitch__Accepted familiar trips",
                      "Passenger Help for Route": "GPS Action when Glitch__Passenger Help for route",
                      "Decided to Stop": "GPS Action when Glitch__Decided to stop working",
                      "Cancelled Trip": "GPS Action when Glitch__Cancelled Trip",
                      "Changed Location": "GPS Action when Glitch__Changed Location",
                      "Switched to Tapsi": "GPS Action when Glitch__Switched to Tapsi"}
    gps_action_vals = [
        wide[c].sum() if c in wide.columns else 0 for c in gps_action_map.values()]
    fig, ax = new_fig("Driver Actions During GPS Glitch – wide_survey")
    ax.barh(list(gps_action_map.keys()), gps_action_vals,
            color=ACCENT, edgecolor="white")
    for i, v in enumerate(gps_action_vals):
        ax.text(v+5, i, str(int(v)), va="center", fontsize=9)
    ax.invert_yaxis()
    ax.set_xlabel("Count")
    style_ax(ax)
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 23 – COMMISSION & TAX TRANSPARENCY
    # ================================================================
    # What drivers believe about commission rates and tax deductions.
    # Business value: if many drivers give wrong answers, the platform needs
    # better communication about its fee structure.
    # Same 2x2 subplot with horizontal bars pattern as PAGE 3.
    fig, axes = plt.subplots(2, 2, figsize=(13, 8), facecolor=BG_COLOR)
    fig.suptitle("Commission & Tax Transparency – What Drivers Believe",
                 fontsize=15, fontweight="bold", y=0.97)
    for ax, col, color, label in [
        (axes[0, 0], "snapp_comm_info", SNAPP_COLOR, "Snapp Commission Rate"),
        (axes[0, 1], "tapsi_comm_info", TAPSI_COLOR, "Tapsi Commission Rate"),
        (axes[1, 0], "snapp_tax_info", SNAPP_COLOR, "Snapp Tax Info"),
        (axes[1, 1], "tapsi_tax_info", TAPSI_COLOR, "Tapsi Tax Info"),
    ]:
        data = short[col].dropna()
        vc = data.value_counts().head(10)
        total = len(data)
        ax.barh(vc.index[::-1], vc.values[::-1],
                color=color, edgecolor="white")
        for i, (k, v) in enumerate(zip(vc.index[::-1], vc.values[::-1])):
            ax.text(v+1, i, f"{v} ({v/total*100:.0f}%)",
                    va="center", fontsize=8)
        ax.set_title(f"{label}  (n={total})", fontsize=10)
        style_ax(ax)
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 24 – UNPAID FARES – INCIDENT RATE & COMPENSATION
    # ================================================================
    # How often drivers experience unpaid fares, and whether the platform
    # compensates them (fully, partially, or not at all).
    # Business value: compensation fairness is a major trust driver --
    # Tapsi's higher full-compensation rate (63%) builds driver loyalty.
    fig, axes = plt.subplots(1, 2, figsize=(14, 6), facecolor=BG_COLOR)
    fig.suptitle("Unpaid by Passenger – Incident Rate & Compensation",
                 fontsize=15, fontweight="bold", y=0.99)
    ax = axes[0]
    platforms = ["Snapp", "Tapsi"]
    followup_cols = ["snapp_unpaid_by_passenger_followup",
                     "tapsi_unpaid_by_passenger_followup"]
    outcomes = ["No", "Yes - No", "Yes - Yes"]
    outcome_colors = ["#EF5350", "#FFA726", "#66BB6A"]
    for pi, (platform, col, color) in enumerate(zip(platforms, followup_cols, [SNAPP_COLOR, TAPSI_COLOR])):
        data = short[col].dropna()
        total = len(data)
        vals = [len(data[data == o]) for o in outcomes]
        for oi, (o, v) in enumerate(zip(outcomes, vals)):
            ax.bar(oi + pi*(0.35+0.02) - 0.22, v, 0.35,
                   color=outcome_colors[oi], edgecolor="white")
            ax.text(oi + pi*(0.35+0.02) - 0.22, v+5,
                    f"{v/total*100:.0f}%", ha="center", fontsize=8)
    ax.set_xticks(np.arange(3)+0.11)
    ax.set_xticklabels(
        ["No Followup", "Followed – Not\nResolved", "Followed –\nResolved"], fontsize=9)
    ax.set_ylabel("Count")
    ax.set_title("Unpaid Fare Followup", fontsize=11)
    snapp_p = mpatches.Patch(color=SNAPP_COLOR, label="Snapp")
    tapsi_p = mpatches.Patch(color=TAPSI_COLOR, label="Tapsi")
    ax.legend(handles=[snapp_p, tapsi_p], frameon=False, fontsize=9)
    style_ax(ax)
    ax = axes[1]
    comp_cats = ["Yes - all of it", "Yes - Part of it", "No compensation"]
    comp_colors = ["#66BB6A", "#FFA726", "#EF5350"]
    x = np.arange(len(comp_cats))
    w = 0.35
    for pi, (platform, (col, pcolor)) in enumerate([("Snapp", ("snapp_compensate_unpaid_by_passenger", SNAPP_COLOR)),
                                                    ("Tapsi", ("tapsi_compensate_unpaid_by_passenger", TAPSI_COLOR))]):
        data = short[col].dropna()
        total = len(data)
        vals = [len(data[data == c]) for c in comp_cats]
        offset = (pi-0.5)*(w+0.02)
        ax.bar(x+offset, vals, w, color=pcolor,
               edgecolor="white", label=platform, alpha=0.85)
        for xi, v in enumerate(vals):
            ax.text(xi+offset, v+1,
                    f"{v/total*100:.0f}%", ha="center", fontsize=8)
    ax.set_xticks(x)
    ax.set_xticklabels(["Full\nCompensation", "Partial", "None"], fontsize=9)
    ax.set_title("Compensation Outcome", fontsize=11)
    ax.legend(frameon=False, fontsize=9)
    style_ax(ax)
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 25 – CUSTOMER SUPPORT CHANNEL USAGE
    # ================================================================
    # How drivers contact customer support (app, call, in-person, etc.).
    # Business value: informs CS channel investment -- if most contact is via
    # app, investing in chatbot quality pays off more than call center expansion.
    fig, axes = plt.subplots(1, 2, figsize=(13, 5), facecolor=BG_COLOR)
    fig.suptitle("Customer Support Channel Usage – Snapp vs Tapsi",
                 fontsize=15, fontweight="bold", y=0.99)
    for ax, col, color, label in [(axes[0], "snapp_customer_support", SNAPP_COLOR, "Snapp"),
                                  (axes[1], "tapsi_customer_support", TAPSI_COLOR, "Tapsi")]:
        data = short[col].dropna().value_counts()
        total = data.sum()
        ax.barh(data.index[::-1], data.values[::-1],
                color=color, edgecolor="white")
        for i, v in enumerate(data.values[::-1]):
            ax.text(v+5, i, f"{v:,} ({v/total*100:.0f}%)",
                    va="center", fontsize=9)
        ax.set_title(f"{label}  (n={total})", fontsize=11)
        style_ax(ax)
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 26 – CUSTOMER SUPPORT DEEP DIVE
    # ================================================================
    # 3-panel deep dive: issue resolution rate, satisfaction sub-scores
    # (wait time, solution quality, behaviour, relevance), and top
    # dissatisfaction reasons for Snapp CS.
    # Business value: pinpoints exactly where CS falls short (e.g., wait time
    # vs solution quality) for targeted improvement.
    fig, axes = plt.subplots(1, 3, figsize=(16, 6), facecolor=BG_COLOR)
    fig.suptitle("Customer Support Quality Deep Dive – Snapp vs Tapsi",
                 fontsize=15, fontweight="bold", y=0.99)
    ax = axes[0]
    solve_cats = ["Yes", "To an extent", "No"]
    for pi, (col, color, label) in enumerate([("snapp_CS_solved", SNAPP_COLOR, "Snapp"),
                                              ("tapsi_CS_solved", TAPSI_COLOR, "Tapsi")]):
        data = short[col].dropna()
        total = len(data)
        vals = [len(data[data == c]) for c in solve_cats]
        x = np.arange(len(solve_cats))
        w = 0.35
        offset = (pi-0.5)*(w+0.02)
        ax.bar(x+offset, vals, w, color=color,
               edgecolor="white", label=label, alpha=0.85)
        for xi, v in enumerate(vals):
            ax.text(xi+offset, v+1,
                    f"{v/total*100:.0f}%", ha="center", fontsize=8)
    ax.set_xticks(x)
    ax.set_xticklabels(solve_cats, fontsize=10)
    ax.set_title("Issue Resolution Rate", fontsize=11)
    ax.legend(frameon=False, fontsize=9)
    style_ax(ax)
    ax = axes[1]
    cs_dims = ["Overall", "Wait Time", "Solution", "Behaviour", "Relevance"]
    snapp_cs_means = [short["snapp_CS_satisfaction_overall"].mean(),
                      short["snapp_CS_satisfaction_waittime"].mean(),
                      short["snapp_CS_satisfaction_solution"].mean(),
                      short["snapp_CS_satisfaction_behaviour"].mean(),
                      short["snapp_CS_satisfaction_relevance"].mean()]
    tapsi_cs_means = [short["tapsi_CS_satisfaction_overall"].mean(),
                      short["tapsi_CS_satisfaction_waittime"].mean(),
                      short["tapsi_CS_satisfaction_solution"].mean(),
                      short["tapsi_CS_satisfaction_behaviour"].mean(),
                      short["tapsi_CS_satisfaction_relevance"].mean()]
    x = np.arange(len(cs_dims))
    w = 0.35
    ax.bar(x-w/2, snapp_cs_means, w, color=SNAPP_COLOR,
           label="Snapp", edgecolor="white")
    ax.bar(x+w/2, tapsi_cs_means, w, color=TAPSI_COLOR,
           label="Tapsi", edgecolor="white")
    for xi, (sv, tv) in enumerate(zip(snapp_cs_means, tapsi_cs_means)):
        ax.text(xi-w/2, sv+0.05, f"{sv:.2f}",
                ha="center", fontsize=7.5, fontweight="bold")
        ax.text(xi+w/2, tv+0.05, f"{tv:.2f}",
                ha="center", fontsize=7.5, fontweight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels(cs_dims, fontsize=9, rotation=10)
    ax.set_ylim(0, 5.5)
    ax.set_title("CS Satisfaction Sub-scores (1–5)", fontsize=11)
    ax.legend(frameon=False, fontsize=9)
    style_ax(ax)
    ax = axes[2]
    reasons_s = short["snapp_CS_satisfaction_important_reason"].dropna(
    ).value_counts().head(5)
    ax.barh(reasons_s.index[::-1], reasons_s.values[::-1],
            color=SNAPP_COLOR, edgecolor="white")
    total_s = reasons_s.sum()
    for i, v in enumerate(reasons_s.values[::-1]):
        ax.text(v+1, i, f"{v} ({v/total_s*100:.0f}%)", va="center", fontsize=9)
    ax.set_title("Snapp CS Dissatisfaction\nMain Reason (top 5)", fontsize=11)
    style_ax(ax)
    save_fig(pdf, fig)

    # ================================================================
    # PAGES 27–65
    # ================================================================

    # PAGE 27 – COLLABORATION REASONS
    # Why drivers chose to work with Snapp or Tapsi (top 10 reasons).
    # Business value: reveals the primary value propositions each platform
    # offers from the driver's perspective.
    fig, axes = plt.subplots(1, 2, figsize=(14, 6), facecolor=BG_COLOR)
    fig.suptitle("Collaboration Reasons – Why Drivers Chose Each Platform",
                 fontsize=15, fontweight="bold", y=0.99)
    for ax, col, color, label in [(axes[0], "snapp_collab_reason", SNAPP_COLOR, "Snapp"),
                                  (axes[1], "tapsi_collab_reason", TAPSI_COLOR, "Tapsi")]:
        data = short[col].dropna()
        data = data[data.str.match(r'^[A-Za-z0-9 _\-\/]+$', na=False)]
        vc = data.value_counts().head(10)
        total = data.notna().sum()
        ax.barh(vc.index[::-1], vc.values[::-1],
                color=color, edgecolor="white")
        for i, v in enumerate(vc.values[::-1]):
            ax.text(v+1, i, f"{v} ({v/total*100:.1f}%)",
                    va="center", fontsize=9)
        ax.set_title(f"{label}  (n={total})", fontsize=11)
        style_ax(ax)
    save_fig(pdf, fig)

    # PAGE 28 – INCOME SOURCE PREFERENCE
    # Drivers' perception of which platform yields better income.
    # Business value: subjective income perception drives platform switching.
    fig, axes = plt.subplots(1, 2, figsize=(12, 5), facecolor=BG_COLOR)
    fig.suptitle("Which Service Yields Better Income? (Driver Perception)",
                 fontsize=15, fontweight="bold", y=0.99)
    for ax, col, color, label in [(axes[0], "snapp_better_income", SNAPP_COLOR, "Snapp Drivers' View"),
                                  (axes[1], "tapsi_better_income", TAPSI_COLOR, "Tapsi Drivers' View")]:
        data = short[col].dropna().value_counts()
        total = data.sum()
        ax.barh(data.index, data.values, color=color, edgecolor="white")
        for i, v in enumerate(data.values):
            ax.text(v+1, i, f"{v} ({v/total*100:.0f}%)",
                    va="center", fontsize=9)
        ax.set_title(f"{label}  (n={total})", fontsize=11)
        style_ax(ax)
    save_fig(pdf, fig)

    # PAGE 29 – SATISFACTION BY YEARWEEK (TREND LINES)
    # Fare, Income, and Request Count satisfaction tracked weekly.
    # Business value: shows whether satisfaction is improving or declining,
    # and whether changes align with known business events (fare changes, etc.).
    fig, axes = plt.subplots(1, 3, figsize=(
        16, 5), facecolor=BG_COLOR, sharey=True)
    fig.suptitle("Avg Satisfaction by Year-Week: Snapp vs Tapsi",
                 fontsize=15, fontweight="bold", y=0.99)
    for ax, (scol, tcol, label) in zip(axes, SAT_PAIRS):
        weekly_sat = short.groupby("yearweek").agg(
            snapp=(scol, "mean"), tapsi=(tcol, "mean"))
        ax.plot(weekly_sat.index.astype(str), weekly_sat["snapp"], marker="o",
                color=SNAPP_COLOR, linewidth=2, label="Snapp")
        ax.plot(weekly_sat.index.astype(str), weekly_sat["tapsi"], marker="s",
                color=TAPSI_COLOR, linewidth=2, label="Tapsi")
        for idx, row in weekly_sat.iterrows():
            if not np.isnan(row["snapp"]):
                ax.annotate(f"{row['snapp']:.2f}", (str(idx), row["snapp"]),
                            textcoords="offset points", xytext=(0, 8), ha="center", fontsize=7, color=SNAPP_COLOR)
            if not np.isnan(row["tapsi"]):
                ax.annotate(f"{row['tapsi']:.2f}", (str(idx), row["tapsi"]),
                            textcoords="offset points", xytext=(0, -12), ha="center", fontsize=7, color=TAPSI_COLOR)
        ax.set_title(label, fontsize=11)
        ax.set_ylim(0, 5.5)
        ax.set_xlabel("Year-Week")
        ax.legend(frameon=False, fontsize=8)
        ax.tick_params(axis="x", rotation=45)
        style_ax(ax)
    axes[0].set_ylabel("Mean Satisfaction (1–5)")
    save_fig(pdf, fig)

    # PAGES 30–32 – SATISFACTION BY GROUP
    # Each call to plot_sat_by_group creates a 3-panel page (Fare, Income,
    # Request Count) segmented by one demographic variable.
    # PAGE 30: by cooperation type -- do full-time drivers rate differently?
    plot_sat_by_group(pdf, short, "cooperation_type", "Cooperation Type")
    # PAGE 31: by city (top 10) -- which cities have the happiest drivers?
    plot_sat_by_group(pdf, short, "city", "City (Top 10)",
                      top_n=10, min_group_size=20)
    # PAGE 32: by driver type -- joint drivers can compare platforms directly.
    plot_sat_by_group(pdf, short, "driver_type",
                      "Driver Type (Snapp Exclusive vs Joint)")

    # PAGE 33 – SATISFACTION HONEYMOON EFFECT
    # Shows how satisfaction and recommendation scores decline as drivers gain
    # more tenure on Snapp.  New drivers (<3 months) typically rate higher.
    # Business value: quantifies the "honeymoon effect" -- retention strategies
    # should target the tenure band where satisfaction drops most sharply.
    valid_tenure = [
        t for t in TENURE_ORDER if t in short["snapp_age"].unique()]
    sat_by_tenure = short.groupby("snapp_age").agg(
        snapp_overall=("snapp_overall_satisfaction", "mean"),
        snapp_fare=("snapp_fare_satisfaction", "mean"),
        snapp_income=("snapp_income_satisfaction", "mean"),
        snapp_rec=("snapp_recommend", "mean"),
        n=("snapp_overall_satisfaction", "count"),
    ).reindex(valid_tenure)
    labels_map = dict(zip(TENURE_ORDER, TENURE_LABELS))
    xticklabels = [labels_map.get(t, t) for t in valid_tenure]
    fig, axes = plt.subplots(1, 2, figsize=(14, 6), facecolor=BG_COLOR)
    fig.suptitle("Honeymoon Effect – Satisfaction Declines with Snapp Tenure",
                 fontsize=15, fontweight="bold", y=0.99)
    ax = axes[0]
    x = np.arange(len(valid_tenure))
    ax.plot(x, sat_by_tenure["snapp_overall"], marker="o", color=SNAPP_COLOR,
            linewidth=2.5, markersize=9, label="Overall Sat.")
    ax.plot(x, sat_by_tenure["snapp_fare"], marker="s", color=ACCENT,
            linewidth=2, markersize=7, linestyle="--", label="Fare Sat.")
    ax.plot(x, sat_by_tenure["snapp_income"], marker="^", color=ACCENT2,
            linewidth=2, markersize=7, linestyle="--", label="Income Sat.")
    for i, (idx, row) in enumerate(sat_by_tenure.iterrows()):
        ax.annotate(f"{row['snapp_overall']:.2f}", (i, row["snapp_overall"]),
                    textcoords="offset points", xytext=(0, 10), ha="center", fontsize=9)
    ax.set_xticks(x)
    ax.set_xticklabels(xticklabels)
    ax.set_ylim(0, 5.5)
    ax.set_ylabel("Mean Satisfaction (1–5)")
    ax.set_xlabel("Snapp Tenure")
    ax.set_title("Snapp Satisfaction by Tenure", fontsize=11)
    ax.legend(frameon=False, fontsize=9)
    style_ax(ax)
    ax = axes[1]
    ax.plot(x, sat_by_tenure["snapp_rec"], marker="D", color=TAPSI_COLOR,
            linewidth=2.5, markersize=9, label="Snapp Recommend (0–10)")
    for i, (idx, row) in enumerate(sat_by_tenure.iterrows()):
        ax.annotate(f"{row['snapp_rec']:.2f}", (i, row["snapp_rec"]),
                    textcoords="offset points", xytext=(0, 10), ha="center", fontsize=9)
    n_labels = [f"{xticklabels[i]}\n(n={int(sat_by_tenure['n'].iloc[i]):,})"
                for i in range(len(valid_tenure))]
    ax.set_xticks(x)
    ax.set_xticklabels(n_labels, fontsize=9)
    ax.set_ylim(0, 10)
    ax.set_ylabel("Avg Recommendation Score (0–10)")
    ax.set_xlabel("Snapp Tenure")
    ax.set_title("Recommendation by Tenure", fontsize=11)
    ax.legend(frameon=False, fontsize=9)
    style_ax(ax)
    save_fig(pdf, fig)

    # PAGE 34 – SATISFACTION BY AGE GROUP
    # Satisfaction segmented by driver age bracket.
    # Business value: younger drivers may have different expectations.
    age_order = ["<18", "18_25", "26_35", "36_45", "46_55", "56_65", ">65"]
    plot_sat_by_group(pdf, short, "age", "Age Group", order=age_order)

    # PAGE 35 – SATISFACTION BY ACTIVE TIME (ENGAGEMENT LEVEL)
    # Satisfaction and recommendation segmented by weekly driving hours.
    # Business value: heavy users (>40h/week) may be less satisfied because
    # they hit more pain points, or more satisfied because they earn more.
    active_order = ["few hours/month", "<20hour/mo", "5_20hour/week",
                    "20_40h/week", ">40h/week", "8_12hour/day", ">12h/day"]
    active_labels_display = {
        "few hours/month": "Few h/mo", "<20hour/mo": "<20h/mo",
        "5_20hour/week": "5–20h/wk", "20_40h/week": "20–40h/wk",
        ">40h/week": ">40h/wk", "8_12hour/day": "8–12h/d", ">12h/day": ">12h/d"
    }
    df_active = short.copy()
    df_active["active_time_ordered"] = df_active["active_time"].map(
        lambda x: active_order.index(x) if x in active_order else 99)
    df_active = df_active[df_active["active_time_ordered"] < 99]
    active_grps = [
        a for a in active_order if a in df_active["active_time"].unique()]
    active_sat = df_active.groupby("active_time").agg(
        snapp_overall=("snapp_overall_satisfaction", "mean"),
        tapsi_overall=("tapsi_overall_satisfaction", "mean"),
        snapp_rec=("snapp_recommend", "mean"),
        tapsi_rec=("tapsidriver_tapsi_recommend", "mean"),
        n=("snapp_overall_satisfaction", "count"),
    ).reindex(active_grps)
    fig, axes = plt.subplots(1, 2, figsize=(14, 6), facecolor=BG_COLOR)
    fig.suptitle("Satisfaction & Recommendation by Driver Engagement Level",
                 fontsize=15, fontweight="bold", y=0.99)
    x = np.arange(len(active_grps))
    w = 0.35
    xlabels = [active_labels_display.get(a, a) for a in active_grps]
    ax = axes[0]
    ax.bar(x - w/2, active_sat["snapp_overall"], w,
           color=SNAPP_COLOR, label="Snapp", edgecolor="white")
    ax.bar(x + w/2, active_sat["tapsi_overall"], w,
           color=TAPSI_COLOR, label="Tapsi", edgecolor="white")
    ax.set_xticks(x)
    ax.set_xticklabels(xlabels, rotation=15, ha="right")
    ax.set_ylim(0, 5.5)
    ax.set_title("Overall Satisfaction", fontsize=11)
    ax.legend(frameon=False, fontsize=9)
    style_ax(ax)
    ax = axes[1]
    ax.plot(xlabels, active_sat["snapp_rec"], marker="o",
            color=SNAPP_COLOR, linewidth=2.5, label="Snapp Recommend")
    ax.plot(xlabels, active_sat["tapsi_rec"], marker="s",
            color=TAPSI_COLOR, linewidth=2.5, label="Tapsi Recommend")
    ax.set_ylim(0, 10)
    ax.set_title("Recommendation Score (0–10)", fontsize=11)
    ax.tick_params(axis="x", rotation=15)
    ax.legend(frameon=False, fontsize=9)
    style_ax(ax)
    save_fig(pdf, fig)

    # PAGE 36 – SATISFACTION BY OCCUPATION
    # Satisfaction and recommendation by primary job (only jobs with n>=100).
    # Business value: professional drivers vs side-giggers have different
    # satisfaction profiles and churn risk.
    job_sat = short.groupby("original_job").agg(
        n=("snapp_overall_satisfaction", "count"),
        snapp_overall=("snapp_overall_satisfaction", "mean"),
        tapsi_overall=("tapsi_overall_satisfaction", "mean"),
        snapp_rec=("snapp_recommend", "mean"),
    ).query("n >= 100").sort_values("snapp_overall")
    fig, axes = plt.subplots(1, 2, figsize=(14, 7), facecolor=BG_COLOR)
    fig.suptitle("Satisfaction by Primary Occupation (jobs with n≥100)",
                 fontsize=15, fontweight="bold", y=0.99)
    ax = axes[0]
    jobs = job_sat.index.tolist()
    ax.barh(jobs, job_sat["snapp_overall"],
            color=SNAPP_COLOR, alpha=0.85, label="Snapp Overall")
    ax.barh(jobs, job_sat["tapsi_overall"], color=TAPSI_COLOR,
            alpha=0.55, label="Tapsi Overall", left=0)
    ax.set_title("Snapp vs Tapsi Overall Satisfaction", fontsize=11)
    ax.legend(frameon=False, fontsize=9)
    style_ax(ax)
    for i, (job, row) in enumerate(job_sat.iterrows()):
        ax.text(row["snapp_overall"] + 0.02, i,
                f"{row['snapp_overall']:.2f} / {row['tapsi_overall']:.2f}", va="center", fontsize=8)
    ax.set_xlabel("Mean Satisfaction (1–5)")
    ax = axes[1]
    bar_colors = [SNAPP_COLOR if v >= job_sat["snapp_rec"].median(
    ) else GREY for v in job_sat["snapp_rec"]]
    ax.barh(jobs, job_sat["snapp_rec"], color=bar_colors, edgecolor="white")
    ax.axvline(job_sat["snapp_rec"].median(), color="black", linestyle="--", linewidth=1,
               label=f"Median: {job_sat['snapp_rec'].median():.1f}")
    for i, (job, row) in enumerate(job_sat.iterrows()):
        ax.text(row["snapp_rec"] + 0.05, i,
                f"{row['snapp_rec']:.1f} (n={int(row['n'])})", va="center", fontsize=8)
    ax.set_title("Snapp Recommendation Score (0–10)", fontsize=11)
    ax.set_xlabel("Mean Recommendation")
    ax.legend(frameon=False, fontsize=9)
    style_ax(ax)
    save_fig(pdf, fig)

    # PAGE 37 – TAPSI CARPOOLING
    # 4-panel deep dive into Tapsi's carpooling feature: familiarity,
    # offer acceptance rate, refusal reasons, and satisfaction.
    # Business value: measures feature adoption and identifies barriers.
    #
    # CHARTING PATTERN -- "pie chart":
    # ax.pie(values, labels=..., autopct="%1.0f%%") creates a pie chart.
    # wedgeprops={"edgecolor": "white"} separates slices visually.
    # Pie charts are used sparingly -- only for binary/few-category data
    # where the part-of-whole relationship is the key insight.
    fig, axes = plt.subplots(2, 2, figsize=(13, 9), facecolor=BG_COLOR)
    fig.suptitle("Tapsi Carpooling – Familiarity, Adoption, Refusal & Satisfaction",
                 fontsize=15, fontweight="bold", y=0.97)
    ax = axes[0, 0]
    fam = short["tapsi_carpooling_familiar"].value_counts()
    ax.pie(fam.values, labels=fam.index, autopct="%1.0f%%", colors=[
           TAPSI_COLOR, LGREY], startangle=90, wedgeprops={"edgecolor": "white"})
    ax.set_title("Carpooling Familiarity", fontsize=11)
    ax = axes[0, 1]
    offer_data = short["tapsi_carpooling_gotoffer_accepted"].dropna(
    ).value_counts()
    offer_colors = {"No": LGREY, "got offer - rejected": "#FF6D00",
                    "got offer - accepted": "#66BB6A"}
    ax.barh(offer_data.index, offer_data.values, color=[
            offer_colors.get(k, GREY) for k in offer_data.index], edgecolor="white")
    total = offer_data.sum()
    for i, v in enumerate(offer_data.values):
        ax.text(v + 5, i, f"{v} ({v/total*100:.0f}%)", va="center", fontsize=9)
    ax.set_title("Carpooling Offer Outcome", fontsize=11)
    style_ax(ax)
    ax = axes[1, 0]
    carp_refusal = {"Canceled by Passenger": "Tapsi Carpooling refusal__Canceled by Passenger",
                    "Long Wait Time": "Tapsi Carpooling refusal__Long Wait Time",
                    "Passenger Distance": "Tapsi Carpooling refusal__Passenger Distance",
                    "Not Familiar": "Tapsi Carpooling refusal__Not Familiar"}
    r_labels = list(carp_refusal.keys())
    r_vals = [
        wide[c].sum() if c in wide.columns else 0 for c in carp_refusal.values()]
    ax.barh(r_labels, r_vals, color=TAPSI_COLOR, edgecolor="white")
    for i, v in enumerate(r_vals):
        ax.text(v + 2, i, str(int(v)), va="center", fontsize=9)
    ax.set_title("Carpooling Refusal Reasons (wide_survey)", fontsize=11)
    style_ax(ax)
    ax = axes[1, 1]
    carp_sat = short["tapsi_carpooling_satisfaction_overall"].dropna()
    sat_counts = carp_sat.value_counts().sort_index()
    ax.bar(sat_counts.index.astype(int).astype(str),
           sat_counts.values, color=TAPSI_COLOR, edgecolor="white")
    total = sat_counts.sum()
    for xi, v in zip(sat_counts.index.astype(int).astype(str), sat_counts.values):
        ax.text(xi, v + 1, f"{v} ({v/total*100:.0f}%)",
                ha="center", fontsize=9)
    ax.set_title(
        f"Carpooling Satisfaction (1–5, mean={carp_sat.mean():.2f})", fontsize=11)
    ax.set_xlabel("Rating")
    style_ax(ax)
    save_fig(pdf, fig)

    # PAGE 38 – ECOPLUS & MAGICAL WINDOW
    # Adoption funnels for Snapp EcoPlus and Tapsi Magical Window features.
    # Business value: how many drivers know about these features vs actually use them.
    fig, axes = plt.subplots(1, 2, figsize=(13, 5), facecolor=BG_COLOR)
    fig.suptitle("Feature Adoption – Snapp EcoPlus & Tapsi Magical Window",
                 fontsize=15, fontweight="bold", y=0.99)
    ax = axes[0]
    ecoplus_familiar = short["snapp_ecoplus_familiar"].dropna().value_counts()
    ecoplus_usage = short["snapp_ecoplus_access_usage"].dropna().value_counts()
    cats = ["Familiar", "Has Access\n& Uses",
            "Has Access\n& Not Using", "Not Familiar"]
    vals = [ecoplus_familiar.get("Yes", 0), ecoplus_usage.get("Yes-Yes", 0),
            ecoplus_usage.get("Yes-No", 0), ecoplus_familiar.get("No", 0)]
    colors_ = [SNAPP_COLOR, "#66BB6A", "#FFA726", LGREY]
    ax.bar(cats, vals, color=colors_, edgecolor="white")
    total = len(short)
    for i, v in enumerate(vals):
        ax.text(i, v + 10, f"{v:,}\n({v/total*100:.1f}%)",
                ha="center", fontsize=9)
    ax.set_title("Snapp EcoPlus Adoption Funnel", fontsize=11)
    style_ax(ax)
    ax = axes[1]
    mw = short["tapsi_magical_window"].dropna().value_counts()
    mw_colors = {"Yes": TAPSI_COLOR, "No": "#FFA726", "Not Familiar": LGREY}
    ax.bar(mw.index, mw.values, color=[mw_colors.get(
        k, GREY) for k in mw.index], edgecolor="white")
    total_mw = mw.sum()
    for i, (k, v) in enumerate(mw.items()):
        ax.text(i, v + 10, f"{v:,} ({v/total_mw*100:.0f}%)",
                ha="center", fontsize=9)
    ax.set_title("Tapsi Magical Window Awareness", fontsize=11)
    style_ax(ax)
    save_fig(pdf, fig)

    # PAGE 39 – DRIVER PRIVACY & PARTICIPATION ATTITUDES
    # How comfortable drivers are talking about their work around others,
    # and why some avoid it (stigma, privacy, family pressure, etc.).
    # Business value: social perception of rideshare driving affects recruitment.
    fig, axes = plt.subplots(1, 2, figsize=(13, 5), facecolor=BG_COLOR)
    fig.suptitle("Driver Privacy & Participation Attitudes (Snapp)",
                 fontsize=15, fontweight="bold", y=0.99)
    ax = axes[0]
    feeling = short["snapp_participate_feeling"].dropna().value_counts()
    total_f = feeling.sum()
    feel_colors = {"no difference": SNAPP_COLOR, "no worry": "#66BB6A", "talk to some people": ACCENT,
                   "prefer not to talk": "#FFA726", "no talk at all": "#EF5350"}
    ax.barh(feeling.index, feeling.values, color=[feel_colors.get(
        k, GREY) for k in feeling.index], edgecolor="white")
    for i, v in enumerate(feeling.values):
        ax.text(v + 3, i, f"{v} ({v/total_f*100:.0f}%)",
                va="center", fontsize=9)
    ax.set_title(
        "Comfort Level When Working\nAround Other People", fontsize=11)
    ax.set_xlabel("Count")
    style_ax(ax)
    ax = axes[1]
    reason = short["snapp_not_talking_reason"].dropna().value_counts()
    total_r = reason.sum()
    ax.barh(reason.index, reason.values, color=ACCENT2, edgecolor="white")
    for i, v in enumerate(reason.values):
        ax.text(v + 1, i, f"{v} ({v/total_r*100:.0f}%)",
                va="center", fontsize=9)
    ax.set_title("Why Drivers Avoid Talking\nAbout Their Work", fontsize=11)
    ax.set_xlabel("Count")
    style_ax(ax)
    save_fig(pdf, fig)

    # PAGE 40 – DEMAND & SUPPLY METRICS
    # 3-panel view: % of demand processed, missed trips per 10, max simultaneous demand.
    # Business value: supply-demand mismatch is the root cause of many driver complaints.
    fig, axes = plt.subplots(1, 3, figsize=(15, 5), facecolor=BG_COLOR)
    fig.suptitle("Demand & Supply – How Much Demand Do Drivers Process?",
                 fontsize=15, fontweight="bold", y=0.99)
    for ax, col, color, label in [
        (axes[0], "demand_process", ACCENT, "% of Demand Processed"),
        (axes[1], "missed_demand_per_10", ACCENT2, "Missed per 10 Trips"),
        (axes[2], "max_demand", SNAPP_COLOR, "Max Simultaneous Demand"),
    ]:
        data = short[col].dropna().value_counts()
        total = data.sum()
        ax.barh(data.index, data.values, color=color, edgecolor="white")
        for i, v in enumerate(data.values):
            ax.text(v + 1, i, f"{v} ({v/total*100:.0f}%)",
                    va="center", fontsize=9)
        ax.set_title(label, fontsize=11)
        style_ax(ax)
    save_fig(pdf, fig)

    # PAGE 41 – COMMISSION-FREE RIDES
    # Scatter plot: commission-free rides vs total rides.
    # The y=x line shows where a driver would have ALL rides commission-free.
    # Business value: shows how much of the incentive budget goes to commission waivers.
    #
    # CHARTING PATTERN -- "scatter plot":
    # ax.scatter(x, y, alpha=0.3) plots individual driver data points.
    # alpha < 1.0 makes overlapping points semi-transparent so density is visible.
    # ax.plot(lims, lims, "--") draws a diagonal reference line.
    fig, axes = plt.subplots(1, 2, figsize=(12, 5), facecolor=BG_COLOR)
    fig.suptitle("Commission-Free Rides vs Total Rides",
                 fontsize=15, fontweight="bold", y=0.99)
    for ax, ride_col, cf_col, color, label in [
        (axes[0], "snapp_ride", "snapp_commfree", SNAPP_COLOR, "Snapp"),
        (axes[1], "tapsi_ride", "tapsi_commfree", TAPSI_COLOR, "Tapsi"),
    ]:
        mask = short[cf_col].notna()
        ax.scatter(short.loc[mask, ride_col], short.loc[mask, cf_col],
                   alpha=0.3, color=color, edgecolors="white", linewidth=0.3, s=30)
        lims = [0, 85]
        ax.plot(lims, lims, "--", color=GREY, linewidth=0.8, label="y = x")
        ax.set_xlim(lims)
        ax.set_ylim(lims)
        ax.set_xlabel("Total Rides")
        ax.set_title(f"{label}  (n={mask.sum()})", fontsize=11)
        ax.legend(frameon=False, fontsize=9)
        style_ax(ax)
    axes[0].set_ylabel("Commission-Free Rides")
    save_fig(pdf, fig)

    # PAGE 42 – TOP 15 CITIES
    # Geographic distribution of survey responses by city.
    # Business value: ensures the survey is not overly Tehran-centric.
    city_counts = short["city"].value_counts().head(15)
    fig, ax = new_fig("Top 15 Cities by Response Count")
    ax.barh(city_counts.index[::-1], city_counts.values[::-1],
            color=ACCENT, edgecolor="white")
    for i, v in enumerate(city_counts.values[::-1]):
        ax.text(v + 1, i, f"{v:,}", va="center", fontsize=9)
    ax.set_xlabel("Responses")
    style_ax(ax)
    save_fig(pdf, fig)

    # PAGE 43 – CITY SATISFACTION
    # Satisfaction and recommendation scores for the top 12 cities.
    # Business value: identifies cities where satisfaction is lagging and
    # operations teams can intervene with targeted improvements.
    top_cities = short["city"].value_counts().head(12).index
    city_sat = (short[short["city"].isin(top_cities)].groupby("city")
                .agg(snapp_sat=("snapp_overall_satisfaction", "mean"), tapsi_sat=("tapsi_overall_satisfaction", "mean"),
                     snapp_rec=("snapp_recommend", "mean"), tapsi_rec=("tapsidriver_tapsi_recommend", "mean"),
                     n=("snapp_overall_satisfaction", "count")).sort_values("snapp_sat"))
    fig, axes = plt.subplots(1, 2, figsize=(14, 7), facecolor=BG_COLOR)
    fig.suptitle("City-Level Satisfaction Comparison (Top 12 Cities)",
                 fontsize=15, fontweight="bold", y=0.99)
    cities = city_sat.index.tolist()
    x = np.arange(len(cities))
    w = 0.35
    ax = axes[0]
    ax.barh(x - w/2, city_sat["snapp_sat"], w,
            color=SNAPP_COLOR, label="Snapp", edgecolor="white")
    ax.barh(x + w/2, city_sat["tapsi_sat"], w,
            color=TAPSI_COLOR, label="Tapsi", edgecolor="white")
    ax.set_yticks(x)
    ax.set_yticklabels(cities)
    ax.set_xlim(0, 6)
    ax.set_xlabel("Mean Satisfaction (1–5)")
    ax.set_title("Overall Satisfaction", fontsize=11)
    ax.legend(frameon=False, fontsize=9)
    style_ax(ax)
    ax = axes[1]
    ax.barh(x - w/2, city_sat["snapp_rec"], w,
            color=SNAPP_COLOR, label="Snapp", edgecolor="white")
    ax.barh(x + w/2, city_sat["tapsi_rec"], w,
            color=TAPSI_COLOR, label="Tapsi", edgecolor="white")
    ax.set_yticks(x)
    ax.set_yticklabels(cities)
    ax.set_xlabel("Mean Recommendation (0–10)")
    ax.set_title("Recommendation Score", fontsize=11)
    ax.legend(frameon=False, fontsize=9)
    style_ax(ax)
    save_fig(pdf, fig)

    # PAGE 44 – REGISTRATION & REFERRAL
    # 4-panel: registration type and main registration reason for each platform.
    # Business value: shows how drivers found the platform (referral, ad, organic).
    fig, axes = plt.subplots(2, 2, figsize=(13, 9), facecolor=BG_COLOR)
    fig.suptitle("Registration & Referral Analysis",
                 fontsize=15, fontweight="bold", y=0.97)
    for (ax, col, color, label) in [
        (axes[0, 0], "snapp_register_type",
         SNAPP_COLOR, "Snapp Registration Type"),
        (axes[0, 1], "tapsi_register_type",
         TAPSI_COLOR, "Tapsi Registration Type"),
        (axes[1, 0], "snapp_main_reg_reason",
         SNAPP_COLOR, "Snapp Registration Reason"),
        (axes[1, 1], "tapsi_main_reg_reason",
         TAPSI_COLOR, "Tapsi Registration Reason"),
    ]:
        data = short[col].dropna().value_counts()
        total = data.sum()
        ax.barh(data.index[::-1], data.values[::-1],
                color=color, edgecolor="white")
        for i, v in enumerate(data.values[::-1]):
            ax.text(v + 2, i, f"{v} ({v/total*100:.0f}%)",
                    va="center", fontsize=9)
        ax.set_title(label, fontsize=10)
        style_ax(ax)
    save_fig(pdf, fig)

    # PAGES 45-47 – LONG SURVEY MULTI-CHOICE PAGES
    # These use plot_long_snapp_vs_tapsi: a reusable function that filters
    # the long DataFrame by question name and shows answer distributions
    # as horizontal bar charts in a 2-panel (Snapp | Tapsi) layout.
    # PAGE 45: Incentive Type distribution from long survey
    plot_long_snapp_vs_tapsi(pdf, long, "Snapp Incentive Type",
                             "Tapsi Incentive Type", "Incentive Type – long_survey")
    # PAGE 46: Whether drivers received their incentive bonus
    plot_long_snapp_vs_tapsi(pdf, long, "Snapp Incentive GotBonus",
                             "Tapsi Incentive GotBonus", "Incentive Got Bonus – long_survey")
    # PAGE 47: CS contact categories from the long survey perspective
    plot_long_snapp_vs_tapsi(pdf, long, "Snapp Customer Support Category",
                             "Tapsi Customer Support Category", "Customer Support Categories – long_survey")

    # PAGE 48 – SNAPP NAVIGATION ISSUES
    # What makes drivers unsatisfied with Snapp navigation, and why they refuse it.
    # Business value: direct product feedback for the Snapp navigation team.
    nav_unsat_q = long[long["question"] == "Snapp Navigation Unsatisfaction"]
    nav_refusal_q = long[long["question"] == "Snapp Navigation Refusal"]
    fig, axes = plt.subplots(1, 2, figsize=(14, 6), facecolor=BG_COLOR)
    fig.suptitle("Snapp Navigation Issues – long_survey",
                 fontsize=15, fontweight="bold", y=0.99)
    for ax, qdata, color, label in [
        (axes[0], nav_unsat_q, SNAPP_COLOR, "Navigation Unsatisfaction"),
        (axes[1], nav_refusal_q, ACCENT, "Navigation Refusal Reasons"),
    ]:
        if len(qdata) == 0:
            ax.set_title(f"{label} (no data)", fontsize=11)
            style_ax(ax)
            continue
        vc = qdata["answer"].value_counts().sort_values(ascending=True)
        total = vc.sum()
        ax.barh(vc.index, vc.values, color=color, edgecolor="white")
        for i, (k, v) in enumerate(vc.items()):
            ax.text(v + 5, i, f"{v} ({v/total*100:.0f}%)",
                    va="center", fontsize=9)
        ax.set_title(f"{label}  (n={total:,})", fontsize=11)
        style_ax(ax)
        ax.set_xlabel("Count")
    save_fig(pdf, fig)

    # PAGE 49 – DECLINE REASON & APP MENU USAGE
    # Why drivers decline trips (from long survey), and which SnappDriver app
    # menu features they actually use.
    # Business value: app menu usage shows which features are discovered vs ignored.
    fig, axes = plt.subplots(1, 2, figsize=(14, 6), facecolor=BG_COLOR)
    fig.suptitle("Decline Reason & SnappDriver App Menu Usage – long_survey",
                 fontsize=15, fontweight="bold", y=0.99)
    for ax, question, color, label in [
        (axes[0], "Decline Reason", SNAPP_COLOR, "Decline Reason"),
        (axes[1], "SnappDriver App Menu", ACCENT, "SnappDriver App Menu"),
    ]:
        qdata = long[long["question"] == question]
        if len(qdata) == 0:
            ax.set_title(f"{label} (no data)", fontsize=11)
            style_ax(ax)
            continue
        vc = qdata["answer"].value_counts().sort_values(ascending=True)
        total = vc.sum()
        ax.barh(vc.index, vc.values, color=color, edgecolor="white")
        for i, (k, v) in enumerate(vc.items()):
            ax.text(v + 2, i, f"{v} ({v/total*100:.0f}%)",
                    va="center", fontsize=9)
        ax.set_title(f"{label}  (n={total:,})", fontsize=11)
        style_ax(ax)
        ax.set_xlabel("Count")
    save_fig(pdf, fig)

    # PAGE 50 – TAPSI-ONLY QUESTIONS (LONG SURVEY)
    # Tapsi-specific multi-choice questions: incentive unsatisfaction reasons
    # and carpooling refusal reasons from the long survey format.
    fig, axes = plt.subplots(1, 2, figsize=(14, 5), facecolor=BG_COLOR)
    fig.suptitle("Tapsi-Only Questions – long_survey",
                 fontsize=15, fontweight="bold", y=0.99)
    for ax, question, color, label in [
        (axes[0], "Tapsi Incentive Unsatisfaction",
         TAPSI_COLOR, "Incentive Unsatisfaction"),
        (axes[1], "Tapsi Carpooling refusal",
         TAPSI_COLOR, "Carpooling Refusal"),
    ]:
        qdata = long[long["question"] == question]
        if len(qdata) == 0:
            ax.set_title(f"{label} (no data)", fontsize=11)
            style_ax(ax)
            continue
        vc = qdata["answer"].value_counts().sort_values(ascending=True)
        total = vc.sum()
        ax.barh(vc.index, vc.values, color=color, edgecolor="white")
        for i, (k, v) in enumerate(vc.items()):
            ax.text(v + 0.3, i, f"{v} ({v/total*100:.0f}%)",
                    va="center", fontsize=9)
        ax.set_title(f"{label}  (n={total})", fontsize=11)
        style_ax(ax)
        ax.set_xlabel("Count")
    save_fig(pdf, fig)

    # PAGES 51-52 – CS CATEGORY BY DRIVER TYPE
    # Customer support categories broken down by Joint vs Snapp-Exclusive drivers.
    # Business value: do joint drivers have different support needs?
    # Uses a grouped horizontal bar chart with driver_type as the group variable.
    #
    # CHARTING PATTERN -- "grouped barh by pivot table":
    # groupby(["driver_type", "answer"]).size().unstack() creates a pivot table
    # with driver types as rows and answer categories as columns.
    # Each driver type gets its own set of bars at offset y-positions.
    for q, platform, color in [
        ("Snapp Customer Support Category", "Snapp", SNAPP_COLOR),
        ("Tapsi Customer Support Category", "Tapsi", TAPSI_COLOR),
    ]:
        qdata = long[long["question"] == q]
        if len(qdata) == 0:
            continue
        pivot = qdata.groupby(["driver_type", "answer"]
                              ).size().unstack(fill_value=0)
        answers = pivot.columns.tolist()
        groups = pivot.index.tolist()
        fig, ax = new_fig(
            f"{platform} Customer Support by Driver Type", figsize=(14, 6))
        y = np.arange(len(answers))
        n_g = len(groups)
        total_w = 0.7
        w = total_w / n_g
        grp_colors = ["#42A5F5", "#EF5350"]
        for i, grp in enumerate(groups):
            vals = pivot.loc[grp].values
            ax.barh(y + i * w - total_w/2, vals, w, label=grp,
                    color=grp_colors[i % len(grp_colors)], edgecolor="white")
        ax.set_yticks(y)
        ax.set_yticklabels(answers)
        ax.legend(frameon=False, fontsize=9)
        ax.set_xlabel("Count")
        style_ax(ax)
        save_fig(pdf, fig)

    # PAGE 53 – APP USAGE & ECOPLUS REFUSAL
    # Which SnappDriver app features are used, and why drivers refuse EcoPlus.
    # Business value: EcoPlus refusal reasons guide feature improvement.
    fig, axes = plt.subplots(1, 2, figsize=(14, 5), facecolor=BG_COLOR)
    fig.suptitle("Snapp App Usage & EcoPlus Refusal – long_survey",
                 fontsize=15, fontweight="bold", y=0.99)
    for ax, question, color, label in [
        (axes[0], "Snapp Usage app", SNAPP_COLOR, "Snapp App Usage"),
        (axes[1], "Snapp Ecoplus Refusal", ACCENT2, "EcoPlus Refusal Reasons"),
    ]:
        qdata = long[long["question"] == question]
        if len(qdata) == 0:
            ax.set_title(f"{label} (no data)", fontsize=11)
            style_ax(ax)
            continue
        vc = qdata["answer"].value_counts().sort_values(ascending=True)
        total = vc.sum()
        ax.barh(vc.index, vc.values, color=color, edgecolor="white")
        for i, (k, v) in enumerate(vc.items()):
            ax.text(v + 0.5, i, f"{v} ({v/total*100:.0f}%)",
                    va="center", fontsize=9)
        ax.set_title(f"{label}  (n={total})", fontsize=11)
        style_ax(ax)
        ax.set_xlabel("Count")
    save_fig(pdf, fig)

    # PAGE 54 – JOINT VS SNAPP-EXCLUSIVE KEY METRICS
    # 3-panel comparison: overall satisfaction, avg weekly rides, and avg incentive
    # for Joint vs Snapp-Exclusive drivers.
    # Business value: are exclusive drivers happier or just less informed about
    # alternatives?  Do they ride more because they are not splitting time?
    metrics_by_dt = short.groupby("driver_type").agg(
        n=("snapp_overall_satisfaction", "count"), snapp_sat=("snapp_overall_satisfaction", "mean"),
        tapsi_sat=("tapsi_overall_satisfaction", "mean"), snapp_ride=("snapp_ride", "mean"),
        tapsi_ride=("tapsi_ride", "mean"), snapp_inc=("snapp_incentive", "mean"),
        tapsi_inc=("tapsi_incentive", "mean"), snapp_rec=("snapp_recommend", "mean"),
        tapsi_rec=("tapsidriver_tapsi_recommend", "mean"))
    fig, axes = plt.subplots(1, 3, figsize=(16, 5), facecolor=BG_COLOR)
    fig.suptitle("Joint vs Snapp Exclusive – Key Metric Comparison",
                 fontsize=15, fontweight="bold", y=0.99)
    groups_ = metrics_by_dt.index.tolist()
    x = np.arange(len(groups_))
    w = 0.35
    ax = axes[0]
    ax.bar(x - w/2, metrics_by_dt["snapp_sat"], w,
           color=SNAPP_COLOR, label="Snapp Sat.", edgecolor="white")
    ax.bar(x + w/2, metrics_by_dt["tapsi_sat"], w,
           color=TAPSI_COLOR, label="Tapsi Sat.", edgecolor="white")
    ax.set_xticks(x)
    ax.set_xticklabels(groups_)
    ax.set_ylim(0, 5.5)
    for xi, (sv, tv) in enumerate(zip(metrics_by_dt["snapp_sat"], metrics_by_dt["tapsi_sat"])):
        ax.text(xi - w/2, sv + 0.05, f"{sv:.2f}",
                ha="center", fontsize=9, fontweight="bold")
        ax.text(xi + w/2, tv + 0.05, f"{tv:.2f}",
                ha="center", fontsize=9, fontweight="bold")
    ax.set_title("Overall Satisfaction", fontsize=11)
    ax.legend(frameon=False, fontsize=9)
    style_ax(ax)
    ax = axes[1]
    ax.bar(x - w/2, metrics_by_dt["snapp_ride"], w,
           color=SNAPP_COLOR, label="Snapp Rides", edgecolor="white")
    ax.bar(x + w/2, metrics_by_dt["tapsi_ride"], w,
           color=TAPSI_COLOR, label="Tapsi Rides", edgecolor="white")
    ax.set_xticks(x)
    ax.set_xticklabels(groups_)
    for xi, (sv, tv) in enumerate(zip(metrics_by_dt["snapp_ride"], metrics_by_dt["tapsi_ride"])):
        ax.text(xi - w/2, sv + 0.3, f"{sv:.0f}",
                ha="center", fontsize=9, fontweight="bold")
        ax.text(xi + w/2, tv + 0.3, f"{tv:.0f}",
                ha="center", fontsize=9, fontweight="bold")
    ax.set_title("Avg Weekly Rides", fontsize=11)
    ax.legend(frameon=False, fontsize=9)
    style_ax(ax)
    ax = axes[2]
    ax.bar(x - w/2, metrics_by_dt["snapp_inc"] / 1e6, w,
           color=SNAPP_COLOR, label="Snapp Incentive", edgecolor="white")
    ax.bar(x + w/2, metrics_by_dt["tapsi_inc"] / 1e6, w,
           color=TAPSI_COLOR, label="Tapsi Incentive", edgecolor="white")
    ax.set_xticks(x)
    ax.set_xticklabels(groups_)
    for xi, (sv, tv) in enumerate(zip(metrics_by_dt["snapp_inc"] / 1e6, metrics_by_dt["tapsi_inc"] / 1e6)):
        ax.text(xi - w/2, sv + 0.05, f"{sv:.1f}M",
                ha="center", fontsize=9, fontweight="bold")
        ax.text(xi + w/2, tv + 0.05, f"{tv:.1f}M",
                ha="center", fontsize=9, fontweight="bold")
    ax.set_title("Avg Incentive (M Rials)", fontsize=11)
    ax.legend(frameon=False, fontsize=9)
    style_ax(ax)
    save_fig(pdf, fig)

    # PAGE 55 – INCENTIVE FULL FUNNEL
    # Tracks the conversion from "received incentive notification" to "actually
    # participated in the incentive program" for both platforms.
    # Business value: a large drop between notification and participation means
    # the incentive offer is not compelling enough or too complex.
    fig, axes = plt.subplots(1, 2, figsize=(14, 6), facecolor=BG_COLOR)
    fig.suptitle("Incentive Full Funnel – Notification → Participation",
                 fontsize=15, fontweight="bold", y=0.99)
    funnel_labels = [
        "Got Notification\n(Yes)", "Got Notification\n(No)", "Participated\n(Yes)", "Participated\n(No)"]
    funnel_colors = [SNAPP_COLOR, LGREY, "#66BB6A", "#EF5350"]
    ax = axes[0]
    notif_vc = short["snapp_gotmessage_text_incentive"].dropna().value_counts()
    partic_vc = short["snapp_incentive_message_participation"].dropna(
    ).value_counts()
    funnel_vals = [notif_vc.get("Yes", 0), notif_vc.get(
        "No", 0), partic_vc.get("Yes", 0), partic_vc.get("No", 0)]
    bars = ax.bar(funnel_labels, funnel_vals,
                  color=funnel_colors, edgecolor="white")
    total_n = notif_vc.sum()
    total_p = partic_vc.sum()
    totals_ = [total_n, total_n, total_p, total_p]
    for i, (b, v, tot) in enumerate(zip(bars, funnel_vals, totals_)):
        if tot > 0:
            ax.text(b.get_x() + b.get_width()/2, v + 30,
                    f"{v:,}\n({v/tot*100:.0f}%)", ha="center", fontsize=9)
    ax.set_title("Snapp Incentive Funnel", fontsize=11)
    ax.set_ylabel("Count")
    style_ax(ax)
    ax = axes[1]
    tapsi_notif_col = "tapsi_gotmessage_incentive"
    tapsi_partic_col = "tapsi_incentive_participation"
    t_notif_vc = short[tapsi_notif_col].dropna().value_counts(
    ) if tapsi_notif_col in short.columns else pd.Series(dtype=int)
    t_partic_vc = short[tapsi_partic_col].dropna().value_counts(
    ) if tapsi_partic_col in short.columns else pd.Series(dtype=int)
    t_funnel_vals = [t_notif_vc.get("Yes", 0), t_notif_vc.get(
        "No", 0), t_partic_vc.get("Yes", 0), t_partic_vc.get("No", 0)]
    bars2 = ax.bar(funnel_labels, t_funnel_vals,
                   color=funnel_colors, edgecolor="white")
    t_total_n = t_notif_vc.sum() if len(t_notif_vc) else 1
    t_total_p = t_partic_vc.sum() if len(t_partic_vc) else 1
    for i, (b, v, tot) in enumerate(zip(bars2, t_funnel_vals, [t_total_n, t_total_n, t_total_p, t_total_p])):
        if tot > 0:
            ax.text(b.get_x() + b.get_width()/2, v + 30,
                    f"{v:,}\n({v/tot*100:.0f}%)", ha="center", fontsize=9)
    ax.set_title("Tapsi Incentive Funnel", fontsize=11)
    ax.set_ylabel("Count")
    style_ax(ax)
    save_fig(pdf, fig)

    # PAGE 56 – INCENTIVE ACTIVE DURATION
    # How long drivers stay active in an incentive program (few hours, 1 day, 1-6 days, etc.).
    # Business value: if most exit within hours, the incentive structure may need
    # longer engagement windows.
    time_order = ["Few Hours", "1 Day", "1_6 Days", "7 Days", ">7 Days"]
    fig, axes = plt.subplots(1, 2, figsize=(14, 5), facecolor=BG_COLOR)
    fig.suptitle("Incentive Active Duration – Snapp vs Tapsi",
                 fontsize=15, fontweight="bold", y=0.99)
    for ax, col, color, label in [
        (axes[0], "snapp_incentive_length", SNAPP_COLOR, "Snapp"),
        (axes[1], "tapsi_incentive_active_duration", TAPSI_COLOR, "Tapsi"),
    ]:
        if col not in short.columns:
            ax.set_title(f"{label} (no data)")
            style_ax(ax)
            continue
        data = short[col].dropna()
        present = [t for t in time_order if t in data.values]
        vc = data.value_counts().reindex(present).dropna()
        total = vc.sum()
        ax.bar(vc.index, vc.values, color=color, edgecolor="white")
        for i, (k, v) in enumerate(vc.items()):
            ax.text(i, v + 5, f"{v:,}\n({v/total*100:.0f}%)",
                    ha="center", fontsize=9)
        ax.set_title(f"{label} Incentive Duration  (n={total:,})", fontsize=11)
        ax.set_xlabel("Duration")
        ax.set_ylabel("Count")
        style_ax(ax)
    save_fig(pdf, fig)

    # PAGE 57 – TAPSI RE-ACTIVATION TIMING
    # How long a driver was inactive before responding to a Tapsi incentive.
    # Business value: if many responders were dormant >6 months, incentives are
    # effectively re-acquiring churned drivers, not just activating current ones.
    inact_col = "tapsi_inactive_b4_incentive"
    inact_order = ["Same Day", "1_3 Day Before", "4_7 Day Before", "1_4 Week Before",
                   "1_3 Month Before", "3_6 Month Before", ">6 Month Before"]
    fig, ax = plt.subplots(figsize=(12, 6), facecolor=BG_COLOR)
    fig.suptitle("Tapsi Re-activation Timing: Inactivity Before Incentive Response",
                 fontsize=15, fontweight="bold", y=1.01)
    if inact_col in short.columns:
        data = short[inact_col].dropna()
        present = [v for v in inact_order if v in data.values]
        vc = data.value_counts().reindex(present).dropna()
        total = vc.sum()
        bar_colors = [TAPSI_COLOR if i == 0 else (
            "#FFA726" if i <= 2 else LGREY) for i in range(len(vc))]
        ax.barh(vc.index[::-1], vc.values[::-1],
                color=list(reversed(bar_colors)), edgecolor="white")
        for i, (k, v) in enumerate(zip(vc.index[::-1], vc.values[::-1])):
            ax.text(v + 20, i, f"{v:,} ({v/total*100:.0f}%)",
                    va="center", fontsize=9)
        ax.set_xlabel("Count")
        ax.set_title(
            f"(n={total:,}) — orange = recently active, grey = long dormant", fontsize=10)
    else:
        ax.set_title(f"{inact_col} not found in data", fontsize=11)
    style_ax(ax)
    save_fig(pdf, fig)

    # PAGE 58 – APP NPS vs PLATFORM NPS
    # Compares two different NPS questions: "Would you recommend the driver app?"
    # vs "Would you recommend driving on this platform?"
    # Business value: if App NPS is much lower than Platform NPS, the app itself
    # (not the business model) is the problem.
    #
    # CHARTING PATTERN -- "NPS distribution with color-coded bars":
    # Each bar is colored by NPS category: red (0-6 = detractor),
    # grey (7-8 = passive), green (9-10 = promoter).
    # This visual instantly shows the promoter/detractor split.
    fig, axes = plt.subplots(2, 2, figsize=(14, 10), facecolor=BG_COLOR)
    fig.suptitle("App NPS vs Platform NPS – Snapp & Tapsi",
                 fontsize=15, fontweight="bold", y=0.98)
    nps_pairs = [
        (axes[0, 0], "snapp_refer_others", SNAPP_COLOR,
         "Snapp APP NPS\n(recommend the SnappDriver app)"),
        (axes[0, 1], "snapp_recommend", SNAPP_COLOR,
         "Snapp PLATFORM NPS\n(recommend driving on Snapp)"),
        (axes[1, 0], "tapsi_refer_others", TAPSI_COLOR,
         "Tapsi APP NPS\n(recommend the Tapsi Driver app)"),
        (axes[1, 1], "tapsidriver_tapsi_recommend", TAPSI_COLOR,
         "Tapsi PLATFORM NPS\n(recommend driving on Tapsi)"),
    ]
    for ax, col, color, label in nps_pairs:
        if col not in short.columns:
            ax.set_title(f"{label}\n(no data)", fontsize=9)
            style_ax(ax)
            continue
        data = short[col].dropna()
        nps = nps_score(data)
        promoters = (data >= 9).sum()
        detractors = (data <= 6).sum()
        total = len(data)
        vc = data.value_counts().sort_index()
        bar_clr = ["#EF5350" if s <= 6 else (
            "#B0BEC5" if s <= 8 else "#66BB6A") for s in vc.index]
        ax.bar(vc.index.astype(str), vc.values,
               color=bar_clr, edgecolor="white")
        for xi, (k, v) in enumerate(vc.items()):
            ax.text(xi, v + 3, f"{v}", ha="center", fontsize=7)
        ax.set_title(
            f"{label}\nNPS={nps:+.0f}  |  P={promoters/total*100:.0f}% D={detractors/total*100:.0f}% (n={total:,})", fontsize=9)
        ax.set_xlabel("Score (0–10)")
        ax.set_ylabel("Count")
        style_ax(ax)
    save_fig(pdf, fig)

    # PAGE 59 – COMMISSION KNOWLEDGE x SATISFACTION CROSS-TAB
    # Groups drivers by their commission knowledge response, then shows mean
    # overall satisfaction for each group.
    # Business value: tests the hypothesis that drivers who understand commissions
    # are more (or less) satisfied than those who do not.
    fig, axes = plt.subplots(1, 2, figsize=(14, 6), facecolor=BG_COLOR)
    fig.suptitle("Commission Knowledge × Overall Satisfaction Cross-Tab",
                 fontsize=15, fontweight="bold", y=0.99)
    for ax, comm_col, sat_col, color, label in [
        (axes[0], "snapp_comm_info",
         "snapp_overall_satisfaction", SNAPP_COLOR, "Snapp"),
        (axes[1], "tapsi_comm_info",
         "tapsi_overall_satisfaction", TAPSI_COLOR, "Tapsi"),
    ]:
        sub = short[[comm_col, sat_col]].dropna()
        if len(sub) == 0:
            ax.set_title(f"{label} (no data)")
            style_ax(ax)
            continue
        comm_groups = sub.groupby(comm_col)[sat_col].mean().sort_values()
        n_per_group = sub.groupby(comm_col)[sat_col].count()
        ax.barh(comm_groups.index, comm_groups.values,
                color=color, edgecolor="white", alpha=0.85)
        for i, (k, v) in enumerate(comm_groups.items()):
            ax.text(
                v + 0.02, i, f"{v:.2f}  (n={n_per_group[k]:,})", va="center", fontsize=8)
        ax.set_xlim(0, 5.5)
        ax.axvline(sub[sat_col].mean(), color="black", linestyle="--", linewidth=1,
                   label=f"Overall mean: {sub[sat_col].mean():.2f}")
        ax.set_title(f"{label}: Sat. by Commission Knowledge", fontsize=11)
        ax.set_xlabel("Mean Satisfaction (1–5)")
        ax.legend(frameon=False, fontsize=9)
        style_ax(ax)
    save_fig(pdf, fig)

    # PAGE 60 – UNPAID FARE FOLLOW-UP SATISFACTION
    # Satisfaction with the unpaid fare resolution process: overall and time-to-resolve.
    # Business value: measures whether the follow-up process itself is satisfactory,
    # not just whether the issue was resolved.
    fig, axes = plt.subplots(1, 2, figsize=(14, 6), facecolor=BG_COLOR)
    fig.suptitle("Unpaid Fare Follow-up Satisfaction – Snapp vs Tapsi",
                 fontsize=15, fontweight="bold", y=0.99)
    followup_pairs = [
        ("snapp_satisfaction_followup_overall", "tapsi_satisfaction_followup_overall",
         "Overall Satisfaction with Follow-up"),
        ("snapp_satisfaction_followup_time", "tapsi_satisfaction_followup_time",
         "Satisfaction with Time-to-Resolve"),
    ]
    for ax, (scol, tcol, label) in zip(axes, followup_pairs):
        s_data = short[scol].dropna(
        ) if scol in short.columns else pd.Series(dtype=float)
        t_data = short[tcol].dropna(
        ) if tcol in short.columns else pd.Series(dtype=float)
        ratings = [1, 2, 3, 4, 5]
        s_counts = [s_data.value_counts().get(r, 0) for r in ratings]
        t_counts = [t_data.value_counts().get(r, 0) for r in ratings]
        x = np.arange(len(ratings))
        w = 0.35
        ax.bar(x - w/2, s_counts, w, color=SNAPP_COLOR,
               label=f"Snapp (n={len(s_data):,})", edgecolor="white")
        ax.bar(x + w/2, t_counts, w, color=TAPSI_COLOR,
               label=f"Tapsi (n={len(t_data):,})", edgecolor="white")
        s_mean = s_data.mean() if len(s_data) else 0
        t_mean = t_data.mean() if len(t_data) else 0
        ax.set_xticks(x)
        ax.set_xticklabels([str(r) for r in ratings])
        ax.set_title(
            f"{label}\nSnapp mean={s_mean:.2f}  |  Tapsi mean={t_mean:.2f}", fontsize=10)
        ax.set_xlabel("Rating (1–5)")
        ax.set_ylabel("Count")
        ax.legend(frameon=False, fontsize=9)
        style_ax(ax)
    save_fig(pdf, fig)

    # PAGE 61 – TRIP LENGTH PREFERENCE
    # What trip length drivers prefer to accept (short, average, long).
    # Business value: if most prefer short trips, long-trip pricing may need adjustment.
    trip_order = ["Short Trip", "Average Trip", "Long Trip"]
    fig, axes = plt.subplots(1, 2, figsize=(12, 5), facecolor=BG_COLOR)
    fig.suptitle("Trip Length Preference – What Drivers Mostly Accept",
                 fontsize=15, fontweight="bold", y=0.99)
    for ax, col, color, label in [
        (axes[0], "snapp_accepted_trip_length", SNAPP_COLOR, "Snapp"),
        (axes[1], "tapsi_accepted_trip_length", TAPSI_COLOR, "Tapsi"),
    ]:
        if col not in short.columns:
            ax.set_title(f"{label} (no data)")
            style_ax(ax)
            continue
        data = short[col].dropna()
        present = [t for t in trip_order if t in data.values]
        vc = data.value_counts().reindex(present).dropna()
        total = vc.sum()
        ax.bar(vc.index, vc.values, color=color, edgecolor="white")
        for i, (k, v) in enumerate(vc.items()):
            ax.text(i, v + 20, f"{v:,}\n({v/total*100:.0f}%)",
                    ha="center", fontsize=9)
        ax.set_title(
            f"{label} Trip Length Preference  (n={total:,})", fontsize=11)
        ax.set_xlabel("Trip Type")
        ax.set_ylabel("Count")
        style_ax(ax)
    save_fig(pdf, fig)

    # PAGE 62 – NAVIGATION APP USED IN LAST TRIP
    # Which navigation app drivers actually used in their most recent trip.
    # Business value: actual usage (vs familiarity on PAGE 18) shows real adoption.
    fig, axes = plt.subplots(1, 2, figsize=(14, 6), facecolor=BG_COLOR)
    fig.suptitle("Navigation App Used in Last Trip – Snapp vs Tapsi",
                 fontsize=15, fontweight="bold", y=0.99)
    for ax, col, nav_order, color, label in [
        (axes[0], "snapp_last_trip_navigation", ["Neshan", "Balad",
         "Google Map", "Waze", "No Navigation App"], SNAPP_COLOR, "Snapp"),
        (axes[1], "tapsi_navigation_type", ["Neshan", "Balad",
         "In-App Navigation", "No Navigation App"], TAPSI_COLOR, "Tapsi"),
    ]:
        if col not in short.columns:
            ax.set_title(f"{label} (no data)")
            style_ax(ax)
            continue
        data = short[col].dropna()
        present = [v for v in nav_order if v in data.values]
        vc = data.value_counts().reindex(present).dropna()
        total = vc.sum()
        bar_colors_nav = [LGREY if "No" in k else (
            TAPSI_COLOR if "In-App" in k and color == TAPSI_COLOR else color) for k in vc.index]
        ax.barh(vc.index[::-1], vc.values[::-1],
                color=list(reversed(bar_colors_nav)), edgecolor="white")
        for i, (k, v) in enumerate(zip(vc.index[::-1], vc.values[::-1])):
            ax.text(v + 50, i, f"{v:,} ({v/total*100:.0f}%)",
                    va="center", fontsize=9)
        ax.set_title(
            f"{label} Navigation in Last Trip  (n={total:,})", fontsize=11)
        ax.set_xlabel("Count")
        style_ax(ax)
    save_fig(pdf, fig)

    # PAGE 63 – JOINING BONUS
    # Whether drivers received a bonus when they first registered on each platform.
    # Business value: measures the reach of new-driver acquisition programs.
    fig, axes = plt.subplots(1, 2, figsize=(12, 5), facecolor=BG_COLOR)
    fig.suptitle("Joining Bonus & Registration Origin – Snapp vs Tapsi",
                 fontsize=15, fontweight="bold", y=0.99)
    for ax, col, color, label in [
        (axes[0], "snapp_joining_bonus", SNAPP_COLOR, "Snapp"),
        (axes[1], "tapsi_joining_bonus", TAPSI_COLOR, "Tapsi"),
    ]:
        if col not in short.columns:
            ax.set_title(f"{label} (no data)")
            style_ax(ax)
            continue
        data = short[col].dropna().value_counts()
        total = data.sum()
        bonus_colors = {"Yes": "#66BB6A", "No": "#EF5350"}
        ax.bar(data.index, data.values, color=[bonus_colors.get(
            k, GREY) for k in data.index], edgecolor="white")
        for i, (k, v) in enumerate(data.items()):
            ax.text(i, v + 20, f"{v:,}\n({v/total*100:.0f}%)",
                    ha="center", fontsize=9)
        ax.set_title(
            f"{label} Joining/Registration Bonus  (n={total:,})", fontsize=11)
        ax.set_xlabel("Received Bonus?")
        ax.set_ylabel("Count")
        style_ax(ax)
    save_fig(pdf, fig)

    # PAGE 64 – TAPSI NAVIGATION DEEP-DIVE
    # 4-panel: Tapsi in-app navigation usage and satisfaction, offline navigation
    # familiarity and usage during GPS issues.
    # Business value: Tapsi has built its own navigation -- how well is it working?
    fig, axes = plt.subplots(2, 2, figsize=(14, 10), facecolor=BG_COLOR)
    fig.suptitle("Tapsi Navigation Deep-Dive – In-App & Offline Navigation",
                 fontsize=15, fontweight="bold", y=0.98)
    for ax, col, color, label in [
        (axes[0, 0], "tapsi_in_app_navigation_usage",
         TAPSI_COLOR, "Used Tapsi In-App Navigation"),
        (axes[0, 1], "tapsi_in_app_navigation_satisfaction",
         TAPSI_COLOR, "In-App Navigation Satisfaction (1–5)"),
        (axes[1, 0], "tapsi_offline_navigation_familiar",
         "#5C6BC0", "Familiar with Tapsi Offline Navigation"),
        (axes[1, 1], "tapsi_offline_navigation_usage", "#5C6BC0",
         "Offline Navigation Usage During GPS Issues"),
    ]:
        if col not in short.columns:
            ax.set_title(f"{label}\n(no data)", fontsize=10)
            style_ax(ax)
            continue
        data = short[col].dropna().value_counts()
        total = data.sum()
        ax.bar(data.index.astype(str), data.values,
               color=color, edgecolor="white")
        for i, (k, v) in enumerate(data.items()):
            ax.text(i, v + 5, f"{v:,}\n({v/total*100:.0f}%)",
                    ha="center", fontsize=9)
        ax.set_title(f"{label}  (n={total:,})", fontsize=10)
        ax.set_ylabel("Count")
        style_ax(ax)
    save_fig(pdf, fig)

    # PAGE 65 – TAPSI GPS PERFORMANCE & MAGICAL WINDOW
    # 3-panel: driver perception of Tapsi GPS vs Snapp, Magical Window awareness,
    # and Tapsi Incentive GotBonus from the long survey.
    # Business value: competitive GPS perception and feature awareness tracking.
    fig, axes = plt.subplots(1, 3, figsize=(16, 5), facecolor=BG_COLOR)
    fig.suptitle("Tapsi: GPS Performance Perception & Magical Window / Referral Program",
                 fontsize=15, fontweight="bold", y=0.99)
    ax = axes[0]
    if "tapsi_gps_better" in short.columns:
        data = short["tapsi_gps_better"].dropna().value_counts()
        total = data.sum()
        gps_colors = {"Yes": "#66BB6A", "No": "#EF5350", "Similar": "#FFA726"}
        ax.bar(data.index, data.values, color=[gps_colors.get(
            k, GREY) for k in data.index], edgecolor="white")
        for i, (k, v) in enumerate(data.items()):
            ax.text(i, v + 5, f"{v:,}\n({v/total*100:.0f}%)",
                    ha="center", fontsize=9)
        ax.set_title(
            f"Was Tapsi App Better\nDuring GPS Issues?  (n={total:,})", fontsize=10)
        ax.set_ylabel("Count")
        style_ax(ax)
    else:
        ax.set_title("tapsi_gps_better (no data)")
        style_ax(ax)
    ax = axes[1]
    if "tapsi_magical_window" in short.columns:
        data = short["tapsi_magical_window"].dropna().value_counts()
        total = data.sum()
        mw_clrs = {"Yes": TAPSI_COLOR, "No": "#FFA726", "Not Familiar": LGREY}
        ax.bar(data.index, data.values, color=[mw_clrs.get(
            k, GREY) for k in data.index], edgecolor="white")
        for i, (k, v) in enumerate(data.items()):
            ax.text(i, v + 10, f"{v:,}\n({v/total*100:.0f}%)",
                    ha="center", fontsize=9)
        ax.set_title(
            f"Tapsi Magical Window\nAwareness  (n={total:,})", fontsize=10)
        ax.set_ylabel("Count")
        style_ax(ax)
    else:
        ax.set_title("tapsi_magical_window (no data)")
        style_ax(ax)
    ax = axes[2]
    tapsi_ref_q = long[long["question"] ==
                       "Tapsi Incentive GotBonus"] if "question" in long.columns else pd.DataFrame()
    if len(tapsi_ref_q) > 0:
        vc = tapsi_ref_q["answer"].value_counts().sort_values(ascending=True)
        total = vc.sum()
        ax.barh(vc.index, vc.values, color=TAPSI_COLOR, edgecolor="white")
        for i, (k, v) in enumerate(vc.items()):
            ax.text(v + 1, i, f"{v} ({v/total*100:.0f}%)",
                    va="center", fontsize=8)
        ax.set_title(
            f"Tapsi Incentive GotBonus\n(long_survey, n={total:,})", fontsize=10)
        ax.set_xlabel("Count")
        style_ax(ax)
    else:
        ax.set_title(
            "Tapsi Incentive GotBonus\n(no data in long_survey)", fontsize=10)
        style_ax(ax)
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 66 – SPEED SATISFACTION
    # ================================================================
    # Distribution of app speed satisfaction (1-5) for each platform.
    # Business value: app performance (loading times, lag) directly affects
    # driver productivity and willingness to use the platform.
    # Same paired-histogram pattern as PAGE 8.
    # Note: pages 66+ use HAVE_SHORT/HAVE_WIDE guard checks and produce
    # placeholder pages when data is unavailable (graceful degradation).
    if not HAVE_SHORT:
        placeholder_page(pdf, "Page 66 – Speed Satisfaction",
                         "short DataFrame not available")
    else:
        scol_speed = "snapp_speed_satisfaction"
        tcol_speed = "tapsi_speed_satisfaction"
        have_scol = scol_speed in short.columns
        have_tcol = tcol_speed in short.columns
        if not have_scol and not have_tcol:
            placeholder_page(pdf, "Page 66 – Speed Satisfaction",
                             "Neither snapp_speed_satisfaction nor "
                             "tapsi_speed_satisfaction found in short")
        else:
            fig, axes = plt.subplots(1, 2, figsize=(12, 5),
                                     facecolor=BG_COLOR, sharey=True)
            fig.suptitle("Speed Satisfaction Distribution (1\u20135 scale)",
                         fontsize=15, fontweight="bold", y=0.99)
            for ax, col, color, label in [
                (axes[0], scol_speed, SNAPP_COLOR, "Snapp"),
                (axes[1], tcol_speed, TAPSI_COLOR, "Tapsi"),
            ]:
                if col not in short.columns:
                    ax.set_title(f"{label}  (column missing)", fontsize=11)
                    style_ax(ax)
                    continue
                data = short[col].dropna()
                if len(data) == 0:
                    ax.set_title(f"{label}  (no data)", fontsize=11)
                    style_ax(ax)
                    continue
                counts = data.value_counts().sort_index()
                ax.bar(counts.index.astype(int).astype(str),
                       counts.values, color=color, edgecolor="white")
                total = counts.sum()
                for x, y in zip(counts.index.astype(int).astype(str),
                                counts.values):
                    ax.text(x, y + 1, f"{y}\n({y/total*100:.0f}%)",
                            ha="center", fontsize=8)
                ax.set_title(
                    f"{label}  (n={int(total)}, mean={data.mean():.2f})",
                    fontsize=11)
                ax.set_xlabel("Rating")
                style_ax(ax)
            axes[0].set_ylabel("Count")
            save_fig(pdf, fig)

    # ================================================================
    # PAGE 67 – SNAPP CARFIX DEEP-DIVE
    # ================================================================
    # CarFix is Snapp's car maintenance / parts marketplace for drivers.
    # 4-panel layout: adoption funnel (familiar -> ever used -> used last month),
    # satisfaction across multiple dimensions, recommendation NPS, and
    # reasons for not using (from long survey).
    # Business value: measures whether drivers are adopting this ancillary service
    # and where satisfaction gaps exist in the buying experience.
    #
    # CHARTING PATTERN -- "adoption funnel":
    # A series of bars where each subsequent bar should be smaller (or equal),
    # showing progressive drop-off from awareness to usage.
    # Colors darken at each stage to reinforce the narrowing funnel.
    if not HAVE_SHORT:
        placeholder_page(pdf, "Page 67 – Snapp CarFix Deep-Dive",
                         "short DataFrame not available")
    else:
        _cf_funnel_cols = ["snappcarfix_familiar", "snappcarfix_use_ever",
                           "snappcarfix_use_lastmo"]
        _cf_sat_cols = [
            ("snappcarfix_satisfaction_overall", "Overall"),
            ("snappcarfix_satisfaction_experience", "Experience"),
            ("snappcarfix_satisfaction_productprice", "Product Price"),
            ("snappcarfix_satisfaction_quality", "Quality"),
            ("snappcarfix_satisfaction_variety", "Variety"),
            ("snappcarfix_satisfaction_buyingprocess", "Buying Process"),
            ("snappcarfix_satisfaction_deliverytime", "Delivery Time"),
            ("snappcarfix_satisfaction_waittime", "Wait Time"),
            ("snappcarfix_satisfaction_behaviour", "Behaviour"),
        ]
        _cf_lastm_cols = [
            ("snappcarfix_satisfaction_quality_lastm", "Quality (last mo)"),
            ("snappcarfix_satisfaction_price_lastm", "Price (last mo)"),
            ("snappcarfix_satisfaction_variety_lastm", "Variety (last mo)"),
            ("snappcarfix_satisfaction_easyusage", "Easy Usage"),
            ("snappcarfix_satisfaction_ontimedelivery", "On-time Delivery"),
            ("snappcarfix_satisfaction_CS", "Customer Support"),
        ]
        _cf_rec_col = "snappcarfix_recommend"

        fig, axes = plt.subplots(2, 2, figsize=(14, 10), facecolor=BG_COLOR)
        fig.suptitle("Snapp CarFix \u2013 Funnel, Satisfaction & Recommendation",
                     fontsize=15, fontweight="bold", y=0.98)

        # --- Top-left: funnel (familiar -> use_ever -> use_lastmo) ---
        ax = axes[0, 0]
        funnel_labels = ["Familiar", "Ever Used", "Used Last Month"]
        funnel_vals = []
        for fc in _cf_funnel_cols:
            if fc in short.columns:
                vc = short[fc].dropna().value_counts()
                funnel_vals.append(vc.get("Yes", 0))
            else:
                funnel_vals.append(0)
        total_resp = sum(short[_cf_funnel_cols[0]].dropna().shape[0]
                         if _cf_funnel_cols[0] in short.columns else 0
                         for _ in [1])
        bar_colors_f = [SNAPP_COLOR, "#66BB6A", "#43A047"]
        bars = ax.bar(funnel_labels, funnel_vals, color=bar_colors_f,
                      edgecolor="white")
        for b, v in zip(bars, funnel_vals):
            pct = f" ({v/total_resp*100:.0f}%)" if total_resp > 0 else ""
            ax.text(b.get_x() + b.get_width() / 2, v + max(funnel_vals) * 0.02,
                    f"{v:,}{pct}", ha="center", fontsize=9, fontweight="bold")
        ax.set_title(f"Adoption Funnel  (n={total_resp:,})", fontsize=11)
        ax.set_ylabel("Count (Yes)")
        style_ax(ax)

        # --- Top-right: satisfaction bars (all sat + last-month) ---
        ax = axes[0, 1]
        all_sat = _cf_sat_cols + _cf_lastm_cols
        sat_labels = []
        sat_means = []
        for col, lbl in all_sat:
            if col in short.columns:
                d = pd.to_numeric(short[col], errors="coerce").dropna()
                if len(d) > 0:
                    sat_labels.append(lbl)
                    sat_means.append(d.mean())
        if sat_labels:
            y_pos = np.arange(len(sat_labels))
            bars_s = ax.barh(y_pos, sat_means, color=SNAPP_COLOR,
                             edgecolor="white", height=0.6)
            for i, v in enumerate(sat_means):
                ax.text(v + 0.05, i, f"{v:.2f}", va="center", fontsize=8,
                        fontweight="bold")
            ax.set_yticks(y_pos)
            ax.set_yticklabels(sat_labels, fontsize=8)
            ax.set_xlim(0, 5.5)
            ax.set_xlabel("Mean (1\u20135)")
            ax.invert_yaxis()
        ax.set_title("Satisfaction Dimensions", fontsize=11)
        style_ax(ax)

        # --- Bottom-left: recommendation distribution (0-10) ---
        ax = axes[1, 0]
        if _cf_rec_col in short.columns:
            rec_data = pd.to_numeric(short[_cf_rec_col],
                                     errors="coerce").dropna()
            if len(rec_data) > 0:
                rec_counts = rec_data.value_counts().sort_index()
                rec_total = rec_counts.sum()
                colors_nps = []
                for idx in rec_counts.index:
                    if idx <= 6:
                        colors_nps.append("#EF5350")
                    elif idx <= 8:
                        colors_nps.append(LGREY)
                    else:
                        colors_nps.append(SNAPP_COLOR)
                ax.bar(rec_counts.index.astype(int).astype(str),
                       rec_counts.values, color=colors_nps, edgecolor="white")
                for x, y in zip(rec_counts.index.astype(int).astype(str),
                                rec_counts.values):
                    ax.text(x, y + rec_total * 0.005,
                            f"{y}\n({y/rec_total*100:.0f}%)",
                            ha="center", fontsize=7)
                _nps = nps_score(rec_data)
                ax.set_title(f"Recommendation (0\u201310)  NPS={_nps:.1f}  "
                             f"n={int(rec_total)}", fontsize=10)
            else:
                ax.set_title("Recommendation (no data)", fontsize=11)
        else:
            ax.set_title("snappcarfix_recommend not found", fontsize=11)
        ax.set_xlabel("Score")
        ax.set_ylabel("Count")
        style_ax(ax)

        # --- Bottom-right: NotUse Reasons from long ---
        ax = axes[1, 1]
        if HAVE_LONG and "question" in long.columns:
            notuse_q = "Snappcarfix NotUse Reason"
            notuse_data = long[long["question"] == notuse_q]
            if len(notuse_data) > 0:
                ans_counts = notuse_data["answer"].dropna().value_counts(
                ).sort_values(ascending=True)
                total_nu = ans_counts.sum()
                ax.barh(ans_counts.index, ans_counts.values,
                        color=ACCENT, edgecolor="white")
                for i, (ans, v) in enumerate(ans_counts.items()):
                    ax.text(v + total_nu * 0.01, i,
                            f"{v} ({v/total_nu*100:.0f}%)",
                            va="center", fontsize=8)
                ax.set_title(f"Not-Use Reasons  (n={total_nu})", fontsize=11)
            else:
                ax.set_title("NotUse Reasons (no data in long)", fontsize=11)
        else:
            ax.set_title("NotUse Reasons (long not available)", fontsize=11)
        ax.set_xlabel("Count")
        style_ax(ax)

        save_fig(pdf, fig)

    # ================================================================
    # PAGE 68 – TAPSI GARAGE DEEP-DIVE
    # ================================================================
    # Tapsi Garage is Tapsi's equivalent of Snapp CarFix (car maintenance marketplace).
    # Same 4-panel layout as PAGE 67 (funnel, satisfaction, recommendation, not-use reasons).
    # Business value: direct competitive comparison with CarFix adoption and satisfaction.
    # Same adoption-funnel pattern as PAGE 67.
    if not HAVE_SHORT:
        placeholder_page(pdf, "Page 68 – Tapsi Garage Deep-Dive",
                         "short DataFrame not available")
    else:
        _tg_funnel_cols = ["tapsigarage_familiar", "tapsigarage_use_ever",
                           "tapsigarage_use_lastmo"]
        _tg_sat_cols = [
            ("tapsigarage_satisfaction_overall", "Overall"),
            ("tapsigarage_satisfaction_experience", "Experience"),
            ("tapsigarage_satisfaction_productprice", "Product Price"),
            ("tapsigarage_satisfaction_quality_experience", "Quality"),
            ("tapsigarage_satisfaction_variety_experience", "Variety"),
            ("tapsigarage_satisfaction_buyingprocess", "Buying Process"),
            ("tapsigarage_satisfaction_deliverytime", "Delivery Time"),
            ("tapsigarage_satisfaction_waittime", "Wait Time"),
            ("tapsigarage_satisfaction_behaviour", "Behaviour"),
        ]
        _tg_lastm_cols = [
            ("tapsigarage_satisfaction_quality", "Quality (last mo)"),
            ("tapsigarage_satisfaction_price", "Price (last mo)"),
            ("tapsigarage_satisfaction_variety", "Variety (last mo)"),
            ("tapsigarage_satisfaction_easyusage", "Easy Usage"),
            ("tapsigarage_satisfaction_ontimedelivery", "On-time Delivery"),
            ("tapsigarage_satisfaction_CS", "Customer Support"),
        ]
        _tg_rec_col = "tapsigarage_recommend"

        fig, axes = plt.subplots(2, 2, figsize=(14, 10), facecolor=BG_COLOR)
        fig.suptitle("Tapsi Garage \u2013 Funnel, Satisfaction & Recommendation",
                     fontsize=15, fontweight="bold", y=0.98)

        # --- Top-left: funnel ---
        ax = axes[0, 0]
        funnel_labels_tg = ["Familiar", "Ever Used", "Used Last Month"]
        funnel_vals_tg = []
        for fc in _tg_funnel_cols:
            if fc in short.columns:
                vc = short[fc].dropna().value_counts()
                funnel_vals_tg.append(vc.get("Yes", 0))
            else:
                funnel_vals_tg.append(0)
        total_resp_tg = (short[_tg_funnel_cols[0]].dropna().shape[0]
                         if _tg_funnel_cols[0] in short.columns else 0)
        bar_colors_tg = [TAPSI_COLOR, "#FFA726", "#F57C00"]
        bars = ax.bar(funnel_labels_tg, funnel_vals_tg, color=bar_colors_tg,
                      edgecolor="white")
        for b, v in zip(bars, funnel_vals_tg):
            pct = (f" ({v/total_resp_tg*100:.0f}%)"
                   if total_resp_tg > 0 else "")
            ax.text(b.get_x() + b.get_width() / 2,
                    v + max(funnel_vals_tg) * 0.02 if max(funnel_vals_tg) > 0 else 1,
                    f"{v:,}{pct}", ha="center", fontsize=9, fontweight="bold")
        ax.set_title(f"Adoption Funnel  (n={total_resp_tg:,})", fontsize=11)
        ax.set_ylabel("Count (Yes)")
        style_ax(ax)

        # --- Top-right: satisfaction bars ---
        ax = axes[0, 1]
        all_sat_tg = _tg_sat_cols + _tg_lastm_cols
        sat_labels_tg = []
        sat_means_tg = []
        for col, lbl in all_sat_tg:
            if col in short.columns:
                d = pd.to_numeric(short[col], errors="coerce").dropna()
                if len(d) > 0:
                    sat_labels_tg.append(lbl)
                    sat_means_tg.append(d.mean())
        if sat_labels_tg:
            y_pos = np.arange(len(sat_labels_tg))
            ax.barh(y_pos, sat_means_tg, color=TAPSI_COLOR,
                    edgecolor="white", height=0.6)
            for i, v in enumerate(sat_means_tg):
                ax.text(v + 0.05, i, f"{v:.2f}", va="center", fontsize=8,
                        fontweight="bold")
            ax.set_yticks(y_pos)
            ax.set_yticklabels(sat_labels_tg, fontsize=8)
            ax.set_xlim(0, 5.5)
            ax.set_xlabel("Mean (1\u20135)")
            ax.invert_yaxis()
        ax.set_title("Satisfaction Dimensions", fontsize=11)
        style_ax(ax)

        # --- Bottom-left: recommendation distribution ---
        ax = axes[1, 0]
        if _tg_rec_col in short.columns:
            rec_data_tg = pd.to_numeric(short[_tg_rec_col],
                                        errors="coerce").dropna()
            if len(rec_data_tg) > 0:
                rec_counts_tg = rec_data_tg.value_counts().sort_index()
                rec_total_tg = rec_counts_tg.sum()
                colors_nps_tg = []
                for idx in rec_counts_tg.index:
                    if idx <= 6:
                        colors_nps_tg.append("#EF5350")
                    elif idx <= 8:
                        colors_nps_tg.append(LGREY)
                    else:
                        colors_nps_tg.append(TAPSI_COLOR)
                ax.bar(rec_counts_tg.index.astype(int).astype(str),
                       rec_counts_tg.values, color=colors_nps_tg,
                       edgecolor="white")
                for x, y in zip(rec_counts_tg.index.astype(int).astype(str),
                                rec_counts_tg.values):
                    ax.text(x, y + rec_total_tg * 0.005,
                            f"{y}\n({y/rec_total_tg*100:.0f}%)",
                            ha="center", fontsize=7)
                _nps_tg = nps_score(rec_data_tg)
                ax.set_title(f"Recommendation (0\u201310)  NPS={_nps_tg:.1f}  "
                             f"n={int(rec_total_tg)}", fontsize=10)
            else:
                ax.set_title("Recommendation (no data)", fontsize=11)
        else:
            ax.set_title("tapsigarage_recommend not found", fontsize=11)
        ax.set_xlabel("Score")
        ax.set_ylabel("Count")
        style_ax(ax)

        # --- Bottom-right: placeholder (no NotUse question for Tapsi Garage) ---
        ax = axes[1, 1]
        ax.text(0.5, 0.5, "No NotUse Reason question\navailable for "
                "Tapsi Garage", ha="center", va="center",
                fontsize=12, color=GREY, transform=ax.transAxes)
        ax.set_title("Not-Use Reasons (N/A)", fontsize=11)
        ax.axis("off")

        save_fig(pdf, fig)

    # ================================================================
    # PAGE 69 – MIXED INCENTIVE STRATEGY
    # ================================================================
    # "Mixed incentive" is a combined incentive approach.  This page covers
    # awareness, activation familiarity, trip effect, Snapp-only preference,
    # and driver choice among incentive options.
    # Business value: tests whether a mixed incentive model would retain more drivers.
    #
    # CHARTING PATTERN -- "gridspec layout":
    # gridspec.GridSpec(2, 3) creates a flexible grid where the bottom-right
    # panel spans two columns (gs[1, 1:]).  This is useful when panels have
    # unequal importance -- the "choice" panel needs more width.
    if not HAVE_SHORT:
        placeholder_page(pdf, "Page 69 – Mixed Incentive Strategy",
                         "short DataFrame not available")
    else:
        _mix_cols = ["mixincentive", "mixincentive_activate_familiar",
                     "mixincentive_tripeffect", "mixincentive_onlysnapp",
                     "mixincentive_choice"]
        _mix_present = [c for c in _mix_cols if c in short.columns]
        if len(_mix_present) == 0:
            placeholder_page(pdf, "Page 69 – Mixed Incentive Strategy",
                             "No mixincentive columns found in short")
        else:
            fig = plt.figure(figsize=(15, 10), facecolor=BG_COLOR)
            fig.suptitle("Mixed Incentive Strategy \u2013 Awareness, Activation "
                         "& Preferences",
                         fontsize=15, fontweight="bold", y=0.98)
            gs = gridspec.GridSpec(2, 3, figure=fig, hspace=0.35, wspace=0.35)

            # --- Awareness pie ---
            ax = fig.add_subplot(gs[0, 0])
            if "mixincentive" in short.columns:
                aw_data = short["mixincentive"].dropna().value_counts()
                if len(aw_data) > 0:
                    aw_colors = [SNAPP_COLOR if "Yes" in str(k) else LGREY
                                 for k in aw_data.index]
                    ax.pie(aw_data.values, labels=aw_data.index,
                           autopct="%1.0f%%", colors=aw_colors,
                           startangle=90,
                           wedgeprops={"edgecolor": "white"},
                           textprops={"fontsize": 8})
                    ax.set_title(f"Awareness  (n={aw_data.sum()})",
                                 fontsize=11)
                else:
                    ax.set_title("Awareness (no data)", fontsize=11)
            else:
                ax.set_title("mixincentive not found", fontsize=11)

            # --- Activation familiarity ---
            ax = fig.add_subplot(gs[0, 1])
            if "mixincentive_activate_familiar" in short.columns:
                af_data = short["mixincentive_activate_familiar"].dropna(
                ).value_counts()
                if len(af_data) > 0:
                    af_colors = [SNAPP_COLOR if "Yes" in str(k) else LGREY
                                 for k in af_data.index]
                    ax.pie(af_data.values, labels=af_data.index,
                           autopct="%1.0f%%", colors=af_colors,
                           startangle=90,
                           wedgeprops={"edgecolor": "white"},
                           textprops={"fontsize": 8})
                    ax.set_title(f"Activation Familiarity  "
                                 f"(n={af_data.sum()})", fontsize=11)
                else:
                    ax.set_title("Activation Familiarity (no data)",
                                 fontsize=11)
            else:
                ax.set_title("activate_familiar not found", fontsize=11)

            # --- Trip effect distribution ---
            ax = fig.add_subplot(gs[0, 2])
            if "mixincentive_tripeffect" in short.columns:
                te_data = short["mixincentive_tripeffect"].dropna(
                ).value_counts()
                if len(te_data) > 0:
                    total_te = te_data.sum()
                    ax.barh(te_data.index, te_data.values,
                            color=ACCENT, edgecolor="white")
                    for i, (k, v) in enumerate(te_data.items()):
                        ax.text(v + total_te * 0.01, i,
                                f"{v} ({v/total_te*100:.0f}%)",
                                va="center", fontsize=8)
                    ax.set_title(f"Trip Effect  (n={total_te})", fontsize=11)
                else:
                    ax.set_title("Trip Effect (no data)", fontsize=11)
            else:
                ax.set_title("tripeffect not found", fontsize=11)
            style_ax(ax)

            # --- Snapp-only preference ---
            ax = fig.add_subplot(gs[1, 0])
            if "mixincentive_onlysnapp" in short.columns:
                os_data = short["mixincentive_onlysnapp"].dropna(
                ).value_counts()
                if len(os_data) > 0:
                    total_os = os_data.sum()
                    ax.barh(os_data.index, os_data.values,
                            color=SNAPP_COLOR, edgecolor="white")
                    for i, (k, v) in enumerate(os_data.items()):
                        ax.text(v + total_os * 0.01, i,
                                f"{v} ({v/total_os*100:.0f}%)",
                                va="center", fontsize=8)
                    ax.set_title(f"Snapp-Only Preference  (n={total_os})",
                                 fontsize=11)
                else:
                    ax.set_title("Snapp-Only (no data)", fontsize=11)
            else:
                ax.set_title("onlysnapp not found", fontsize=11)
            style_ax(ax)

            # --- Choice distribution ---
            ax = fig.add_subplot(gs[1, 1:])
            if "mixincentive_choice" in short.columns:
                ch_data = short["mixincentive_choice"].dropna().value_counts()
                if len(ch_data) > 0:
                    total_ch = ch_data.sum()
                    ch_colors = [SNAPP_COLOR, TAPSI_COLOR, ACCENT, ACCENT2,
                                 GREY, LGREY]
                    ax.barh(ch_data.index, ch_data.values,
                            color=ch_colors[:len(ch_data)],
                            edgecolor="white")
                    for i, (k, v) in enumerate(ch_data.items()):
                        ax.text(v + total_ch * 0.01, i,
                                f"{v} ({v/total_ch*100:.0f}%)",
                                va="center", fontsize=8)
                    ax.set_title(f"Incentive Choice  (n={total_ch})",
                                 fontsize=11)
                else:
                    ax.set_title("Choice (no data)", fontsize=11)
            else:
                ax.set_title("mixincentive_choice not found", fontsize=11)
            style_ax(ax)

            save_fig(pdf, fig)

    # ================================================================
    # PAGE 70 – REQUEST REFUSAL REASONS (wide binary)
    # ================================================================
    # Why drivers refuse ride requests, from the wide survey binary columns.
    # Similar to PAGE 16 but uses different column names ("Request Refusal" vs
    # "Ride Refusal") from a different survey section.
    # Business value: identifies the most common friction points in ride acceptance.
    # Same paired-horizontal-bar pattern as PAGE 16.
    if not HAVE_WIDE:
        placeholder_page(pdf, "Page 70 – Request Refusal Reasons",
                         "wide DataFrame not available")
    else:
        _refusal_suffixes = [
            "Application Problems", "Low Fare", "Short Accept Time",
            "Not Realized There's Request", "Unfamiliar Route",
            "Wait for better Offer", "Traffic", "Unfamiliar App",
            "Target Destination", "Internet Problems",
            "Long DistanceToOrigin", "Long Route", "Working with Tapsi",
        ]
        _refusal_labels = [
            "App Problems", "Low Fare", "Short Accept Time",
            "Didn't Realize Request", "Unfamiliar Route",
            "Wait for Better Offer", "Traffic", "Unfamiliar App",
            "Target Destination", "Internet Problems",
            "Long Dist. to Origin", "Long Route", "Working w/ Other",
        ]
        snapp_ref_cols = [f"Snapp Request Refusal__{s}"
                          for s in _refusal_suffixes]
        tapsi_ref_cols = [f"Tapsi Request Refusal__{s}"
                          for s in _refusal_suffixes]
        snapp_ref_vals = [
            wide[c].sum() if c in wide.columns else 0
            for c in snapp_ref_cols
        ]
        tapsi_ref_vals = [
            wide[c].sum() if c in wide.columns else 0
            for c in tapsi_ref_cols
        ]
        if sum(snapp_ref_vals) == 0 and sum(tapsi_ref_vals) == 0:
            placeholder_page(pdf, "Page 70 – Request Refusal Reasons",
                             "No Request Refusal binary columns found "
                             "in wide")
        else:
            fig, ax = new_fig(
                "Request Refusal Reasons \u2013 Snapp vs Tapsi (wide)",
                figsize=(14, 7))
            y = np.arange(len(_refusal_labels))
            h = 0.35
            ax.barh(y - h / 2, snapp_ref_vals, h, color=SNAPP_COLOR,
                    label="Snapp", edgecolor="white")
            ax.barh(y + h / 2, tapsi_ref_vals, h, color=TAPSI_COLOR,
                    label="Tapsi", edgecolor="white")
            for i, (sv, tv) in enumerate(zip(snapp_ref_vals,
                                             tapsi_ref_vals)):
                if sv > 0:
                    ax.text(sv + 2, i - h / 2, str(int(sv)),
                            va="center", fontsize=8)
                if tv > 0:
                    ax.text(tv + 2, i + h / 2, str(int(tv)),
                            va="center", fontsize=8)
            ax.set_yticks(y)
            ax.set_yticklabels(_refusal_labels)
            ax.invert_yaxis()
            ax.legend(frameon=False, fontsize=10)
            ax.set_xlabel("Count")
            style_ax(ax)
            save_fig(pdf, fig)

    # ================================================================
    # PAGE 71 – APP NOTIFICATION CHANNELS
    # ================================================================
    # How Snapp communicates with drivers (SMS, call, app notification, Telegram,
    # Instagram) and which broadcast channels drivers follow.
    # Business value: guides marketing channel investment -- reach drivers where
    # they actually pay attention.
    # Same wide-binary-column pattern as PAGE 11.
    if not HAVE_WIDE:
        placeholder_page(pdf, "Page 71 – App Notification Channels",
                         "wide DataFrame not available")
    else:
        _msg_type_labels = ["SMS", "Call", "Snapp Drivers App",
                            "Instagram Page", "Telegram Channel",
                            "Notification Bar"]
        _msg_type_cols = [
            "Snapp Got Message Type__Sms",
            "Snapp Got Message Type__Call",
            "Snapp Got Message Type__Snapp Drivers App",
            "Snapp Got Message Type__Snapp Instagram Page",
            "Snapp Got Message Type__Snapp Telegram Channel",
            "Snapp Got Message Type__Notification Bar",
        ]
        _broadcast_labels = ["Telegram", "Instagram (drivers.snapp@)",
                             "SnappClub (club.snapp.ir)"]
        _broadcast_cols = [
            "Snapp Driversapp Broadcast Channel__Telegram",
            "Snapp Driversapp Broadcast Channel__Instagram (drivers.snapp@)",
            "Snapp Driversapp Broadcast Channel__SnappClub (club.snapp.ir)",
        ]

        msg_vals = [wide[c].sum() if c in wide.columns else 0
                    for c in _msg_type_cols]
        bcast_vals = [wide[c].sum() if c in wide.columns else 0
                      for c in _broadcast_cols]

        if sum(msg_vals) == 0 and sum(bcast_vals) == 0:
            placeholder_page(pdf, "Page 71 – App Notification Channels",
                             "No Got Message Type / Broadcast Channel "
                             "columns found in wide")
        else:
            fig, axes = plt.subplots(1, 2, figsize=(14, 6),
                                     facecolor=BG_COLOR)
            fig.suptitle("Snapp Notification Channels (wide)",
                         fontsize=15, fontweight="bold", y=0.99)

            # --- Message type ---
            ax = axes[0]
            total_msg = sum(msg_vals)
            ax.barh(_msg_type_labels, msg_vals, color=SNAPP_COLOR,
                    edgecolor="white")
            for i, v in enumerate(msg_vals):
                if v > 0:
                    pct = f" ({v/total_msg*100:.0f}%)" if total_msg > 0 else ""
                    ax.text(v + max(msg_vals) * 0.01, i,
                            f"{int(v)}{pct}", va="center", fontsize=9)
            ax.set_title(f"How Messages Received  (total mentions="
                         f"{int(total_msg)})", fontsize=11)
            ax.set_xlabel("Count")
            ax.invert_yaxis()
            style_ax(ax)

            # --- Broadcast channels ---
            ax = axes[1]
            total_bc = sum(bcast_vals)
            ax.barh(_broadcast_labels, bcast_vals, color=ACCENT,
                    edgecolor="white")
            for i, v in enumerate(bcast_vals):
                if v > 0:
                    pct = f" ({v/total_bc*100:.0f}%)" if total_bc > 0 else ""
                    ax.text(v + max(bcast_vals) * 0.01, i,
                            f"{int(v)}{pct}", va="center", fontsize=9)
            ax.set_title(f"Broadcast Channels Followed  (total mentions="
                         f"{int(total_bc)})", fontsize=11)
            ax.set_xlabel("Count")
            ax.invert_yaxis()
            style_ax(ax)

            save_fig(pdf, fig)

    # ================================================================
    # PAGE 72 – FIX LOCATION FEATURE + OS DISTRIBUTION
    # ================================================================
    # 4-panel: Fix Location feature adoption and satisfaction, OS distribution
    # (Android vs iOS), and GPS problem awareness.
    # Business value: OS distribution affects app development priorities; Fix Location
    # adoption shows whether drivers use GPS correction tools.
    if not HAVE_SHORT:
        placeholder_page(pdf, "Page 72 – Fix Location & OS Distribution",
                         "short DataFrame not available")
    else:
        fig, axes = plt.subplots(2, 2, figsize=(13, 9), facecolor=BG_COLOR)
        fig.suptitle("Fix Location Feature & OS Distribution",
                     fontsize=15, fontweight="bold", y=0.97)

        # --- Top-left: Fix Location funnel ---
        ax = axes[0, 0]
        fl_funnel_cols = ["fixlocation_familiar", "fixlocation_use"]
        fl_funnel_labels = ["Familiar", "Used"]
        fl_vals = []
        for fc in fl_funnel_cols:
            if fc in short.columns:
                vc = short[fc].dropna().value_counts()
                fl_vals.append(vc.get("Yes", 0))
            else:
                fl_vals.append(0)
        fl_total = (short[fl_funnel_cols[0]].dropna().shape[0]
                    if fl_funnel_cols[0] in short.columns else 0)
        fl_colors = [SNAPP_COLOR, "#66BB6A"]
        bars = ax.bar(fl_funnel_labels, fl_vals, color=fl_colors,
                      edgecolor="white", width=0.5)
        for b, v in zip(bars, fl_vals):
            pct = f" ({v/fl_total*100:.0f}%)" if fl_total > 0 else ""
            ax.text(b.get_x() + b.get_width() / 2,
                    v + max(fl_vals) * 0.02 if max(fl_vals) > 0 else 1,
                    f"{v:,}{pct}", ha="center", fontsize=9, fontweight="bold")
        ax.set_title(f"Fix Location Adoption  (n={fl_total:,})", fontsize=11)
        ax.set_ylabel("Count (Yes)")
        style_ax(ax)

        # --- Top-right: Fix Location satisfaction ---
        ax = axes[0, 1]
        fl_sat_col = "fixlocation_satisfaction"
        if fl_sat_col in short.columns:
            fl_sat = pd.to_numeric(short[fl_sat_col],
                                   errors="coerce").dropna()
            if len(fl_sat) > 0:
                fl_counts = fl_sat.value_counts().sort_index()
                fl_sat_total = fl_counts.sum()
                ax.bar(fl_counts.index.astype(int).astype(str),
                       fl_counts.values, color=SNAPP_COLOR,
                       edgecolor="white")
                for x, y in zip(fl_counts.index.astype(int).astype(str),
                                fl_counts.values):
                    ax.text(x, y + fl_sat_total * 0.005,
                            f"{y}\n({y/fl_sat_total*100:.0f}%)",
                            ha="center", fontsize=8)
                ax.set_title(f"Fix Location Satisfaction  "
                             f"(n={int(fl_sat_total)}, "
                             f"mean={fl_sat.mean():.2f})", fontsize=10)
            else:
                ax.set_title("Fix Location Satisfaction (no data)",
                             fontsize=11)
        else:
            ax.set_title("fixlocation_satisfaction not found", fontsize=11)
        ax.set_xlabel("Rating (1\u20135)")
        ax.set_ylabel("Count")
        style_ax(ax)

        # --- Bottom-left: OS pie chart ---
        ax = axes[1, 0]
        os_col = "OS"
        if os_col in short.columns:
            os_data = short[os_col].dropna().value_counts()
            if len(os_data) > 0:
                os_colors = ["#66BB6A", ACCENT, LGREY, GREY, ACCENT2]
                ax.pie(os_data.values, labels=os_data.index,
                       autopct="%1.1f%%",
                       colors=os_colors[:len(os_data)],
                       startangle=90,
                       wedgeprops={"edgecolor": "white"},
                       textprops={"fontsize": 9})
                ax.set_title(f"OS Distribution  (n={os_data.sum()})",
                             fontsize=11)
            else:
                ax.set_title("OS (no data)", fontsize=11)
        else:
            ax.set_title("OS column not found", fontsize=11)

        # --- Bottom-right: GPS problem awareness ---
        ax = axes[1, 1]
        gps_col = "gps_problem"
        if gps_col in short.columns:
            gps_data = short[gps_col].dropna().value_counts()
            if len(gps_data) > 0:
                total_gps = gps_data.sum()
                gps_colors = {"No": "#66BB6A",
                              "Yes - sometimes": "#FFA726",
                              "Yes - often": "#EF5350"}
                bar_c = [gps_colors.get(k, GREY) for k in gps_data.index]
                ax.barh(gps_data.index, gps_data.values, color=bar_c,
                        edgecolor="white")
                for i, (k, v) in enumerate(gps_data.items()):
                    ax.text(v + total_gps * 0.01, i,
                            f"{v} ({v/total_gps*100:.0f}%)",
                            va="center", fontsize=9)
                ax.set_title(f"GPS Problem Awareness  (n={total_gps})",
                             fontsize=11)
                ax.set_xlabel("Count")
            else:
                ax.set_title("GPS Problem (no data)", fontsize=11)
        else:
            ax.text(0.5, 0.5, "gps_problem column\nnot found",
                    ha="center", va="center", fontsize=12, color=GREY,
                    transform=ax.transAxes)
            ax.set_title("GPS Problem (N/A)", fontsize=11)
            ax.axis("off")
        style_ax(ax)

        save_fig(pdf, fig)

    # ================================================================
    # PAGE 73 – INCENTIVE RULES AWARENESS + PREFERENCE
    # ================================================================
    # Do drivers understand incentive rules?  Which incentive type do they prefer?
    # Business value: low rules awareness = communication gap; preference data
    # guides incentive program design.
    if not HAVE_SHORT:
        placeholder_page(pdf, "Page 73 – Incentive Rules & Preference",
                         "short DataFrame not available")
    else:
        ir_col = "incentive_rules"
        ip_col = "incentive_preference"
        have_ir = ir_col in short.columns
        have_ip = ip_col in short.columns
        if not have_ir and not have_ip:
            placeholder_page(pdf, "Page 73 – Incentive Rules & Preference",
                             "Neither incentive_rules nor "
                             "incentive_preference found in short")
        else:
            fig, axes = plt.subplots(1, 2, figsize=(14, 6),
                                     facecolor=BG_COLOR)
            fig.suptitle("Incentive Rules Awareness & Preference",
                         fontsize=15, fontweight="bold", y=0.99)

            # --- Rules awareness ---
            ax = axes[0]
            if have_ir:
                ir_data = short[ir_col].dropna().value_counts()
                if len(ir_data) > 0:
                    total_ir = ir_data.sum()
                    ax.barh(ir_data.index, ir_data.values,
                            color=ACCENT, edgecolor="white")
                    for i, (k, v) in enumerate(ir_data.items()):
                        ax.text(v + total_ir * 0.01, i,
                                f"{v} ({v/total_ir*100:.0f}%)",
                                va="center", fontsize=9)
                    ax.set_title(f"Incentive Rules Awareness  "
                                 f"(n={total_ir})", fontsize=11)
                else:
                    ax.set_title("Incentive Rules (no data)", fontsize=11)
            else:
                ax.set_title("incentive_rules not found", fontsize=11)
            ax.set_xlabel("Count")
            style_ax(ax)

            # --- Incentive preference ---
            ax = axes[1]
            if have_ip:
                ip_data = short[ip_col].dropna().value_counts()
                if len(ip_data) > 0:
                    total_ip = ip_data.sum()
                    ip_colors = [SNAPP_COLOR, TAPSI_COLOR, ACCENT,
                                 ACCENT2, GREY, LGREY]
                    ax.barh(ip_data.index, ip_data.values,
                            color=ip_colors[:len(ip_data)],
                            edgecolor="white")
                    for i, (k, v) in enumerate(ip_data.items()):
                        ax.text(v + total_ip * 0.01, i,
                                f"{v} ({v/total_ip*100:.0f}%)",
                                va="center", fontsize=9)
                    ax.set_title(f"Incentive Preference  (n={total_ip})",
                                 fontsize=11)
                else:
                    ax.set_title("Incentive Preference (no data)",
                                 fontsize=11)
            else:
                ax.set_title("incentive_preference not found", fontsize=11)
            ax.set_xlabel("Count")
            style_ax(ax)

            save_fig(pdf, fig)

    # ================================================================
    # PAGE 74 – NEXT-WEEK USAGE INTENT + RATE PASSENGER FEATURE
    # ================================================================
    # Will drivers use Snapp next week (retention intent), and are they familiar
    # with the rate-passenger feature?
    # Business value: next-week intent is a leading indicator of churn; rate-passenger
    # feature awareness affects two-sided marketplace quality.
    if not HAVE_SHORT:
        placeholder_page(pdf, "Page 74 – Next-Week Intent & Rate Passenger",
                         "short DataFrame not available")
    else:
        nw_col = "snapp_use_nextweek"
        rp_col = "ratepassenger_familiar_use"
        have_nw = nw_col in short.columns
        have_rp = rp_col in short.columns
        if not have_nw and not have_rp:
            placeholder_page(pdf,
                             "Page 74 – Next-Week Intent & Rate Passenger",
                             "Neither snapp_use_nextweek nor "
                             "ratepassenger_familiar_use found in short")
        else:
            fig, axes = plt.subplots(1, 2, figsize=(14, 6),
                                     facecolor=BG_COLOR)
            fig.suptitle("Next-Week Usage Intent & Rate Passenger Feature",
                         fontsize=15, fontweight="bold", y=0.99)

            # --- Next-week intent ---
            ax = axes[0]
            if have_nw:
                nw_data = short[nw_col].dropna().value_counts()
                if len(nw_data) > 0:
                    total_nw = nw_data.sum()
                    nw_colors_map = {"Yes": SNAPP_COLOR, "No": "#EF5350"}
                    nw_bar_c = [nw_colors_map.get(str(k), ACCENT)
                                for k in nw_data.index]
                    ax.bar(nw_data.index.astype(str), nw_data.values,
                           color=nw_bar_c, edgecolor="white")
                    for i, (k, v) in enumerate(nw_data.items()):
                        ax.text(i, v + total_nw * 0.005,
                                f"{v}\n({v/total_nw*100:.0f}%)",
                                ha="center", fontsize=9)
                    ax.set_title(f"Snapp Next-Week Intent  (n={total_nw})",
                                 fontsize=11)
                else:
                    ax.set_title("Next-Week Intent (no data)", fontsize=11)
            else:
                ax.set_title("snapp_use_nextweek not found", fontsize=11)
            ax.set_xlabel("Response")
            ax.set_ylabel("Count")
            style_ax(ax)

            # --- Rate passenger feature ---
            ax = axes[1]
            if have_rp:
                rp_data = short[rp_col].dropna().value_counts()
                if len(rp_data) > 0:
                    total_rp = rp_data.sum()
                    rp_colors = [ACCENT, SNAPP_COLOR, LGREY, GREY,
                                 ACCENT2, TAPSI_COLOR]
                    ax.bar(range(len(rp_data)), rp_data.values,
                           color=rp_colors[:len(rp_data)],
                           edgecolor="white")
                    ax.set_xticks(range(len(rp_data)))
                    ax.set_xticklabels(rp_data.index, fontsize=8,
                                       rotation=25, ha="right")
                    for i, (k, v) in enumerate(rp_data.items()):
                        ax.text(i, v + total_rp * 0.005,
                                f"{v}\n({v/total_rp*100:.0f}%)",
                                ha="center", fontsize=9)
                    ax.set_title(f"Rate Passenger Feature  (n={total_rp})",
                                 fontsize=11)
                else:
                    ax.set_title("Rate Passenger (no data)", fontsize=11)
            else:
                ax.set_title("ratepassenger_familiar_use not found",
                             fontsize=11)
            ax.set_xlabel("Response")
            ax.set_ylabel("Count")
            style_ax(ax)

            save_fig(pdf, fig)

print(f"\nReport saved to {OUTPUT_PDF}")
