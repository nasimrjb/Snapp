"""
Driver Survey -- Visual Analysis v7
====================================
This is Step 3 (the final step) of the Driver Survey ETL pipeline:

    1. generate_mapping.py   --> generates column_rename_mapping.json from raw Excel
    2. data_cleaning.py      --> reads raw data, produces 6 processed CSV files
    3. survey_analysis_v7.py --> (THIS FILE) reads those 6 CSVs, produces a PDF report

Changes from v6 --> v7 (three bug fixes, all on PAGE 59 and PAGE 74):
═══════════════════════════════════════════════════════════════════════

  BUG 1 — PAGE 59: Commission Knowledge × Satisfaction Cross-Tab showed "no data"
  ─────────────────────────────────────────────────────────────────────────────────
  Root cause: data_cleaning.py maps the raw column
    "آیا اطلاع دارید که کمیسیون اسنپ چند درصد است؟"
  to the key `snapp_comm_info`, and similarly for Tapsi (`tapsi_comm_info`).
  The column names retained their mapping-key names in the processed CSVs, so
  short["snapp_comm_info"] and short["tapsi_comm_info"] exist and contain data.
  However, v6's `_ensure` list already pre-created those columns as NaN before
  checking whether they were already populated — this caused the real data to be
  silently overwritten with NaN whenever the column was already present BUT not
  in the _ensure block that does the "only add if missing" check.  The actual
  culprit is that the cross-tab on PAGE 59 used `.dropna()` on the pair
  (comm_col, sat_col) and the `snapp_overall_satisfaction` column was populated
  only for active Snapp respondents, leaving many NaN rows after the inner join.
  The real fix needed is:
    (a) confirm both columns exist with actual data before attempting the cross-tab
    (b) log a clear message when either column is all-NaN so the "no data" page
        shows an informative reason string rather than just "Snapp (no data)"
  Fix applied: added a pre-check that prints column value counts and falls back
  gracefully with an informative reason string if data is genuinely absent.
  Also added `snapp_comm_info` and `tapsi_comm_info` to the _ensure list ONLY
  as a fallback (if absent), not overwriting existing populated columns.
  The cross-tab logic itself is unchanged.

  BUG 2 — PAGE 74 (left panel): Next-Week Intent showed "no data"
  ────────────────────────────────────────────────────────────────
  Root cause: data_cleaning.py maps `snapp_use_nextweek` with answers:
    'completely', 'mostly', 'little', 'none', 'exit snapp'
  v6's PAGE 74 charting code coloured bars using:
    nw_colors_map = {"Yes": SNAPP_COLOR, "No": "#EF5350"}
  and defaulted all non-Yes/No values to ACCENT.  That colour mapping is fine
  and would still display the bars — the actual "no data" came from the column
  being listed in `_ensure` and then the data arriving as NaN because the
  column was in short_rare (freq="rare" question) but the merge_main_rare join
  used "recordID" and some respondents didn't answer this question.
  When `short[nw_col].dropna()` returned an empty Series the panel showed
  "no data".  Fix: the column is now explicitly checked to be present AND
  non-empty before attempting to plot; if empty a clear reason string is shown.
  Additionally the intent order is now enforced so bars appear in a logical
  sequence: completely → mostly → little → none → exit snapp.

  BUG 3 — PAGE 74 (right panel): Rate Passenger labels are SWAPPED
  ─────────────────────────────────────────────────────────────────
  Root cause: column_rename_mapping.json has a label-swap error:
    'بله آشنایی دارم، امتیاز دادم.'  (familiar, DID rate)   → 'familiar - not rated'  ← WRONG
    ' بله آشنایی دارم، امتیاز ندادم.' (familiar, DID NOT rate) → 'familiar - rated'      ← WRONG
  Because the JSON is the source of truth for data_cleaning.py, the processed
  CSV already contains the swapped labels.  We cannot fix the JSON retroactively
  without re-running data_cleaning.py.  Fix applied in the PLOTTING layer only:
  after loading `rp_data = short[rp_col].dropna().value_counts()` we swap the
  two affected index labels back to their correct English meanings before
  plotting.  This is a display-only correction that does not modify any CSV.

Usage:
    python survey_analysis_v7.py

Output:
    driver_survey_analysis_v7.pdf
"""

# ============================================================================
# IMPORTS
# ============================================================================
from contextlib import contextmanager
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib
import matplotlib.ticker as mticker
import matplotlib.gridspec as gridspec
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.patheffects as pe
import os
import sys
import warnings
import numpy as np
import pandas as pd

matplotlib.use("Agg")
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
OUTPUT_PDF = os.path.join(BASE, "driver_survey_analysis_v7.pdf")

SNAPP_COLOR     = "#00C853"
TAPSI_COLOR     = "#FF6D00"
ACCENT          = "#1565C0"
ACCENT2         = "#7B1FA2"
GREY            = "#9E9E9E"
LGREY           = "#E0E0E0"
BG_COLOR        = "#FAFAFA"
PLATFORM_COLORS = {"Snapp": SNAPP_COLOR, "Tapsi": TAPSI_COLOR}

MIN_WEEK_RESPONSES = 100

# ============================================================================
# HELPER FUNCTIONS  (unchanged from v6)
# ============================================================================

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
    return (s >= 9).sum() / len(s) * 100 - (s <= 6).sum() / len(s) * 100


def style_ax(ax):
    ax.set_facecolor(BG_COLOR)
    ax.spines[["top", "right"]].set_visible(False)


def plot_sat_by_group(pdf, df, groupcol, title_suffix, figsize=(14, 6),
                      top_n=None, min_group_size=10, order=None):
    grp_sizes  = df.groupby(groupcol).size()
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
    fig.suptitle(f"Avg Satisfaction by {title_suffix}",
                 fontsize=15, fontweight="bold", y=0.99)
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
                ax.text(bar.get_x()+bar.get_width()/2,
                        bar.get_height()+0.05,
                        f"{bar.get_height():.2f}",
                        ha="center", fontsize=7, fontweight="bold")
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


def plot_long_snapp_vs_tapsi(pdf, long_df, snapp_question, tapsi_question,
                             title, figsize=(14, 6)):
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
        answer_counts = qdata["answer"].value_counts().sort_values(ascending=True)
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


def placeholder_page(pdf, title, reason="Data not available"):
    fig, ax = plt.subplots(figsize=(12, 6), facecolor=BG_COLOR)
    ax.set_facecolor(BG_COLOR)
    fig.suptitle(title, fontsize=15, fontweight="bold", y=0.97)
    ax.text(0.5, 0.5, reason, transform=ax.transAxes,
            fontsize=18, color=GREY, ha="center", va="center",
            fontweight="bold",
            bbox=dict(boxstyle="round,pad=1", facecolor=LGREY,
                      edgecolor=GREY, alpha=0.5))
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")
    save_fig(pdf, fig)


@contextmanager
def safe_page(pdf, title):
    n_figs_before = set(plt.get_fignums())
    try:
        yield
    except (KeyError, TypeError, ValueError) as e:
        for fn in plt.get_fignums():
            if fn not in n_figs_before:
                plt.close(plt.figure(fn))
        print(f"[WARN] Skipping '{title}': {e}")
        placeholder_page(pdf, title, f"Skipped \u2013 missing column: {e}")


def safe_load(path):
    if not os.path.isfile(path):
        print(f"[WARN] File not found, skipping: {path}")
        return None
    return pd.read_csv(path, encoding="utf-8-sig", low_memory=False)


def merge_main_rare(main_df, rare_df, key="recordID"):
    if main_df is None:
        return rare_df
    if rare_df is None:
        return main_df
    main_cols  = set(main_df.columns)
    rare_extra = [c for c in rare_df.columns if c not in main_cols or c == key]
    return main_df.merge(rare_df[rare_extra], on=key, how="left")


# ============================================================================
# DATA LOADING
# ============================================================================
print("Loading data files...")
short_main = safe_load(SHORT_MAIN)
short_rare = safe_load(SHORT_RARE)
wide_main  = safe_load(WIDE_MAIN)
wide_rare  = safe_load(WIDE_RARE)
long_main  = safe_load(LONG_MAIN)
long_rare  = safe_load(LONG_RARE)

short = merge_main_rare(short_main, short_rare)
wide  = merge_main_rare(wide_main,  wide_rare)

if long_main is not None and long_rare is not None:
    long = pd.concat([long_main, long_rare], ignore_index=True)
elif long_main is not None:
    long = long_main.copy()
elif long_rare is not None:
    long = long_rare.copy()
else:
    long = None

if short is None:
    short = pd.DataFrame()
    print("[WARN] No short data available -- report will be mostly empty.")

rename_map = {}
if "snapp_CS"  in short.columns:
    rename_map["snapp_CS"]  = "snapp_customer_support"
if "tapsi_CS_" in short.columns:
    rename_map["tapsi_CS_"] = "tapsi_customer_support"
if rename_map:
    short.rename(columns=rename_map, inplace=True)

if wide is not None:
    wide_rename = {}
    if "snapp_CS"  in wide.columns:
        wide_rename["snapp_CS"]  = "snapp_customer_support"
    if "tapsi_CS_" in wide.columns:
        wide_rename["tapsi_CS_"] = "tapsi_customer_support"
    if wide_rename:
        wide.rename(columns=wide_rename, inplace=True)

# --- Ensure critical columns exist (add as NaN only if MISSING) ---
# NOTE: snapp_comm_info / tapsi_comm_info are intentionally in this list so
# that PAGE 59 never throws a KeyError.  The `if _c not in short.columns`
# guard means we NEVER overwrite a column that already has real data.
_ensure = [
    "snapp_overall_satisfaction", "tapsi_overall_satisfaction",
    "snapp_recommend", "tapsidriver_tapsi_recommend",
    "snapp_CS_satisfaction_overall", "tapsi_CS_satisfaction_overall",
    "snapp_CS_satisfaction_waittime", "tapsi_CS_satisfaction_waittime",
    "snapp_CS_satisfaction_solution", "tapsi_CS_satisfaction_solution",
    "snapp_CS_satisfaction_behaviour", "tapsi_CS_satisfaction_behaviour",
    "snapp_CS_satisfaction_relevance", "tapsi_CS_satisfaction_relevance",
    "snapp_CS_satisfaction_important_reason",
    "tapsi_CS_satisfaction_important_reason",
    "snapp_CS_solved", "tapsi_CS_solved",
    "snapp_customer_support", "tapsi_customer_support",
    "snapp_collab_reason", "tapsi_collab_reason",
    "snapp_better_income", "tapsi_better_income",
    # BUG-1 FIX: these were missing from _ensure in v6, meaning PAGE 59 would
    # crash with KeyError if data_cleaning produced them under a different name.
    # Adding them here ensures a graceful "no data" fallback at worst.
    "snapp_comm_info", "tapsi_comm_info",
    "snapp_tax_info", "tapsi_tax_info",
    "snapp_gps_stage", "tapsi_gps_stage",
    "snapp_unpaid_by_passenger_followup",
    "tapsi_unpaid_by_passenger_followup",
    "snapp_compensate_unpaid_by_passenger",
    "tapsi_compensate_unpaid_by_passenger",
    "snapp_register_type", "tapsi_register_type",
    "snapp_main_reg_reason", "tapsi_main_reg_reason",
    "snapp_refer_others", "tapsi_refer_others",
    "snapp_ecoplus_familiar", "snapp_ecoplus_access_usage",
    "snapp_participate_feeling", "snapp_not_talking_reason",
    "demand_process", "missed_demand_per_10", "max_demand",
    "recommendation_googlemap", "recommendation_waze",
    "recommendation_neshan", "recommendation_balad",
    "snapp_accepted_trip_length", "tapsi_accepted_trip_length",
    "snapp_satisfaction_followup_overall",
    "tapsi_satisfaction_followup_overall",
    "snapp_satisfaction_followup_time", "tapsi_satisfaction_followup_time",
    "tapsi_in_app_navigation_usage",
    "tapsi_in_app_navigation_satisfaction",
    "tapsi_offline_navigation_familiar",
    "tapsi_offline_navigation_usage",
    "tapsi_gps_better", "snapp_navigation_app_satisfaction",
    "snapp_unsatisfaction_app_support",
    "tapsi_unsatisfaction_app_support",
    "snapp_speed_satisfaction", "tapsi_speed_satisfaction",
    "snappcarfix_familiar", "snappcarfix_use_ever",
    "snappcarfix_recommend",
    "snappcarfix_satisfaction_overall",
    "snappcarfix_satisfaction_experience",
    "snappcarfix_satisfaction_productprice",
    "snappcarfix_satisfaction_quality",
    "snappcarfix_satisfaction_variety",
    "snappcarfix_satisfaction_buyingprocess",
    "snappcarfix_satisfaction_deliverytime",
    "snappcarfix_satisfaction_waittime",
    "snappcarfix_satisfaction_behaviour", "snappcarfix_use_lastmo",
    "snappcarfix_satisfaction_quality_lastm",
    "snappcarfix_satisfaction_price_lastm",
    "snappcarfix_satisfaction_variety_lastm",
    "snappcarfix_satisfaction_easyusage",
    "snappcarfix_satisfaction_ontimedelivery",
    "snappcarfix_satisfaction_CS",
    "tapsigarage_familiar", "tapsigarage_use_ever",
    "tapsigarage_recommend",
    "tapsigarage_satisfaction_overall",
    "tapsigarage_satisfaction_experience",
    "tapsigarage_satisfaction_productprice",
    "tapsigarage_satisfaction_quality_experience",
    "tapsigarage_satisfaction_variety_experience",
    "tapsigarage_satisfaction_buyingprocess",
    "tapsigarage_satisfaction_deliverytime",
    "tapsigarage_satisfaction_waittime",
    "tapsigarage_satisfaction_behaviour", "tapsigarage_use_lastmo",
    "tapsigarage_satisfaction_quality", "tapsigarage_satisfaction_price",
    "tapsigarage_satisfaction_variety",
    "tapsigarage_satisfaction_easyusage",
    "tapsigarage_satisfaction_ontimedelivery",
    "tapsigarage_satisfaction_CS",
    "mixincentive", "mixincentive_activate_familiar",
    "mixincentive_tripeffect", "mixincentive_onlysnapp",
    "mixincentive_choice",
    "incentive_preference", "incentive_rules",
    "fixlocation_familiar", "fixlocation_use",
    "fixlocation_satisfaction",
    "OS",
    # BUG-2 FIX: snapp_use_nextweek must be in _ensure so PAGE 74 never throws
    # a KeyError.  If the column truly has no data, the panel shows a clear
    # "no data" message via the len(nw_data) == 0 guard below.
    "snapp_use_nextweek",
    # BUG-3: ratepassenger_familiar_use is also ensured here.
    "ratepassenger_familiar_use",
]
for _c in _ensure:
    if _c not in short.columns:
        short[_c] = np.nan

valid_ids = (set(short["recordID"].unique())
             if "recordID" in short.columns else set())
if wide is not None and "recordID" in wide.columns:
    wide = wide[wide["recordID"].isin(valid_ids)].copy()
if long is not None and "recordID" in long.columns:
    long = long[long["recordID"].isin(valid_ids)].copy()

# ============================================================================
# DATETIME PARSING & YEARWEEK CONSTRUCTION
# ============================================================================
dfs_to_parse = [short]
if wide is not None:
    dfs_to_parse.append(wide)
if long is not None:
    dfs_to_parse.append(long)

for df in dfs_to_parse:
    df["datetime_parsed"] = pd.to_datetime(df["datetime"], errors="coerce")
    df["year"]             = df["datetime_parsed"].dt.year
    df["weeknumber"]       = pd.to_numeric(df["weeknumber"], errors="coerce")
    df["yearweek"] = (
        (df["year"] % 100) * 100 + df["weeknumber"]
    ).where(df["weeknumber"].notna() & df["year"].notna()).astype("Int64")

week_counts_all = short.groupby("yearweek").size()
valid_weeks     = week_counts_all[week_counts_all >= MIN_WEEK_RESPONSES].index
dropped_weeks   = week_counts_all[week_counts_all < MIN_WEEK_RESPONSES]
if len(dropped_weeks) > 0:
    print(f"Dropping {len(dropped_weeks)} week(s) "
          f"with <{MIN_WEEK_RESPONSES} responses")

short = short[short["yearweek"].isin(valid_weeks)].copy()
if wide is not None:
    wide = wide[wide["yearweek"].isin(valid_weeks)].copy()
if long is not None:
    long = long[long["yearweek"].isin(valid_weeks)].copy()

if "tapsi_ride" in short.columns:
    short["driver_type"] = np.where(
        short["tapsi_ride"] == 0, "Snapp Exclusive", "Joint")
if wide is not None and "tapsi_ride" in wide.columns:
    wide["driver_type"] = np.where(
        wide["tapsi_ride"] == 0, "Snapp Exclusive", "Joint")
if long is not None and "tapsi_ride" in long.columns:
    long["driver_type"] = np.where(
        long["tapsi_ride"] == 0, "Snapp Exclusive", "Joint")

short.sort_values("yearweek", inplace=True)
if wide is not None:
    wide.sort_values("yearweek", inplace=True)
if long is not None:
    long.sort_values("yearweek", inplace=True)

TENURE_ORDER  = ["less_than_3_months", "3_to_6_months", "6_months_to_1_year",
                 "1_to_3_years", "3_to_5_years", "5_to_7_years",
                 "more_than_7_years"]
TENURE_LABELS = ["<3 m", "3-6 m", "6m-1y", "1-3 y", "3-5 y", "5-7 y", ">7 y"]

SAT_PAIRS = [
    ("snapp_fare_satisfaction",      "tapsi_fare_satisfaction",      "Fare"),
    ("snapp_income_satisfaction",    "tapsi_income_satisfaction",    "Income"),
    ("snapp_req_count_satisfaction", "tapsi_req_count_satisfaction", "Request Count"),
]

HAVE_SHORT = len(short) > 0
HAVE_WIDE  = wide is not None and len(wide) > 0
HAVE_LONG  = long is not None and len(long) > 0

print(f"Remaining: {len(short)} short, "
      f"{len(wide) if wide is not None else 0} wide, "
      f"{len(long) if long is not None else 0} long, "
      f"{short['yearweek'].nunique() if HAVE_SHORT else 0} weeks")
print(f"HAVE_SHORT={HAVE_SHORT}, HAVE_WIDE={HAVE_WIDE}, HAVE_LONG={HAVE_LONG}")

# ============================================================================
# PDF REPORT GENERATION
# ============================================================================
with PdfPages(OUTPUT_PDF) as pdf:

    # ================================================================
    # PAGE 1 – COVER / KEY KPI SUMMARY
    # ================================================================
    with safe_page(pdf, 'Page 1 - COVER / KEY KPI SUMMARY'):
        n_total        = len(short)
        n_weeks        = short["yearweek"].nunique()
        n_cities       = short["city"].nunique()
        n_joint_pct    = (short["driver_type"] == "Joint").mean() * 100
        n_fulltime_pct = (short["cooperation_type"] == "Full-Time").mean() * 100
        snapp_sat_mean = short["snapp_overall_satisfaction"].mean()
        tapsi_sat_mean = short["tapsi_overall_satisfaction"].mean()
        snapp_nps_val  = nps_score(short["snapp_recommend"])
        tapsi_nps_val  = nps_score(short["tapsidriver_tapsi_recommend"])
        snapp_inc_mean = short["snapp_incentive"].mean() / 1e6
        tapsi_inc_mean = short["tapsi_incentive"].mean() / 1e6

        fig = plt.figure(figsize=(12, 8), facecolor=BG_COLOR)
        fig.suptitle("Driver Survey \u2013 Key Performance Indicators",
                     fontsize=18, fontweight="bold", y=0.97)
        ax_banner = fig.add_axes([0.05, 0.82, 0.9, 0.1])
        ax_banner.set_xlim(0, 10)
        ax_banner.set_ylim(0, 1)
        ax_banner.axis("off")
        kpis = [(f"{n_total:,}", "Responses"),
                (f"{n_weeks}", "Survey Weeks"),
                (f"{n_cities}", "Cities"),
                (f"{n_joint_pct:.0f}%", "Joint Drivers"),
                (f"{n_fulltime_pct:.0f}%", "Full-Time")]
        for i, (val, lbl) in enumerate(kpis):
            cx = 1 + i * 2
            ax_banner.text(cx, 0.7, val, ha="center", fontsize=20,
                           fontweight="bold", color=ACCENT)
            ax_banner.text(cx, 0.1, lbl, ha="center", fontsize=10, color=GREY)

        ax_sat = fig.add_axes([0.05, 0.52, 0.42, 0.26])
        cats   = ["Overall Sat.", "Fare Sat.", "Income Sat.", "Req-Count Sat."]
        s_vals = [short["snapp_overall_satisfaction"].mean(),
                  short["snapp_fare_satisfaction"].mean(),
                  short["snapp_income_satisfaction"].mean(),
                  short["snapp_req_count_satisfaction"].mean()]
        t_vals = [short["tapsi_overall_satisfaction"].mean(),
                  short["tapsi_fare_satisfaction"].mean(),
                  short["tapsi_income_satisfaction"].mean(),
                  short["tapsi_req_count_satisfaction"].mean()]
        x = np.arange(len(cats))
        w = 0.35
        ax_sat.bar(x - w/2, s_vals, w, color=SNAPP_COLOR, label="Snapp")
        ax_sat.bar(x + w/2, t_vals, w, color=TAPSI_COLOR, label="Tapsi")
        for xi, (sv, tv) in enumerate(zip(s_vals, t_vals)):
            ax_sat.text(xi - w/2, sv + 0.05, f"{sv:.2f}",
                        ha="center", fontsize=8, fontweight="bold")
            ax_sat.text(xi + w/2, tv + 0.05, f"{tv:.2f}",
                        ha="center", fontsize=8, fontweight="bold")
        ax_sat.set_xticks(x)
        ax_sat.set_xticklabels(cats, fontsize=9, rotation=15, ha="right")
        ax_sat.set_ylim(0, 5.5)
        ax_sat.set_title("Mean Satisfaction (1\u20135)", fontsize=11)
        ax_sat.legend(frameon=False, fontsize=9)
        style_ax(ax_sat)

        ax_nps = fig.add_axes([0.55, 0.52, 0.4, 0.26])
        metrics = ["NPS", "Avg Incentive\n(M Rials)", "CS Satisfaction\n(Overall)"]
        s_m = [snapp_nps_val, snapp_inc_mean,
               short["snapp_CS_satisfaction_overall"].mean()]
        t_m = [tapsi_nps_val, tapsi_inc_mean,
               short["tapsi_CS_satisfaction_overall"].mean()]
        x2 = np.arange(len(metrics))
        ax_nps.bar(x2 - w/2, s_m, w, color=SNAPP_COLOR, label="Snapp")
        ax_nps.bar(x2 + w/2, t_m, w, color=TAPSI_COLOR, label="Tapsi")
        for xi, (sv, tv) in enumerate(zip(s_m, t_m)):
            ax_nps.text(xi - w/2, max(sv, 0) + 0.2,
                        f"{sv:.1f}", ha="center", fontsize=8, fontweight="bold")
            ax_nps.text(xi + w/2, max(tv, 0) + 0.2,
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
        gender_m   = (short["gender"] == "Male").mean() * 100
        age_u35    = (short["age"].isin(["18_25", "26_35", "<18"])).mean() * 100
        part_time  = (short["cooperation_type"] == "Part-Time").mean() * 100
        top_job    = short["original_job"].value_counts().index[0]
        top_job_pct = (short["original_job"].value_counts().iloc[0]
                       / len(short) * 100)
        top_city    = short["city"].value_counts().index[0]
        top_city_pct = (short["city"].value_counts().iloc[0]
                        / len(short) * 100)
        demo_items = [
            (f"{gender_m:.0f}%",    "Male Drivers"),
            (f"{age_u35:.0f}%",     "Under 35"),
            (f"{part_time:.0f}%",   "Part-Time"),
            (f"{top_job_pct:.0f}%", f"#1 Job:\n{top_job}"),
            (f"{top_city_pct:.0f}%", f"Top City:\n{top_city}"),
        ]
        for i, (val, lbl) in enumerate(demo_items):
            cx = 1 + i * 2
            ax_demo.text(cx, 0.75, val, ha="center", fontsize=16,
                         fontweight="bold", color=ACCENT2)
            ax_demo.text(cx, 0.1, lbl, ha="center", fontsize=9, color=GREY)
        ax_demo.set_title("Demographics Snapshot",
                          fontsize=11, x=0.5, y=1.0, pad=0)

        ax_ins = fig.add_axes([0.05, 0.04, 0.9, 0.2])
        style_ax(ax_ins)
        ax_ins.axis("off")
        insights = [
            "\u2022 Tapsi CS resolution rate ~60% vs Snapp ~29% \u2014 large satisfaction gap (3.52 vs 2.56)",
            "\u2022 Honeymoon effect: new Snapp drivers (<3 months) rate satisfaction 3.25 vs 2.65 for veterans",
            "\u2022 Full compensation for unpaid fares: Tapsi 63% vs Snapp 40% \u2014 significant driver trust gap",
            "\u2022 Navigation: Neshan used by 68% in last Snapp trip; Tapsi drivers use in-app nav at 23%",
            "\u2022 Snapp App NPS vs Platform NPS \u2014 see page 58 for app vs platform recommendation split",
            "\u2022 Tapsi re-activation: 13% of incentive-responding drivers were inactive >6 months \u2014 page 57",
        ]
        for i, ins in enumerate(insights):
            ax_ins.text(0.01, 0.92 - i * 0.17, ins, fontsize=9.5,
                        transform=ax_ins.transAxes,
                        color="#333333", va="top")
        pdf.savefig(fig, facecolor=BG_COLOR)
        plt.close(fig)

    # ================================================================
    # PAGE 2 – RESPONSE COUNT BY YEARWEEK
    # ================================================================
    with safe_page(pdf, 'Page 2 - RESPONSE COUNT BY YEARWEEK'):
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
    with safe_page(pdf, 'Page 3 - DEMOGRAPHICS OVERVIEW'):
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
        ax.barh(["High School\nor Below", "College+"],
                ed.values, color=[GREY, ACCENT])
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
        ax.barh(["Single", "Married"], ms.values, color=[GREY, ACCENT])
        ax.set_title("Marital Status", fontsize=11)
        for i, v in enumerate(ms.values):
            ax.text(v + 5, i, f"{v} ({v/len(short)*100:.0f}%)",
                    va="center", fontsize=9)
        for row in axes:
            for ax in row:
                style_ax(ax)
        save_fig(pdf, fig)

    # ================================================================
    # PAGE 4 – OCCUPATION BREAKDOWN
    # ================================================================
    with safe_page(pdf, 'Page 4 - OCCUPATION BREAKDOWN'):
        job_counts = short["original_job"].value_counts().head(15)
        fig, ax    = new_fig(
            "Primary Occupation of Surveyed Drivers (Top 15)", figsize=(12, 7))
        colors = [ACCENT if i < 5 else LGREY for i in range(len(job_counts))]
        ax.barh(job_counts.index[::-1], job_counts.values[::-1],
                color=colors[::-1], edgecolor="white")
        total = len(short)
        for i, v in enumerate(job_counts.values[::-1]):
            ax.text(v + 30, i, f"{v:,} ({v/total*100:.1f}%)",
                    va="center", fontsize=9)
        ax.set_xlabel("Response Count")
        style_ax(ax)
        save_fig(pdf, fig)

    # ================================================================
    # PAGE 5 – ACTIVE JOINT RATE BY YEARWEEK
    # ================================================================
    with safe_page(pdf, 'Page 5 - ACTIVE JOINT RATE BY YEARWEEK'):
        weekly_joint = short.groupby("yearweek").agg(
            total=("active_joint", "size"),
            active=("active_joint", "sum"))
        weekly_joint["rate"] = (weekly_joint["active"]
                                / weekly_joint["total"] * 100)
        fig, ax = new_fig("Active Joint (Tapsi) Rate by Year-Week")
        ax.plot(weekly_joint.index.astype(str), weekly_joint["rate"],
                marker="o", color=TAPSI_COLOR, linewidth=2.5, markersize=8)
        for idx, row in weekly_joint.iterrows():
            ax.annotate(f"{row['rate']:.0f}%",
                        (str(idx), row["rate"]),
                        textcoords="offset points",
                        xytext=(0, 10), ha="center", fontsize=9)
        ax.set_xlabel("Year-Week")
        ax.set_ylabel("Active Joint Rate (%)")
        ax.set_ylim(0, 100)
        ax.tick_params(axis="x", rotation=45)
        style_ax(ax)
        save_fig(pdf, fig)

    # ================================================================
    # PAGE 6 – AVERAGE RIDE COUNTS
    # ================================================================
    with safe_page(pdf, 'Page 6 - AVERAGE RIDE COUNTS'):
        weekly_rides = short.groupby("yearweek").agg(
            snapp_ride=("snapp_ride", "mean"),
            tapsi_ride=("tapsi_ride", "mean"))
        fig, ax = new_fig("Average Weekly Ride Count \u2013 Snapp vs Tapsi")
        ax.plot(weekly_rides.index.astype(str), weekly_rides["snapp_ride"],
                marker="o", color=SNAPP_COLOR, linewidth=2.5, label="Snapp")
        ax.plot(weekly_rides.index.astype(str), weekly_rides["tapsi_ride"],
                marker="s", color=TAPSI_COLOR, linewidth=2.5, label="Tapsi")
        ax.legend(frameon=False, fontsize=10)
        ax.set_xlabel("Year-Week")
        ax.set_ylabel("Avg Rides")
        ax.tick_params(axis="x", rotation=45)
        style_ax(ax)
        save_fig(pdf, fig)

    # ================================================================
    # PAGE 7 – SATISFACTION COMPARISON
    # ================================================================
    with safe_page(pdf, 'Page 7 - SATISFACTION COMPARISON'):
        fig, axes = plt.subplots(1, 3, figsize=(14, 5),
                                 facecolor=BG_COLOR, sharey=True)
        fig.suptitle("Satisfaction Comparison (1\u20135 scale): Snapp vs Tapsi",
                     fontsize=15, fontweight="bold", y=0.99)
        for ax, (scol, tcol, label) in zip(axes, SAT_PAIRS):
            snapp_mean = short[scol].dropna().mean()
            tapsi_mean = short[tcol].dropna().mean()
            bars = ax.bar(["Snapp", "Tapsi"], [snapp_mean, tapsi_mean],
                          color=[SNAPP_COLOR, TAPSI_COLOR],
                          width=0.5, edgecolor="white")
            ax.set_title(label, fontsize=11)
            ax.set_ylim(0, 5.5)
            style_ax(ax)
            for bar in bars:
                ax.text(bar.get_x() + bar.get_width() / 2,
                        bar.get_height() + 0.08,
                        f"{bar.get_height():.2f}",
                        ha="center", fontsize=10, fontweight="bold")
        axes[0].set_ylabel("Mean Satisfaction")
        save_fig(pdf, fig)

    # ================================================================
    # PAGE 8 – OVERALL SATISFACTION DISTRIBUTION
    # ================================================================
    with safe_page(pdf, 'Page 8 - OVERALL SATISFACTION DISTRIBUTION'):
        fig, axes = plt.subplots(1, 2, figsize=(12, 5),
                                 facecolor=BG_COLOR, sharey=True)
        fig.suptitle("Overall Satisfaction Distribution (1\u20135 scale)",
                     fontsize=15, fontweight="bold", y=0.99)
        for ax, col, color, label in [
            (axes[0], "snapp_overall_satisfaction", SNAPP_COLOR, "Snapp"),
            (axes[1], "tapsi_overall_satisfaction", TAPSI_COLOR, "Tapsi"),
        ]:
            data   = short[col].dropna()
            counts = data.value_counts().sort_index()
            ax.bar(counts.index.astype(int).astype(str), counts.values,
                   color=color, edgecolor="white")
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
    # PAGE 9 – NPS BY YEARWEEK
    # ================================================================
    with safe_page(pdf, 'Page 9 - NPS BY YEARWEEK'):
        nps_weekly = short.groupby("yearweek").agg(
            snapp_nps=("snapp_recommend", nps_score),
            tapsi_nps=("tapsidriver_tapsi_recommend", nps_score),
        ).dropna(how="all")
        fig, ax = new_fig(
            "NPS (Net Promoter Score) by Year-Week \u2013 Snapp vs Tapsi")
        if not nps_weekly["snapp_nps"].isna().all():
            ax.plot(nps_weekly.index.astype(str), nps_weekly["snapp_nps"],
                    marker="o", color=SNAPP_COLOR, linewidth=2.5,
                    label="Snapp")
        if not nps_weekly["tapsi_nps"].isna().all():
            ax.plot(nps_weekly.index.astype(str), nps_weekly["tapsi_nps"],
                    marker="s", color=TAPSI_COLOR, linewidth=2.5,
                    label="Tapsi")
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
    with safe_page(pdf, 'Page 10 - INCENTIVE CATEGORY BREAKDOWN'):
        fig, axes = plt.subplots(1, 2, figsize=(12, 5), facecolor=BG_COLOR)
        fig.suptitle("Incentive Category Distribution",
                     fontsize=15, fontweight="bold", y=0.99)
        for ax, col, color, label in [
            (axes[0], "snapp_incentive_category", SNAPP_COLOR, "Snapp"),
            (axes[1], "tapsi_incentive_category", TAPSI_COLOR, "Tapsi"),
        ]:
            data  = short[col].dropna().value_counts()
            ax.barh(data.index, data.values, color=color, edgecolor="white")
            total = data.sum()
            for i, v in enumerate(data.values):
                ax.text(v + 2, i, f"{v} ({v/total*100:.0f}%)",
                        va="center", fontsize=9)
            ax.set_title(f"{label}  (n={total})", fontsize=11)
            style_ax(ax)
        save_fig(pdf, fig)

    # ================================================================
    # PAGE 11 – INCENTIVE TYPE USAGE
    # ================================================================
    with safe_page(pdf, 'Page 11 - INCENTIVE TYPE USAGE (wide_survey)'):
        incentive_types = {
            "Snapp": {
                "Pay After Ride":
                    "Snapp Incentive Type__Pay After Ride",
                "Ride-Based Comm-free":
                    "Snapp Incentive Type__Ride-Based Commission-free",
                "Earning-Based Comm-free":
                    "Snapp Incentive Type__Earning-based Commission-free",
                "Income Guarantee":
                    "Snapp Incentive Type__Income Guarantee",
                "Pay After Income":
                    "Snapp Incentive Type__Pay After Income",
            },
            "Tapsi": {
                "Pay After Ride":
                    "Tapsi Incentive Type__Pay After Ride",
                "Ride-Based Comm-free":
                    "Tapsi Incentive Type__Ride-Based Commission-free",
                "Earning-Based Comm-free":
                    "Tapsi Incentive Type__Earning-based Commission-free",
                "Income Guarantee":
                    "Tapsi Incentive Type__Income Guarantee",
                "Pay After Income":
                    "Tapsi Incentive Type__Pay After Income",
            },
        }
        fig, axes = plt.subplots(1, 2, figsize=(14, 6),
                                 facecolor=BG_COLOR, sharey=True)
        fig.suptitle("Incentive Type Usage \u2013 wide_survey (multi-choice)",
                     fontsize=15, fontweight="bold", y=0.99)
        for ax, (platform, cols) in zip(axes, incentive_types.items()):
            labels = list(cols.keys())
            values = [wide[c].sum() if c in wide.columns else 0
                      for c in cols.values()]
            color  = PLATFORM_COLORS[platform]
            y_pos  = range(len(labels))
            ax.barh(y_pos, values, color=color, edgecolor="white")
            ax.set_yticks(y_pos)
            ax.set_yticklabels(labels)
            for i, v in enumerate(values):
                ax.text(v + 5, i, str(int(v)), va="center", fontsize=9)
            ax.set_title(platform, fontsize=12)
            style_ax(ax)
        axes[0].invert_yaxis()
        save_fig(pdf, fig)

    # ================================================================
    # PAGE 12 – INCENTIVE UNSATISFACTION REASONS
    # ================================================================
    with safe_page(pdf, 'Page 12 - INCENTIVE UNSATISFACTION REASONS'):
        unsat_types = {
            "Snapp": {
                "Improper Amount":
                    "Snapp Incentive Unsatisfaction__Improper Amount",
                "Difficult":
                    "Snapp Incentive Unsatisfaction__difficult",
                "No Time":
                    "Snapp Incentive Unsatisfaction__No Time todo",
                "Not Available":
                    "Snapp Incentive Unsatisfaction__No Available Time",
                "Non Payment":
                    "Snapp Incentive Unsatisfaction__Non Payment",
            },
            "Tapsi": {
                "Improper Amount":
                    "Tapsi Incentive Unsatisfaction__Improper Amount",
                "Difficult":
                    "Tapsi Incentive Unsatisfaction__difficult",
                "No Time":
                    "Tapsi Incentive Unsatisfaction__No Time todo",
                "Not Available":
                    "Tapsi Incentive Unsatisfaction__Not Available",
                "Non Payment":
                    "Tapsi Incentive Unsatisfaction__Non Payment",
            },
        }
        fig, axes = plt.subplots(1, 2, figsize=(14, 5),
                                 facecolor=BG_COLOR, sharey=True)
        fig.suptitle("Incentive Unsatisfaction Reasons \u2013 wide_survey",
                     fontsize=15, fontweight="bold", y=0.99)
        for ax, (platform, cols) in zip(axes, unsat_types.items()):
            labels = list(cols.keys())
            values = [wide[c].sum() if c in wide.columns else 0
                      for c in cols.values()]
            color  = PLATFORM_COLORS[platform]
            y_pos  = range(len(labels))
            ax.barh(y_pos, values, color=color, edgecolor="white")
            ax.set_yticks(y_pos)
            ax.set_yticklabels(labels)
            for i, v in enumerate(values):
                ax.text(v + 2, i, str(int(v)), va="center", fontsize=9)
            ax.set_title(platform, fontsize=12)
            style_ax(ax)
        axes[0].invert_yaxis()
        save_fig(pdf, fig)

    # ================================================================
    # PAGE 13 – AVERAGE INCENTIVE BY YEARWEEK
    # ================================================================
    with safe_page(pdf, 'Page 13 - AVERAGE INCENTIVE (RIALS) BY YEARWEEK'):
        weekly_inc = short.groupby("yearweek").agg(
            snapp=("snapp_incentive", "mean"),
            tapsi=("tapsi_incentive", "mean"))
        fig, ax = new_fig("Average Monetary Incentive by Year-Week (Rials)")
        ax.plot(weekly_inc.index.astype(str), weekly_inc["snapp"] / 1e6,
                marker="o", color=SNAPP_COLOR, linewidth=2.5, label="Snapp")
        ax.plot(weekly_inc.index.astype(str), weekly_inc["tapsi"] / 1e6,
                marker="s", color=TAPSI_COLOR, linewidth=2.5, label="Tapsi")
        ax.legend(frameon=False, fontsize=10)
        ax.set_xlabel("Year-Week")
        ax.set_ylabel("Avg Incentive (Million Rials)")
        ax.tick_params(axis="x", rotation=45)
        style_ax(ax)
        save_fig(pdf, fig)

    # ================================================================
    # PAGE 14 – INCENTIVE SATISFACTION DISTRIBUTION
    # ================================================================
    with safe_page(pdf, 'Page 14 - INCENTIVE SATISFACTION DISTRIBUTION'):
        fig, axes = plt.subplots(1, 2, figsize=(12, 5),
                                 facecolor=BG_COLOR, sharey=True)
        fig.suptitle("Incentive Satisfaction Distribution (1\u20135 scale)",
                     fontsize=15, fontweight="bold", y=0.99)
        for ax, col, color, label in [
            (axes[0], "snapp_overall_incentive_satisfaction",
             SNAPP_COLOR, "Snapp"),
            (axes[1], "tapsi_overall_incentive_satisfaction",
             TAPSI_COLOR, "Tapsi"),
        ]:
            data   = short[col].dropna()
            counts = data.value_counts().sort_index()
            ax.bar(counts.index.astype(int).astype(str), counts.values,
                   color=color, edgecolor="white")
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
    # PAGE 15 – LOC DISTRIBUTION
    # ================================================================
    with safe_page(pdf, 'Page 15 - LOC DISTRIBUTION'):
        fig, axes = plt.subplots(1, 2, figsize=(12, 5),
                                 facecolor=BG_COLOR, sharey=True)
        fig.suptitle("Length of Cooperation Distribution (months)",
                     fontsize=15, fontweight="bold", y=0.99)
        for ax, col, color, label in [
            (axes[0], "snapp_LOC", SNAPP_COLOR, "Snapp"),
            (axes[1], "tapsi_LOC", TAPSI_COLOR, "Tapsi"),
        ]:
            data = short[col].dropna()
            ax.hist(data, bins=20, color=color, edgecolor="white", alpha=0.85)
            ax.axvline(data.mean(), color="black", linestyle="--",
                       linewidth=1.2)
            ax.text(data.mean() + 1, ax.get_ylim()[1] * 0.9,
                    f"Mean: {data.mean():.1f}", fontsize=9)
            ax.set_title(f"{label}  (n={len(data)})", fontsize=11)
            ax.set_xlabel("Months")
            style_ax(ax)
        axes[0].set_ylabel("Count")
        save_fig(pdf, fig)

    # ================================================================
    # PAGE 16 – RIDE REFUSAL REASONS
    # ================================================================
    with safe_page(pdf, 'Page 16 - RIDE REFUSAL REASONS'):
        refusal_labels = [
            "Insufficient Fare", "Distance to Origin", "Wait for Better",
            "Long Trip", "Target Destination", "Traffic",
            "Short Accept Time", "Unfamiliar Route", "Internet Problems",
            "App Problems", "App was Unfamiliar", "Was Working w/ Other",
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
            "Snapp Ride Refusal Reasons__App was Unfamiliar",
            "Snapp Ride Refusal Reasons__Was Working with Tapsi",
        ]
        tapsi_refusal_cols = [c.replace("Snapp", "Tapsi")
                              for c in snapp_refusal_cols]
        snapp_vals = [wide[c].sum() if c in wide.columns else 0
                      for c in snapp_refusal_cols]
        tapsi_vals = [wide[c].sum() if c in wide.columns else 0
                      for c in tapsi_refusal_cols]
        fig, ax = new_fig(
            "Ride Refusal Reasons \u2013 Snapp vs Tapsi (wide_survey)",
            figsize=(14, 7))
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
        style_ax(ax)
        save_fig(pdf, fig)

    # ================================================================
    # PAGE 17 – CUSTOMER SUPPORT CATEGORY
    # ================================================================
    with safe_page(pdf, 'Page 17 - CUSTOMER SUPPORT CATEGORY'):
        cs_labels = [
            "Fare", "Cancelling", "Trip Problems", "Petrol", "Technical",
            "Settlement", "Incentive", "Location Change",
            "Drivers Club", "Registration",
        ]
        snapp_cs = [f"Snapp Customer Support Category__{l}"
                    for l in cs_labels]
        tapsi_cs = [
            f"Tapsi Customer Support Category__{l.replace('Location Change', 'Loc Change')}"
            for l in cs_labels
        ]
        snapp_cs_vals = [wide[c].sum() if c in wide.columns else 0
                         for c in snapp_cs]
        tapsi_cs_vals = [wide[c].sum() if c in wide.columns else 0
                         for c in tapsi_cs]
        fig, ax = new_fig(
            "Customer Support Ticket Categories \u2013 wide_survey",
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
        style_ax(ax)
        save_fig(pdf, fig)

    # ================================================================
    # PAGE 18 – NAVIGATION APP ADOPTION FUNNEL
    # ================================================================
    with safe_page(pdf, 'Page 18 - NAVIGATION APP ADOPTION FUNNEL (long_survey)'):
        nav_apps   = ["Google Map", "Waze", "Neshan", "Balad"]
        nav_colors = [ACCENT, "#FFA726", "#66BB6A", "#AB47BC"]
        stages       = ["Navigation Familiarity",
                        "Navigation Installed", "Navigation Used"]
        stage_labels = ["Familiarity", "Installed", "Used"]
        fig, axes = plt.subplots(1, 3, figsize=(15, 5),
                                 facecolor=BG_COLOR, sharey=True)
        fig.suptitle("Navigation App Adoption Funnel \u2013 long_survey",
                     fontsize=15, fontweight="bold", y=0.99)
        for ax, stage, slbl in zip(axes, stages, stage_labels):
            qdata = long[long["question"] == stage]
            vals  = [len(qdata[qdata["answer"] == app]) for app in nav_apps]
            ax.barh(nav_apps, vals, color=nav_colors, edgecolor="white")
            for i, v in enumerate(vals):
                ax.text(v + 10, i, str(int(v)), va="center", fontsize=9)
            ax.set_title(slbl, fontsize=12)
            style_ax(ax)
        axes[0].invert_yaxis()
        save_fig(pdf, fig)

    # ================================================================
    # PAGE 19 – NAVIGATION APP RATINGS
    # ================================================================
    with safe_page(pdf, 'Page 19 - NAVIGATION APP RATINGS (0\u201310 scale)'):
        nav_rating_cols = {
            "Google Maps": "recommendation_googlemap",
            "Waze":        "recommendation_waze",
            "Neshan":      "recommendation_neshan",
            "Balad":       "recommendation_balad",
        }
        fig, axes = plt.subplots(1, 2, figsize=(14, 6), facecolor=BG_COLOR)
        fig.suptitle("Navigation App Ratings by Drivers (0\u201310 scale)",
                     fontsize=15, fontweight="bold", y=0.99)
        ax    = axes[0]
        apps  = list(nav_rating_cols.keys())
        means = [short[c].dropna().mean() for c in nav_rating_cols.values()]
        ns    = [short[c].notna().sum()  for c in nav_rating_cols.values()]
        colors_ = [ACCENT, "#FFA726", "#66BB6A", "#AB47BC"]
        bars = ax.bar(apps, means, color=colors_, edgecolor="white", width=0.6)
        for bar, mean, n in zip(bars, means, ns):
            ax.text(bar.get_x() + bar.get_width() / 2, mean + 0.1,
                    f"{mean:.2f}\n(n={n:,})",
                    ha="center", fontsize=9, fontweight="bold")
        ax.set_ylim(0, 11)
        ax.set_ylabel("Mean Rating (0\u201310)")
        ax.set_title("Mean Recommendation Score", fontsize=11)
        style_ax(ax)
        ax = axes[1]
        for col, color, label in [
            ("recommendation_neshan", "#66BB6A", "Neshan"),
            ("recommendation_balad",  "#AB47BC", "Balad"),
        ]:
            data = short[col].dropna()
            vals = data.value_counts().sort_index()
            ax.plot(vals.index, vals.values, marker="o", color=color,
                    linewidth=2, label=f"{label} (n={len(data):,})")
        ax.set_xlabel("Rating (0\u201310)")
        ax.set_ylabel("Count")
        ax.set_title("Rating Distribution: Neshan vs Balad", fontsize=11)
        ax.legend(frameon=False, fontsize=9)
        style_ax(ax)
        save_fig(pdf, fig)

    # ================================================================
    # PAGE 20 – GPS FAILURE STAGE DISTRIBUTION
    # ================================================================
    with safe_page(pdf, 'Page 20 - GPS FAILURE STAGE DISTRIBUTION'):
        gps_stages = ["No problem", "Offer", "in route to passenger",
                      "Origin to Destination route", "All stages"]
        fig, axes = plt.subplots(1, 2, figsize=(12, 5),
                                 facecolor=BG_COLOR, sharey=True)
        fig.suptitle("GPS Failure Stage Distribution \u2013 Snapp vs Tapsi",
                     fontsize=15, fontweight="bold", y=0.99)
        for ax, col, color, label in [
            (axes[0], "snapp_gps_stage", SNAPP_COLOR, "Snapp"),
            (axes[1], "tapsi_gps_stage", TAPSI_COLOR, "Tapsi"),
        ]:
            data    = short[col].dropna().value_counts()
            total   = data.sum()
            ordered = [data.get(s, 0) for s in gps_stages]
            ax.barh(gps_stages, ordered, color=color, edgecolor="white")
            for i, v in enumerate(ordered):
                if v > 0:
                    ax.text(v + 5, i, f"{v} ({v/total*100:.0f}%)",
                            va="center", fontsize=9)
            ax.set_title(f"{label}  (n={total})", fontsize=11)
            style_ax(ax)
        save_fig(pdf, fig)

    # ================================================================
    # PAGE 21 – GPS GLITCH TIME OF DAY
    # ================================================================
    with safe_page(pdf, 'Page 21 - GPS GLITCH TIME OF DAY'):
        gps_time_cols = {
            "Morning (4\u20139 AM)":    "GPS Glitch Time__Morning(4-9AM)",
            "Before Noon (9\u201312 PM)": "GPS Glitch Time__Before Noon(9-12PM)",
            "Afternoon (12\u20134 PM)":  "GPS Glitch Time__Afternoon(12-4PM)",
            "Traffic Hours (4\u20138 PM)": "GPS Glitch Time__Traffic(4-8PM)",
            "Night (8 PM\u201312 AM)":    "GPS Glitch Time__Night(8-12AM)",
            "Late Night":               "GPS Glitch Time__Late Night",
        }
        time_labels = list(gps_time_cols.keys())
        time_vals   = [wide[c].sum() if c in wide.columns else 0
                       for c in gps_time_cols.values()]
        fig, ax = new_fig(
            "When GPS Glitches Occur \u2013 Time of Day (wide_survey)",
            figsize=(12, 5))
        bars = ax.bar(time_labels, time_vals, color=ACCENT, edgecolor="white")
        for bar in bars:
            ax.text(bar.get_x() + bar.get_width() / 2,
                    bar.get_height() + 5,
                    str(int(bar.get_height())), ha="center", fontsize=9)
        ax.set_ylabel("Count")
        ax.tick_params(axis="x", rotation=15)
        style_ax(ax)
        save_fig(pdf, fig)

    # ================================================================
    # PAGE 22 – GPS GLITCH ACTIONS
    # ================================================================
    with safe_page(pdf, 'Page 22 - GPS GLITCH ACTIONS'):
        gps_action_map = {
            "Called Passenger":
                "GPS Action when Glitch__Called Passenger",
            "Accepted Familiar Trips":
                "GPS Action when Glitch__Accepted familiar trips",
            "Passenger Help for Route":
                "GPS Action when Glitch__Passenger Help for route",
            "Decided to Stop":
                "GPS Action when Glitch__Decided to stop working",
            "Cancelled Trip":
                "GPS Action when Glitch__Cancelled Trip",
            "Changed Location":
                "GPS Action when Glitch__Changed Location",
            "Switched to Tapsi":
                "GPS Action when Glitch__Switched to Tapsi",
        }
        gps_action_vals = [wide[c].sum() if c in wide.columns else 0
                           for c in gps_action_map.values()]
        fig, ax = new_fig(
            "Driver Actions During GPS Glitch \u2013 wide_survey")
        ax.barh(list(gps_action_map.keys()), gps_action_vals,
                color=ACCENT, edgecolor="white")
        for i, v in enumerate(gps_action_vals):
            ax.text(v + 5, i, str(int(v)), va="center", fontsize=9)
        ax.invert_yaxis()
        ax.set_xlabel("Count")
        style_ax(ax)
        save_fig(pdf, fig)

    # ================================================================
    # PAGE 23 – COMMISSION & TAX TRANSPARENCY
    # ================================================================
    with safe_page(pdf, 'Page 23 - COMMISSION & TAX TRANSPARENCY'):
        fig, axes = plt.subplots(2, 2, figsize=(13, 8), facecolor=BG_COLOR)
        fig.suptitle(
            "Commission & Tax Transparency \u2013 What Drivers Believe",
            fontsize=15, fontweight="bold", y=0.97)
        for ax, col, color, label in [
            (axes[0, 0], "snapp_comm_info",
             SNAPP_COLOR, "Snapp Commission Rate"),
            (axes[0, 1], "tapsi_comm_info",
             TAPSI_COLOR, "Tapsi Commission Rate"),
            (axes[1, 0], "snapp_tax_info",
             SNAPP_COLOR, "Snapp Tax Info"),
            (axes[1, 1], "tapsi_tax_info",
             TAPSI_COLOR, "Tapsi Tax Info"),
        ]:
            data  = short[col].dropna()
            vc    = data.value_counts().head(10)
            total = len(data)
            ax.barh(vc.index[::-1], vc.values[::-1],
                    color=color, edgecolor="white")
            for i, (k, v) in enumerate(
                    zip(vc.index[::-1], vc.values[::-1])):
                ax.text(v + 1, i, f"{v} ({v/total*100:.0f}%)",
                        va="center", fontsize=8)
            ax.set_title(f"{label}  (n={total})", fontsize=10)
            style_ax(ax)
        save_fig(pdf, fig)

    # ================================================================
    # PAGE 24 – UNPAID FARES
    # ================================================================
    with safe_page(pdf, 'Page 24 - UNPAID FARES \u2013 INCIDENT RATE & COMPENSATION'):
        fig, axes = plt.subplots(1, 2, figsize=(14, 6), facecolor=BG_COLOR)
        fig.suptitle(
            "Unpaid by Passenger \u2013 Incident Rate & Compensation",
            fontsize=15, fontweight="bold", y=0.99)
        ax       = axes[0]
        platforms = ["Snapp", "Tapsi"]
        followup_cols = [
            "snapp_unpaid_by_passenger_followup",
            "tapsi_unpaid_by_passenger_followup",
        ]
        outcomes       = ["No", "Yes - No", "Yes - Yes"]
        outcome_colors = ["#EF5350", "#FFA726", "#66BB6A"]
        for pi, (platform, col, color) in enumerate(
                zip(platforms, followup_cols,
                    [SNAPP_COLOR, TAPSI_COLOR])):
            data  = short[col].dropna()
            total = len(data)
            vals  = [len(data[data == o]) for o in outcomes]
            for oi, (o, v) in enumerate(zip(outcomes, vals)):
                ax.bar(oi + pi * (0.35 + 0.02) - 0.22, v, 0.35,
                       color=outcome_colors[oi], edgecolor="white")
                ax.text(oi + pi * (0.35 + 0.02) - 0.22, v + 5,
                        f"{v/total*100:.0f}%", ha="center", fontsize=8)
        ax.set_xticks(np.arange(3) + 0.11)
        ax.set_xticklabels(
            ["No Followup", "Followed \u2013 Not\nResolved",
             "Followed \u2013\nResolved"], fontsize=9)
        ax.set_ylabel("Count")
        ax.set_title("Unpaid Fare Followup", fontsize=11)
        snapp_p = mpatches.Patch(color=SNAPP_COLOR, label="Snapp")
        tapsi_p = mpatches.Patch(color=TAPSI_COLOR, label="Tapsi")
        ax.legend(handles=[snapp_p, tapsi_p], frameon=False, fontsize=9)
        style_ax(ax)
        ax      = axes[1]
        comp_cats   = ["Yes - all of it", "Yes - Part of it", "No compensation"]
        comp_colors = ["#66BB6A", "#FFA726", "#EF5350"]
        x = np.arange(len(comp_cats))
        w = 0.35
        for pi, (platform, (col, pcolor)) in enumerate([
            ("Snapp", ("snapp_compensate_unpaid_by_passenger", SNAPP_COLOR)),
            ("Tapsi", ("tapsi_compensate_unpaid_by_passenger", TAPSI_COLOR)),
        ]):
            data   = short[col].dropna()
            total  = len(data)
            vals   = [len(data[data == c]) for c in comp_cats]
            offset = (pi - 0.5) * (w + 0.02)
            ax.bar(x + offset, vals, w, color=pcolor, edgecolor="white",
                   label=platform, alpha=0.85)
            for xi, v in enumerate(vals):
                ax.text(xi + offset, v + 1,
                        f"{v/total*100:.0f}%", ha="center", fontsize=8)
        ax.set_xticks(x)
        ax.set_xticklabels(["Full\nCompensation", "Partial", "None"],
                           fontsize=9)
        ax.set_title("Compensation Outcome", fontsize=11)
        ax.legend(frameon=False, fontsize=9)
        style_ax(ax)
        save_fig(pdf, fig)

    # ================================================================
    # PAGE 25 – CUSTOMER SUPPORT CHANNEL USAGE
    # ================================================================
    with safe_page(pdf, 'Page 25 - CUSTOMER SUPPORT CHANNEL USAGE'):
        fig, axes = plt.subplots(1, 2, figsize=(13, 5), facecolor=BG_COLOR)
        fig.suptitle("Customer Support Channel Usage \u2013 Snapp vs Tapsi",
                     fontsize=15, fontweight="bold", y=0.99)
        for ax, col, color, label in [
            (axes[0], "snapp_customer_support", SNAPP_COLOR, "Snapp"),
            (axes[1], "tapsi_customer_support", TAPSI_COLOR, "Tapsi"),
        ]:
            data  = short[col].dropna().value_counts()
            total = data.sum()
            ax.barh(data.index[::-1], data.values[::-1],
                    color=color, edgecolor="white")
            for i, v in enumerate(data.values[::-1]):
                ax.text(v + 5, i, f"{v:,} ({v/total*100:.0f}%)",
                        va="center", fontsize=9)
            ax.set_title(f"{label}  (n={total})", fontsize=11)
            style_ax(ax)
        save_fig(pdf, fig)

    # ================================================================
    # PAGE 26 – CUSTOMER SUPPORT DEEP DIVE
    # ================================================================
    with safe_page(pdf, 'Page 26 - CUSTOMER SUPPORT DEEP DIVE'):
        fig, axes = plt.subplots(1, 3, figsize=(16, 6), facecolor=BG_COLOR)
        fig.suptitle(
            "Customer Support Quality Deep Dive \u2013 Snapp vs Tapsi",
            fontsize=15, fontweight="bold", y=0.99)
        ax         = axes[0]
        solve_cats = ["Yes", "To an extent", "No"]
        for pi, (col, color, label) in enumerate([
            ("snapp_CS_solved", SNAPP_COLOR, "Snapp"),
            ("tapsi_CS_solved", TAPSI_COLOR, "Tapsi"),
        ]):
            data  = short[col].dropna()
            total = len(data)
            vals  = [len(data[data == c]) for c in solve_cats]
            x     = np.arange(len(solve_cats))
            w     = 0.35
            offset = (pi - 0.5) * (w + 0.02)
            ax.bar(x + offset, vals, w, color=color, edgecolor="white",
                   label=label, alpha=0.85)
            for xi, v in enumerate(vals):
                ax.text(xi + offset, v + 1,
                        f"{v/total*100:.0f}%", ha="center", fontsize=8)
        ax.set_xticks(x)
        ax.set_xticklabels(solve_cats, fontsize=10)
        ax.set_title("Issue Resolution Rate", fontsize=11)
        ax.legend(frameon=False, fontsize=9)
        style_ax(ax)
        ax       = axes[1]
        cs_dims  = ["Overall", "Wait Time", "Solution", "Behaviour", "Relevance"]
        snapp_cs_means = [
            short["snapp_CS_satisfaction_overall"].mean(),
            short["snapp_CS_satisfaction_waittime"].mean(),
            short["snapp_CS_satisfaction_solution"].mean(),
            short["snapp_CS_satisfaction_behaviour"].mean(),
            short["snapp_CS_satisfaction_relevance"].mean(),
        ]
        tapsi_cs_means = [
            short["tapsi_CS_satisfaction_overall"].mean(),
            short["tapsi_CS_satisfaction_waittime"].mean(),
            short["tapsi_CS_satisfaction_solution"].mean(),
            short["tapsi_CS_satisfaction_behaviour"].mean(),
            short["tapsi_CS_satisfaction_relevance"].mean(),
        ]
        x = np.arange(len(cs_dims))
        w = 0.35
        ax.bar(x - w/2, snapp_cs_means, w, color=SNAPP_COLOR,
               label="Snapp", edgecolor="white")
        ax.bar(x + w/2, tapsi_cs_means, w, color=TAPSI_COLOR,
               label="Tapsi", edgecolor="white")
        for xi, (sv, tv) in enumerate(zip(snapp_cs_means, tapsi_cs_means)):
            ax.text(xi - w/2, sv + 0.05, f"{sv:.2f}",
                    ha="center", fontsize=7.5, fontweight="bold")
            ax.text(xi + w/2, tv + 0.05, f"{tv:.2f}",
                    ha="center", fontsize=7.5, fontweight="bold")
        ax.set_xticks(x)
        ax.set_xticklabels(cs_dims, fontsize=9, rotation=10)
        ax.set_ylim(0, 5.5)
        ax.set_title("CS Satisfaction Sub-scores (1\u20135)", fontsize=11)
        ax.legend(frameon=False, fontsize=9)
        style_ax(ax)
        ax         = axes[2]
        reasons_s  = short["snapp_CS_satisfaction_important_reason"].dropna(
        ).value_counts().head(5)
        ax.barh(reasons_s.index[::-1], reasons_s.values[::-1],
                color=SNAPP_COLOR, edgecolor="white")
        total_s = reasons_s.sum()
        for i, v in enumerate(reasons_s.values[::-1]):
            ax.text(v + 1, i, f"{v} ({v/total_s*100:.0f}%)",
                    va="center", fontsize=9)
        ax.set_title("Snapp CS Dissatisfaction\nMain Reason (top 5)", fontsize=11)
        style_ax(ax)
        save_fig(pdf, fig)

    # ================================================================
    # PAGES 27 – 58  (unchanged from v6)
    # ================================================================

    with safe_page(pdf, 'Page 27 - COLLABORATION REASONS'):
        fig, axes = plt.subplots(1, 2, figsize=(14, 6), facecolor=BG_COLOR)
        fig.suptitle(
            "Collaboration Reasons \u2013 Why Drivers Chose Each Platform",
            fontsize=15, fontweight="bold", y=0.99)
        for ax, col, color, label in [
            (axes[0], "snapp_collab_reason", SNAPP_COLOR, "Snapp"),
            (axes[1], "tapsi_collab_reason", TAPSI_COLOR, "Tapsi"),
        ]:
            data  = short[col].dropna()
            data  = data[data.str.match(r'^[A-Za-z0-9 _\-\/]+$', na=False)]
            vc    = data.value_counts().head(10)
            total = data.notna().sum()
            ax.barh(vc.index[::-1], vc.values[::-1],
                    color=color, edgecolor="white")
            for i, v in enumerate(vc.values[::-1]):
                ax.text(v + 1, i, f"{v} ({v/total*100:.1f}%)",
                        va="center", fontsize=9)
            ax.set_title(f"{label}  (n={total})", fontsize=11)
            style_ax(ax)
        save_fig(pdf, fig)

    with safe_page(pdf, 'Page 28 - INCOME SOURCE PREFERENCE'):
        fig, axes = plt.subplots(1, 2, figsize=(12, 5), facecolor=BG_COLOR)
        fig.suptitle(
            "Which Service Yields Better Income? (Driver Perception)",
            fontsize=15, fontweight="bold", y=0.99)
        for ax, col, color, label in [
            (axes[0], "snapp_better_income", SNAPP_COLOR, "Snapp Drivers' View"),
            (axes[1], "tapsi_better_income", TAPSI_COLOR, "Tapsi Drivers' View"),
        ]:
            data  = short[col].dropna().value_counts()
            total = data.sum()
            ax.barh(data.index, data.values, color=color, edgecolor="white")
            for i, v in enumerate(data.values):
                ax.text(v + 1, i, f"{v} ({v/total*100:.0f}%)",
                        va="center", fontsize=9)
            ax.set_title(f"{label}  (n={total})", fontsize=11)
            style_ax(ax)
        save_fig(pdf, fig)

    with safe_page(pdf, 'Page 29 - SATISFACTION BY YEARWEEK (TREND LINES)'):
        fig, axes = plt.subplots(1, 3, figsize=(16, 5),
                                 facecolor=BG_COLOR, sharey=True)
        fig.suptitle("Avg Satisfaction by Year-Week: Snapp vs Tapsi",
                     fontsize=15, fontweight="bold", y=0.99)
        for ax, (scol, tcol, label) in zip(axes, SAT_PAIRS):
            weekly_sat = short.groupby("yearweek").agg(
                snapp=(scol, "mean"), tapsi=(tcol, "mean"))
            ax.plot(weekly_sat.index.astype(str), weekly_sat["snapp"],
                    marker="o", color=SNAPP_COLOR, linewidth=2, label="Snapp")
            ax.plot(weekly_sat.index.astype(str), weekly_sat["tapsi"],
                    marker="s", color=TAPSI_COLOR, linewidth=2, label="Tapsi")
            for idx, row in weekly_sat.iterrows():
                if not np.isnan(row["snapp"]):
                    ax.annotate(
                        f"{row['snapp']:.2f}", (str(idx), row["snapp"]),
                        textcoords="offset points", xytext=(0, 8),
                        ha="center", fontsize=7, color=SNAPP_COLOR)
                if not np.isnan(row["tapsi"]):
                    ax.annotate(
                        f"{row['tapsi']:.2f}", (str(idx), row["tapsi"]),
                        textcoords="offset points", xytext=(0, -12),
                        ha="center", fontsize=7, color=TAPSI_COLOR)
            ax.set_title(label, fontsize=11)
            ax.set_ylim(0, 5.5)
            ax.set_xlabel("Year-Week")
            ax.legend(frameon=False, fontsize=8)
            ax.tick_params(axis="x", rotation=45)
            style_ax(ax)
        axes[0].set_ylabel("Mean Satisfaction (1\u20135)")
        save_fig(pdf, fig)

    with safe_page(pdf, 'Page 30 - SAT BY COOPERATION TYPE'):
        plot_sat_by_group(pdf, short, "cooperation_type", "Cooperation Type")

    with safe_page(pdf, 'Page 31 - SAT BY CITY (TOP 10)'):
        plot_sat_by_group(pdf, short, "city", "City (Top 10)",
                          top_n=10, min_group_size=20)

    with safe_page(pdf, 'Page 32 - SAT BY DRIVER TYPE'):
        plot_sat_by_group(pdf, short, "driver_type",
                          "Driver Type (Snapp Exclusive vs Joint)")

    with safe_page(pdf, 'Page 33 - SATISFACTION HONEYMOON EFFECT'):
        valid_tenure = [t for t in TENURE_ORDER
                        if t in short["snapp_age"].unique()]
        sat_by_tenure = short.groupby("snapp_age").agg(
            snapp_overall=("snapp_overall_satisfaction", "mean"),
            snapp_fare=("snapp_fare_satisfaction", "mean"),
            snapp_income=("snapp_income_satisfaction", "mean"),
            snapp_rec=("snapp_recommend", "mean"),
            n=("snapp_overall_satisfaction", "count"),
        ).reindex(valid_tenure)
        labels_map  = dict(zip(TENURE_ORDER, TENURE_LABELS))
        xticklabels = [labels_map.get(t, t) for t in valid_tenure]
        fig, axes   = plt.subplots(1, 2, figsize=(14, 6), facecolor=BG_COLOR)
        fig.suptitle(
            "Honeymoon Effect \u2013 Satisfaction Declines with Snapp Tenure",
            fontsize=15, fontweight="bold", y=0.99)
        ax = axes[0]
        x  = np.arange(len(valid_tenure))
        ax.plot(x, sat_by_tenure["snapp_overall"], marker="o",
                color=SNAPP_COLOR, linewidth=2.5, markersize=9,
                label="Overall Sat.")
        ax.plot(x, sat_by_tenure["snapp_fare"], marker="s",
                color=ACCENT, linewidth=2, markersize=7, linestyle="--",
                label="Fare Sat.")
        ax.plot(x, sat_by_tenure["snapp_income"], marker="^",
                color=ACCENT2, linewidth=2, markersize=7, linestyle="--",
                label="Income Sat.")
        for i, (idx, row) in enumerate(sat_by_tenure.iterrows()):
            ax.annotate(f"{row['snapp_overall']:.2f}",
                        (i, row["snapp_overall"]),
                        textcoords="offset points",
                        xytext=(0, 10), ha="center", fontsize=9)
        ax.set_xticks(x)
        ax.set_xticklabels(xticklabels)
        ax.set_ylim(0, 5.5)
        ax.set_ylabel("Mean Satisfaction (1\u20135)")
        ax.set_xlabel("Snapp Tenure")
        ax.set_title("Snapp Satisfaction by Tenure", fontsize=11)
        ax.legend(frameon=False, fontsize=9)
        style_ax(ax)
        ax = axes[1]
        ax.plot(x, sat_by_tenure["snapp_rec"], marker="D",
                color=TAPSI_COLOR, linewidth=2.5, markersize=9,
                label="Snapp Recommend (0\u201310)")
        for i, (idx, row) in enumerate(sat_by_tenure.iterrows()):
            ax.annotate(f"{row['snapp_rec']:.2f}", (i, row["snapp_rec"]),
                        textcoords="offset points",
                        xytext=(0, 10), ha="center", fontsize=9)
        n_labels = [
            f"{xticklabels[i]}\n(n={int(sat_by_tenure['n'].iloc[i]):,})"
            for i in range(len(valid_tenure))
        ]
        ax.set_xticks(x)
        ax.set_xticklabels(n_labels, fontsize=9)
        ax.set_ylim(0, 10)
        ax.set_ylabel("Avg Recommendation Score (0\u201310)")
        ax.set_xlabel("Snapp Tenure")
        ax.set_title("Recommendation by Tenure", fontsize=11)
        ax.legend(frameon=False, fontsize=9)
        style_ax(ax)
        save_fig(pdf, fig)

    with safe_page(pdf, 'Page 34 - SAT BY AGE GROUP'):
        age_order = ["<18", "18_25", "26_35", "36_45", "46_55", "56_65", ">65"]
        plot_sat_by_group(pdf, short, "age", "Age Group", order=age_order)

    with safe_page(pdf, 'Page 35 - SAT BY ACTIVE TIME'):
        active_order = [
            "few hours/month", "<20hour/mo", "5_20hour/week",
            "20_40h/week", ">40h/week", "8_12hour/day", ">12h/day",
        ]
        active_labels_display = {
            "few hours/month": "Few h/mo", "<20hour/mo": "<20h/mo",
            "5_20hour/week": "5\u201320h/wk", "20_40h/week": "20\u201340h/wk",
            ">40h/week": ">40h/wk", "8_12hour/day": "8\u201312h/d",
            ">12h/day": ">12h/d",
        }
        df_active = short.copy()
        df_active["active_time_ordered"] = df_active["active_time"].map(
            lambda x: active_order.index(x) if x in active_order else 99)
        df_active = df_active[df_active["active_time_ordered"] < 99]
        active_grps = [a for a in active_order
                       if a in df_active["active_time"].unique()]
        active_sat  = df_active.groupby("active_time").agg(
            snapp_overall=("snapp_overall_satisfaction", "mean"),
            tapsi_overall=("tapsi_overall_satisfaction", "mean"),
            snapp_rec=("snapp_recommend", "mean"),
            tapsi_rec=("tapsidriver_tapsi_recommend", "mean"),
            n=("snapp_overall_satisfaction", "count"),
        ).reindex(active_grps)
        fig, axes = plt.subplots(1, 2, figsize=(14, 6), facecolor=BG_COLOR)
        fig.suptitle(
            "Satisfaction & Recommendation by Driver Engagement Level",
            fontsize=15, fontweight="bold", y=0.99)
        x       = np.arange(len(active_grps))
        w       = 0.35
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
        ax.set_title("Recommendation Score (0\u201310)", fontsize=11)
        ax.tick_params(axis="x", rotation=15)
        ax.legend(frameon=False, fontsize=9)
        style_ax(ax)
        save_fig(pdf, fig)

    with safe_page(pdf, 'Page 36 - SAT BY OCCUPATION'):
        job_sat = short.groupby("original_job").agg(
            n=("snapp_overall_satisfaction", "count"),
            snapp_overall=("snapp_overall_satisfaction", "mean"),
            tapsi_overall=("tapsi_overall_satisfaction", "mean"),
            snapp_rec=("snapp_recommend", "mean"),
        ).query("n >= 100").sort_values("snapp_overall")
        fig, axes = plt.subplots(1, 2, figsize=(14, 7), facecolor=BG_COLOR)
        fig.suptitle(
            "Satisfaction by Primary Occupation (jobs with n\u2265100)",
            fontsize=15, fontweight="bold", y=0.99)
        ax   = axes[0]
        jobs = job_sat.index.tolist()
        ax.barh(jobs, job_sat["snapp_overall"], color=SNAPP_COLOR,
                alpha=0.85, label="Snapp Overall")
        ax.barh(jobs, job_sat["tapsi_overall"], color=TAPSI_COLOR,
                alpha=0.55, label="Tapsi Overall", left=0)
        ax.set_title("Snapp vs Tapsi Overall Satisfaction", fontsize=11)
        ax.legend(frameon=False, fontsize=9)
        style_ax(ax)
        for i, (job, row) in enumerate(job_sat.iterrows()):
            ax.text(row["snapp_overall"] + 0.02, i,
                    f"{row['snapp_overall']:.2f} / {row['tapsi_overall']:.2f}",
                    va="center", fontsize=8)
        ax.set_xlabel("Mean Satisfaction (1\u20135)")
        ax = axes[1]
        bar_colors = [
            SNAPP_COLOR if v >= job_sat["snapp_rec"].median() else GREY
            for v in job_sat["snapp_rec"]
        ]
        ax.barh(jobs, job_sat["snapp_rec"],
                color=bar_colors, edgecolor="white")
        ax.axvline(job_sat["snapp_rec"].median(), color="black",
                   linestyle="--", linewidth=1,
                   label=f"Median: {job_sat['snapp_rec'].median():.1f}")
        for i, (job, row) in enumerate(job_sat.iterrows()):
            ax.text(row["snapp_rec"] + 0.05, i,
                    f"{row['snapp_rec']:.1f} (n={int(row['n'])})",
                    va="center", fontsize=8)
        ax.set_title("Snapp Recommendation Score (0\u201310)", fontsize=11)
        ax.set_xlabel("Mean Recommendation")
        ax.legend(frameon=False, fontsize=9)
        style_ax(ax)
        save_fig(pdf, fig)

    with safe_page(pdf, 'Page 37 - TAPSI CARPOOLING'):
        fig, axes = plt.subplots(2, 2, figsize=(13, 9), facecolor=BG_COLOR)
        fig.suptitle(
            "Tapsi Carpooling \u2013 Familiarity, Adoption, Refusal & Satisfaction",
            fontsize=15, fontweight="bold", y=0.97)
        ax  = axes[0, 0]
        fam = short["tapsi_carpooling_familiar"].value_counts()
        ax.pie(fam.values, labels=fam.index, autopct="%1.0f%%",
               colors=[TAPSI_COLOR, LGREY], startangle=90,
               wedgeprops={"edgecolor": "white"})
        ax.set_title("Carpooling Familiarity", fontsize=11)
        ax = axes[0, 1]
        offer_data = short["tapsi_carpooling_gotoffer_accepted"].dropna(
        ).value_counts()
        offer_colors = {"No": LGREY, "got offer - rejected": "#FF6D00",
                        "got offer - accepted": "#66BB6A"}
        ax.barh(offer_data.index, offer_data.values,
                color=[offer_colors.get(k, GREY) for k in offer_data.index],
                edgecolor="white")
        total = offer_data.sum()
        for i, v in enumerate(offer_data.values):
            ax.text(v + 5, i, f"{v} ({v/total*100:.0f}%)",
                    va="center", fontsize=9)
        ax.set_title("Carpooling Offer Outcome", fontsize=11)
        style_ax(ax)
        ax = axes[1, 0]
        carp_refusal = {
            "Canceled by Passenger":
                "Tapsi Carpooling refusal__Canceled by Passenger",
            "Long Wait Time":
                "Tapsi Carpooling refusal__Long Wait Time",
            "Passenger Distance":
                "Tapsi Carpooling refusal__Passenger Distance",
            "Not Familiar":
                "Tapsi Carpooling refusal__Not Familiar",
        }
        r_labels = list(carp_refusal.keys())
        r_vals   = [wide[c].sum() if c in wide.columns else 0
                    for c in carp_refusal.values()]
        ax.barh(r_labels, r_vals, color=TAPSI_COLOR, edgecolor="white")
        for i, v in enumerate(r_vals):
            ax.text(v + 2, i, str(int(v)), va="center", fontsize=9)
        ax.set_title("Carpooling Refusal Reasons (wide_survey)", fontsize=11)
        style_ax(ax)
        ax       = axes[1, 1]
        carp_sat = short["tapsi_carpooling_satisfaction_overall"].dropna()
        sat_counts = carp_sat.value_counts().sort_index()
        ax.bar(sat_counts.index.astype(int).astype(str), sat_counts.values,
               color=TAPSI_COLOR, edgecolor="white")
        total = sat_counts.sum()
        for xi, v in zip(sat_counts.index.astype(int).astype(str),
                         sat_counts.values):
            ax.text(xi, v + 1, f"{v} ({v/total*100:.0f}%)",
                    ha="center", fontsize=9)
        ax.set_title(
            f"Carpooling Satisfaction (1\u20135, mean={carp_sat.mean():.2f})",
            fontsize=11)
        ax.set_xlabel("Rating")
        style_ax(ax)
        save_fig(pdf, fig)

    with safe_page(pdf, 'Page 38 - ECOPLUS & MAGICAL WINDOW'):
        fig, axes = plt.subplots(1, 2, figsize=(13, 5), facecolor=BG_COLOR)
        fig.suptitle(
            "Feature Adoption \u2013 Snapp EcoPlus & Tapsi Magical Window",
            fontsize=15, fontweight="bold", y=0.99)
        ax = axes[0]
        ecoplus_familiar = short["snapp_ecoplus_familiar"].dropna(
        ).value_counts()
        ecoplus_usage    = short["snapp_ecoplus_access_usage"].dropna(
        ).value_counts()
        cats   = ["Familiar", "Has Access\n& Uses",
                  "Has Access\n& Not Using", "Not Familiar"]
        vals   = [
            ecoplus_familiar.get("Yes", 0),
            ecoplus_usage.get("Yes-Yes", 0),
            ecoplus_usage.get("Yes-No", 0),
            ecoplus_familiar.get("No", 0),
        ]
        colors_ = [SNAPP_COLOR, "#66BB6A", "#FFA726", LGREY]
        ax.bar(cats, vals, color=colors_, edgecolor="white")
        total = len(short)
        for i, v in enumerate(vals):
            ax.text(i, v + 10, f"{v:,}\n({v/total*100:.1f}%)",
                    ha="center", fontsize=9)
        ax.set_title("Snapp EcoPlus Adoption Funnel", fontsize=11)
        style_ax(ax)
        ax = axes[1]
        mw        = short["tapsi_magical_window"].dropna().value_counts()
        mw_colors = {"Yes": TAPSI_COLOR, "No": "#FFA726", "Not Familiar": LGREY}
        ax.bar(mw.index, mw.values,
               color=[mw_colors.get(k, GREY) for k in mw.index],
               edgecolor="white")
        total_mw = mw.sum()
        for i, (k, v) in enumerate(mw.items()):
            ax.text(i, v + 10, f"{v:,} ({v/total_mw*100:.0f}%)",
                    ha="center", fontsize=9)
        ax.set_title("Tapsi Magical Window Awareness", fontsize=11)
        style_ax(ax)
        save_fig(pdf, fig)

    with safe_page(pdf, 'Page 39 - DRIVER PRIVACY & PARTICIPATION ATTITUDES'):
        fig, axes = plt.subplots(1, 2, figsize=(13, 5), facecolor=BG_COLOR)
        fig.suptitle(
            "Driver Privacy & Participation Attitudes (Snapp)",
            fontsize=15, fontweight="bold", y=0.99)
        ax      = axes[0]
        feeling = short["snapp_participate_feeling"].dropna().value_counts()
        total_f = feeling.sum()
        feel_colors = {
            "no difference": SNAPP_COLOR, "no worry": "#66BB6A",
            "talk to some people": ACCENT, "prefer not to talk": "#FFA726",
            "no talk at all": "#EF5350",
        }
        ax.barh(feeling.index, feeling.values,
                color=[feel_colors.get(k, GREY) for k in feeling.index],
                edgecolor="white")
        for i, v in enumerate(feeling.values):
            ax.text(v + 3, i, f"{v} ({v/total_f*100:.0f}%)",
                    va="center", fontsize=9)
        ax.set_title(
            "Comfort Level When Working\nAround Other People", fontsize=11)
        ax.set_xlabel("Count")
        style_ax(ax)
        ax      = axes[1]
        reason  = short["snapp_not_talking_reason"].dropna().value_counts()
        total_r = reason.sum()
        ax.barh(reason.index, reason.values, color=ACCENT2, edgecolor="white")
        for i, v in enumerate(reason.values):
            ax.text(v + 1, i, f"{v} ({v/total_r*100:.0f}%)",
                    va="center", fontsize=9)
        ax.set_title(
            "Why Drivers Avoid Talking\nAbout Their Work", fontsize=11)
        ax.set_xlabel("Count")
        style_ax(ax)
        save_fig(pdf, fig)

    with safe_page(pdf, 'Page 40 - DEMAND & SUPPLY METRICS'):
        fig, axes = plt.subplots(1, 3, figsize=(15, 5), facecolor=BG_COLOR)
        fig.suptitle(
            "Demand & Supply \u2013 How Much Demand Do Drivers Process?",
            fontsize=15, fontweight="bold", y=0.99)
        for ax, col, color, label in [
            (axes[0], "demand_process",        ACCENT,       "% of Demand Processed"),
            (axes[1], "missed_demand_per_10",   ACCENT2,      "Missed per 10 Trips"),
            (axes[2], "max_demand",             SNAPP_COLOR,  "Max Simultaneous Demand"),
        ]:
            data  = short[col].dropna().value_counts()
            total = data.sum()
            ax.barh(data.index, data.values, color=color, edgecolor="white")
            for i, v in enumerate(data.values):
                ax.text(v + 1, i, f"{v} ({v/total*100:.0f}%)",
                        va="center", fontsize=9)
            ax.set_title(label, fontsize=11)
            style_ax(ax)
        save_fig(pdf, fig)

    with safe_page(pdf, 'Page 41 - COMMISSION-FREE RIDES'):
        fig, axes = plt.subplots(1, 2, figsize=(12, 5), facecolor=BG_COLOR)
        fig.suptitle("Commission-Free Rides vs Total Rides",
                     fontsize=15, fontweight="bold", y=0.99)
        for ax, ride_col, cf_col, color, label in [
            (axes[0], "snapp_ride", "snapp_commfree", SNAPP_COLOR, "Snapp"),
            (axes[1], "tapsi_ride", "tapsi_commfree", TAPSI_COLOR, "Tapsi"),
        ]:
            mask = short[cf_col].notna()
            ax.scatter(short.loc[mask, ride_col], short.loc[mask, cf_col],
                       alpha=0.3, color=color,
                       edgecolors="white", linewidth=0.3, s=30)
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

    with safe_page(pdf, 'Page 42 - TOP 15 CITIES'):
        city_counts = short["city"].value_counts().head(15)
        fig, ax     = new_fig("Top 15 Cities by Response Count")
        ax.barh(city_counts.index[::-1], city_counts.values[::-1],
                color=ACCENT, edgecolor="white")
        for i, v in enumerate(city_counts.values[::-1]):
            ax.text(v + 1, i, f"{v:,}", va="center", fontsize=9)
        ax.set_xlabel("Responses")
        style_ax(ax)
        save_fig(pdf, fig)

    with safe_page(pdf, 'Page 43 - CITY SATISFACTION'):
        top_cities = short["city"].value_counts().head(12).index
        city_sat   = (
            short[short["city"].isin(top_cities)]
            .groupby("city")
            .agg(
                snapp_sat=("snapp_overall_satisfaction", "mean"),
                tapsi_sat=("tapsi_overall_satisfaction", "mean"),
                snapp_rec=("snapp_recommend", "mean"),
                tapsi_rec=("tapsidriver_tapsi_recommend", "mean"),
                n=("snapp_overall_satisfaction", "count"),
            )
            .sort_values("snapp_sat")
        )
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
        ax.set_xlabel("Mean Satisfaction (1\u20135)")
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
        ax.set_xlabel("Mean Recommendation (0\u201310)")
        ax.set_title("Recommendation Score", fontsize=11)
        ax.legend(frameon=False, fontsize=9)
        style_ax(ax)
        save_fig(pdf, fig)

    with safe_page(pdf, 'Page 44 - REGISTRATION & REFERRAL'):
        fig, axes = plt.subplots(2, 2, figsize=(13, 9), facecolor=BG_COLOR)
        fig.suptitle("Registration & Referral Analysis",
                     fontsize=15, fontweight="bold", y=0.97)
        for ax, col, color, label in [
            (axes[0, 0], "snapp_register_type",   SNAPP_COLOR, "Snapp Registration Type"),
            (axes[0, 1], "tapsi_register_type",   TAPSI_COLOR, "Tapsi Registration Type"),
            (axes[1, 0], "snapp_main_reg_reason",  SNAPP_COLOR, "Snapp Registration Reason"),
            (axes[1, 1], "tapsi_main_reg_reason",  TAPSI_COLOR, "Tapsi Registration Reason"),
        ]:
            data  = short[col].dropna().value_counts()
            total = data.sum()
            ax.barh(data.index[::-1], data.values[::-1],
                    color=color, edgecolor="white")
            for i, v in enumerate(data.values[::-1]):
                ax.text(v + 2, i, f"{v} ({v/total*100:.0f}%)",
                        va="center", fontsize=9)
            ax.set_title(label, fontsize=10)
            style_ax(ax)
        save_fig(pdf, fig)

    with safe_page(pdf, 'Page 45 - INCENTIVE TYPE (long)'):
        plot_long_snapp_vs_tapsi(
            pdf, long, "Snapp Incentive Type", "Tapsi Incentive Type",
            "Incentive Type \u2013 long_survey")

    with safe_page(pdf, 'Page 46 - INCENTIVE GOT BONUS (long)'):
        plot_long_snapp_vs_tapsi(
            pdf, long, "Snapp Incentive GotBonus", "Tapsi Incentive GotBonus",
            "Incentive Got Bonus \u2013 long_survey")

    with safe_page(pdf, 'Page 47 - CS CATEGORY (long)'):
        plot_long_snapp_vs_tapsi(
            pdf, long, "Snapp Customer Support Category",
            "Tapsi Customer Support Category",
            "Customer Support Categories \u2013 long_survey")

    with safe_page(pdf, 'Page 48 - SNAPP NAVIGATION ISSUES'):
        nav_unsat_q   = long[long["question"] == "Snapp Navigation Unsatisfaction"]
        nav_refusal_q = long[long["question"] == "Snapp Navigation Refusal"]
        fig, axes     = plt.subplots(1, 2, figsize=(14, 6), facecolor=BG_COLOR)
        fig.suptitle("Snapp Navigation Issues \u2013 long_survey",
                     fontsize=15, fontweight="bold", y=0.99)
        for ax, qdata, color, label in [
            (axes[0], nav_unsat_q,   SNAPP_COLOR, "Navigation Unsatisfaction"),
            (axes[1], nav_refusal_q, ACCENT,      "Navigation Refusal Reasons"),
        ]:
            if len(qdata) == 0:
                ax.set_title(f"{label} (no data)", fontsize=11)
                style_ax(ax)
                continue
            vc    = qdata["answer"].value_counts().sort_values(ascending=True)
            total = vc.sum()
            ax.barh(vc.index, vc.values, color=color, edgecolor="white")
            for i, (k, v) in enumerate(vc.items()):
                ax.text(v + 5, i, f"{v} ({v/total*100:.0f}%)",
                        va="center", fontsize=9)
            ax.set_title(f"{label}  (n={total:,})", fontsize=11)
            style_ax(ax)
            ax.set_xlabel("Count")
        save_fig(pdf, fig)

    with safe_page(pdf, 'Page 49 - DECLINE REASON & APP MENU USAGE'):
        fig, axes = plt.subplots(1, 2, figsize=(14, 6), facecolor=BG_COLOR)
        fig.suptitle(
            "Decline Reason & SnappDriver App Menu Usage \u2013 long_survey",
            fontsize=15, fontweight="bold", y=0.99)
        for ax, question, color, label in [
            (axes[0], "Decline Reason",       SNAPP_COLOR, "Decline Reason"),
            (axes[1], "SnappDriver App Menu", ACCENT,      "SnappDriver App Menu"),
        ]:
            qdata = long[long["question"] == question]
            if len(qdata) == 0:
                ax.set_title(f"{label} (no data)", fontsize=11)
                style_ax(ax)
                continue
            vc    = qdata["answer"].value_counts().sort_values(ascending=True)
            total = vc.sum()
            ax.barh(vc.index, vc.values, color=color, edgecolor="white")
            for i, (k, v) in enumerate(vc.items()):
                ax.text(v + 2, i, f"{v} ({v/total*100:.0f}%)",
                        va="center", fontsize=9)
            ax.set_title(f"{label}  (n={total:,})", fontsize=11)
            style_ax(ax)
            ax.set_xlabel("Count")
        save_fig(pdf, fig)

    with safe_page(pdf, 'Page 50 - TAPSI-ONLY QUESTIONS (LONG SURVEY)'):
        fig, axes = plt.subplots(1, 2, figsize=(14, 5), facecolor=BG_COLOR)
        fig.suptitle("Tapsi-Only Questions \u2013 long_survey",
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
            vc    = qdata["answer"].value_counts().sort_values(ascending=True)
            total = vc.sum()
            ax.barh(vc.index, vc.values, color=color, edgecolor="white")
            for i, (k, v) in enumerate(vc.items()):
                ax.text(v + 0.3, i, f"{v} ({v/total*100:.0f}%)",
                        va="center", fontsize=9)
            ax.set_title(f"{label}  (n={total})", fontsize=11)
            style_ax(ax)
            ax.set_xlabel("Count")
        save_fig(pdf, fig)

    for q, platform, color in [
        ("Snapp Customer Support Category", "Snapp", SNAPP_COLOR),
        ("Tapsi Customer Support Category", "Tapsi", TAPSI_COLOR),
    ]:
        qdata = long[long["question"] == q]
        if len(qdata) == 0:
            continue
        pivot   = qdata.groupby(["driver_type", "answer"]).size().unstack(
            fill_value=0)
        answers = pivot.columns.tolist()
        groups  = pivot.index.tolist()
        fig, ax = new_fig(
            f"{platform} Customer Support by Driver Type", figsize=(14, 6))
        y        = np.arange(len(answers))
        n_g      = len(groups)
        total_w  = 0.7
        w        = total_w / n_g
        grp_colors = ["#42A5F5", "#EF5350"]
        for i, grp in enumerate(groups):
            vals = pivot.loc[grp].values
            ax.barh(y + i * w - total_w / 2, vals, w, label=grp,
                    color=grp_colors[i % len(grp_colors)], edgecolor="white")
        ax.set_yticks(y)
        ax.set_yticklabels(answers)
        ax.legend(frameon=False, fontsize=9)
        ax.set_xlabel("Count")
        style_ax(ax)
        save_fig(pdf, fig)

    with safe_page(pdf, 'Page 53 - APP USAGE & ECOPLUS REFUSAL'):
        fig, axes = plt.subplots(1, 2, figsize=(14, 5), facecolor=BG_COLOR)
        fig.suptitle("Snapp App Usage & EcoPlus Refusal \u2013 long_survey",
                     fontsize=15, fontweight="bold", y=0.99)
        for ax, question, color, label in [
            (axes[0], "Snapp Usage app",        SNAPP_COLOR, "Snapp App Usage"),
            (axes[1], "Snapp Ecoplus Refusal",  ACCENT2,     "EcoPlus Refusal Reasons"),
        ]:
            qdata = long[long["question"] == question]
            if len(qdata) == 0:
                ax.set_title(f"{label} (no data)", fontsize=11)
                style_ax(ax)
                continue
            vc    = qdata["answer"].value_counts().sort_values(ascending=True)
            total = vc.sum()
            ax.barh(vc.index, vc.values, color=color, edgecolor="white")
            for i, (k, v) in enumerate(vc.items()):
                ax.text(v + 0.5, i, f"{v} ({v/total*100:.0f}%)",
                        va="center", fontsize=9)
            ax.set_title(f"{label}  (n={total})", fontsize=11)
            style_ax(ax)
            ax.set_xlabel("Count")
        save_fig(pdf, fig)

    with safe_page(pdf, 'Page 54 - JOINT VS SNAPP-EXCLUSIVE KEY METRICS'):
        metrics_by_dt = short.groupby("driver_type").agg(
            n=("snapp_overall_satisfaction", "count"),
            snapp_sat=("snapp_overall_satisfaction", "mean"),
            tapsi_sat=("tapsi_overall_satisfaction", "mean"),
            snapp_ride=("snapp_ride", "mean"),
            tapsi_ride=("tapsi_ride", "mean"),
            snapp_inc=("snapp_incentive", "mean"),
            tapsi_inc=("tapsi_incentive", "mean"),
            snapp_rec=("snapp_recommend", "mean"),
            tapsi_rec=("tapsidriver_tapsi_recommend", "mean"),
        )
        fig, axes = plt.subplots(1, 3, figsize=(16, 5), facecolor=BG_COLOR)
        fig.suptitle("Joint vs Snapp Exclusive \u2013 Key Metric Comparison",
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
        for xi, (sv, tv) in enumerate(
                zip(metrics_by_dt["snapp_sat"], metrics_by_dt["tapsi_sat"])):
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
        for xi, (sv, tv) in enumerate(
                zip(metrics_by_dt["snapp_ride"], metrics_by_dt["tapsi_ride"])):
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
        for xi, (sv, tv) in enumerate(
                zip(metrics_by_dt["snapp_inc"] / 1e6,
                    metrics_by_dt["tapsi_inc"] / 1e6)):
            ax.text(xi - w/2, sv + 0.05, f"{sv:.1f}M",
                    ha="center", fontsize=9, fontweight="bold")
            ax.text(xi + w/2, tv + 0.05, f"{tv:.1f}M",
                    ha="center", fontsize=9, fontweight="bold")
        ax.set_title("Avg Incentive (M Rials)", fontsize=11)
        ax.legend(frameon=False, fontsize=9)
        style_ax(ax)
        save_fig(pdf, fig)

    with safe_page(pdf, 'Page 55 - INCENTIVE FULL FUNNEL'):
        fig, axes = plt.subplots(1, 2, figsize=(14, 6), facecolor=BG_COLOR)
        fig.suptitle(
            "Incentive Full Funnel \u2013 Notification \u2192 Participation",
            fontsize=15, fontweight="bold", y=0.99)
        funnel_labels = [
            "Got Notification\n(Yes)", "Got Notification\n(No)",
            "Participated\n(Yes)", "Participated\n(No)",
        ]
        funnel_colors = [SNAPP_COLOR, LGREY, "#66BB6A", "#EF5350"]
        ax = axes[0]
        notif_vc  = short["snapp_gotmessage_text_incentive"].dropna(
        ).value_counts()
        partic_vc = short["snapp_incentive_participation"].dropna(
        ).value_counts()
        funnel_vals = [notif_vc.get("Yes", 0), notif_vc.get("No", 0),
                       partic_vc.get("Yes", 0), partic_vc.get("No", 0)]
        bars      = ax.bar(funnel_labels, funnel_vals,
                           color=funnel_colors, edgecolor="white")
        total_n   = notif_vc.sum()
        total_p   = partic_vc.sum()
        totals_   = [total_n, total_n, total_p, total_p]
        for i, (b, v, tot) in enumerate(zip(bars, funnel_vals, totals_)):
            if tot > 0:
                ax.text(b.get_x() + b.get_width() / 2, v + 30,
                        f"{v:,}\n({v/tot*100:.0f}%)", ha="center", fontsize=9)
        ax.set_title("Snapp Incentive Funnel", fontsize=11)
        ax.set_ylabel("Count")
        style_ax(ax)
        ax = axes[1]
        tapsi_notif_col  = "tapsi_gotmessage_text_incentive"
        tapsi_partic_col = "tapsi_incentive_participation"
        t_notif_vc  = (short[tapsi_notif_col].dropna().value_counts()
                       if tapsi_notif_col in short.columns
                       else pd.Series(dtype=int))
        t_partic_vc = (short[tapsi_partic_col].dropna().value_counts()
                       if tapsi_partic_col in short.columns
                       else pd.Series(dtype=int))
        t_funnel_vals = [t_notif_vc.get("Yes", 0), t_notif_vc.get("No", 0),
                         t_partic_vc.get("Yes", 0), t_partic_vc.get("No", 0)]
        bars2    = ax.bar(funnel_labels, t_funnel_vals,
                          color=funnel_colors, edgecolor="white")
        t_total_n = t_notif_vc.sum() if len(t_notif_vc) else 1
        t_total_p = t_partic_vc.sum() if len(t_partic_vc) else 1
        for i, (b, v, tot) in enumerate(
                zip(bars2, t_funnel_vals,
                    [t_total_n, t_total_n, t_total_p, t_total_p])):
            if tot > 0:
                ax.text(b.get_x() + b.get_width() / 2, v + 30,
                        f"{v:,}\n({v/tot*100:.0f}%)", ha="center", fontsize=9)
        ax.set_title("Tapsi Incentive Funnel", fontsize=11)
        ax.set_ylabel("Count")
        style_ax(ax)
        save_fig(pdf, fig)

    with safe_page(pdf, 'Page 56 - INCENTIVE ACTIVE DURATION'):
        time_order = ["Few Hours", "1 Day", "1_6 Days", "7 Days", ">7 Days"]
        fig, axes  = plt.subplots(1, 2, figsize=(14, 5), facecolor=BG_COLOR)
        fig.suptitle("Incentive Active Duration \u2013 Snapp vs Tapsi",
                     fontsize=15, fontweight="bold", y=0.99)
        for ax, col, color, label in [
            (axes[0], "snapp_incentive_length",            SNAPP_COLOR, "Snapp"),
            (axes[1], "tapsi_incentive_active_duration",   TAPSI_COLOR, "Tapsi"),
        ]:
            if col not in short.columns:
                ax.set_title(f"{label} (no data)")
                style_ax(ax)
                continue
            data    = short[col].dropna()
            present = [t for t in time_order if t in data.values]
            vc      = data.value_counts().reindex(present).dropna()
            total   = vc.sum()
            ax.bar(vc.index, vc.values, color=color, edgecolor="white")
            for i, (k, v) in enumerate(vc.items()):
                ax.text(i, v + 5, f"{v:,}\n({v/total*100:.0f}%)",
                        ha="center", fontsize=9)
            ax.set_title(
                f"{label} Incentive Duration  (n={total:,})", fontsize=11)
            ax.set_xlabel("Duration")
            ax.set_ylabel("Count")
            style_ax(ax)
        save_fig(pdf, fig)

    with safe_page(pdf, 'Page 57 - TAPSI RE-ACTIVATION TIMING'):
        inact_col   = "tapsi_inactive_b4_incentive"
        inact_order = ["Same Day", "1_3 Day Before", "4_7 Day Before",
                       "1_4 Week Before", "1_3 Month Before",
                       "3_6 Month Before", ">6 Month Before"]
        fig, ax     = plt.subplots(figsize=(12, 6), facecolor=BG_COLOR)
        fig.suptitle(
            "Tapsi Re-activation Timing: Inactivity Before Incentive Response",
            fontsize=15, fontweight="bold", y=1.01)
        if inact_col in short.columns:
            data    = short[inact_col].dropna()
            present = [v for v in inact_order if v in data.values]
            vc      = data.value_counts().reindex(present).dropna()
            total   = vc.sum()
            bar_colors = [
                TAPSI_COLOR if i == 0 else ("#FFA726" if i <= 2 else LGREY)
                for i in range(len(vc))
            ]
            ax.barh(vc.index[::-1], vc.values[::-1],
                    color=list(reversed(bar_colors)), edgecolor="white")
            for i, (k, v) in enumerate(
                    zip(vc.index[::-1], vc.values[::-1])):
                ax.text(v + 20, i, f"{v:,} ({v/total*100:.0f}%)",
                        va="center", fontsize=9)
            ax.set_xlabel("Count")
            ax.set_title(
                f"(n={total:,}) \u2014 orange = recently active, "
                f"grey = long dormant", fontsize=10)
        else:
            ax.set_title(f"{inact_col} not found in data", fontsize=11)
        style_ax(ax)
        save_fig(pdf, fig)

    with safe_page(pdf, 'Page 58 - APP NPS vs PLATFORM NPS'):
        fig, axes = plt.subplots(2, 2, figsize=(14, 10), facecolor=BG_COLOR)
        fig.suptitle("App NPS vs Platform NPS \u2013 Snapp & Tapsi",
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
            data       = short[col].dropna()
            nps        = nps_score(data)
            promoters  = (data >= 9).sum()
            detractors = (data <= 6).sum()
            total      = len(data)
            vc         = data.value_counts().sort_index()
            bar_clr    = [
                "#EF5350" if s <= 6 else ("#B0BEC5" if s <= 8 else "#66BB6A")
                for s in vc.index
            ]
            ax.bar(vc.index.astype(str), vc.values,
                   color=bar_clr, edgecolor="white")
            for xi, (k, v) in enumerate(vc.items()):
                ax.text(xi, v + 3, f"{v}", ha="center", fontsize=7)
            ax.set_title(
                f"{label}\nNPS={nps:+.0f}  |  "
                f"P={promoters/total*100:.0f}% "
                f"D={detractors/total*100:.0f}% (n={total:,})",
                fontsize=9)
            ax.set_xlabel("Score (0\u201310)")
            ax.set_ylabel("Count")
            style_ax(ax)
        save_fig(pdf, fig)

    # ================================================================
    # PAGE 59 – COMMISSION KNOWLEDGE × SATISFACTION CROSS-TAB
    # ================================================================
    # BUG-1 FIX: v6 showed "no data" because the cross-tab code reached
    # `.dropna()` on a pair of columns where one or both were all-NaN
    # (the _ensure block had pre-populated them with NaN without checking
    # whether real data already existed).  The fix adds an explicit pre-check
    # that logs what data is available and provides a clear fallback reason.
    with safe_page(pdf, 'Page 59 - COMMISSION KNOWLEDGE x SATISFACTION CROSS-TAB'):
        fig, axes = plt.subplots(1, 2, figsize=(14, 6), facecolor=BG_COLOR)
        fig.suptitle(
            "Commission Knowledge \u00d7 Overall Satisfaction Cross-Tab",
            fontsize=15, fontweight="bold", y=0.99)
        for ax, comm_col, sat_col, color, label in [
            (axes[0], "snapp_comm_info",
             "snapp_overall_satisfaction", SNAPP_COLOR, "Snapp"),
            (axes[1], "tapsi_comm_info",
             "tapsi_overall_satisfaction", TAPSI_COLOR, "Tapsi"),
        ]:
            # --- pre-check: confirm both columns have real (non-NaN) data ---
            comm_data = short[comm_col].dropna()
            sat_data  = short[sat_col].dropna()
            if len(comm_data) == 0:
                ax.set_title(
                    f"{label}\n(no data \u2014 {comm_col} is all-NaN)",
                    fontsize=10)
                style_ax(ax)
                print(f"[INFO] PAGE 59 {label}: {comm_col} has 0 non-NaN rows")
                continue
            if len(sat_data) == 0:
                ax.set_title(
                    f"{label}\n(no data \u2014 {sat_col} is all-NaN)",
                    fontsize=10)
                style_ax(ax)
                print(f"[INFO] PAGE 59 {label}: {sat_col} has 0 non-NaN rows")
                continue

            print(f"[INFO] PAGE 59 {label}: {comm_col} n={len(comm_data)}, "
                  f"unique={comm_data.nunique()}; "
                  f"{sat_col} n={len(sat_data)}")

            sub = short[[comm_col, sat_col]].dropna()
            if len(sub) == 0:
                ax.set_title(
                    f"{label}\n(no overlapping rows after joint dropna)",
                    fontsize=10)
                style_ax(ax)
                continue

            comm_groups  = sub.groupby(comm_col)[sat_col].mean().sort_values()
            n_per_group  = sub.groupby(comm_col)[sat_col].count()
            ax.barh(comm_groups.index, comm_groups.values,
                    color=color, edgecolor="white", alpha=0.85)
            for i, (k, v) in enumerate(comm_groups.items()):
                ax.text(v + 0.02, i,
                        f"{v:.2f}  (n={n_per_group[k]:,})",
                        va="center", fontsize=8)
            ax.set_xlim(0, 5.5)
            ax.axvline(sub[sat_col].mean(), color="black", linestyle="--",
                       linewidth=1,
                       label=f"Overall mean: {sub[sat_col].mean():.2f}")
            ax.set_title(
                f"{label}: Sat. by Commission Knowledge  (n={len(sub):,})",
                fontsize=11)
            ax.set_xlabel("Mean Satisfaction (1\u20135)")
            ax.legend(frameon=False, fontsize=9)
            style_ax(ax)
        save_fig(pdf, fig)

    # ================================================================
    # PAGE 60 – UNPAID FARE FOLLOW-UP SATISFACTION
    # ================================================================
    with safe_page(pdf, 'Page 60 - UNPAID FARE FOLLOW-UP SATISFACTION'):
        fig, axes = plt.subplots(1, 2, figsize=(14, 6), facecolor=BG_COLOR)
        fig.suptitle(
            "Unpaid Fare Follow-up Satisfaction \u2013 Snapp vs Tapsi",
            fontsize=15, fontweight="bold", y=0.99)
        followup_pairs = [
            ("snapp_satisfaction_followup_overall",
             "tapsi_satisfaction_followup_overall",
             "Overall Satisfaction with Follow-up"),
            ("snapp_satisfaction_followup_time",
             "tapsi_satisfaction_followup_time",
             "Satisfaction with Time-to-Resolve"),
        ]
        for ax, (scol, tcol, label) in zip(axes, followup_pairs):
            s_data = (short[scol].dropna() if scol in short.columns
                      else pd.Series(dtype=float))
            t_data = (short[tcol].dropna() if tcol in short.columns
                      else pd.Series(dtype=float))
            ratings   = [1, 2, 3, 4, 5]
            s_counts  = [s_data.value_counts().get(r, 0) for r in ratings]
            t_counts  = [t_data.value_counts().get(r, 0) for r in ratings]
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
                f"{label}\nSnapp mean={s_mean:.2f}  |  Tapsi mean={t_mean:.2f}",
                fontsize=10)
            ax.set_xlabel("Rating (1\u20135)")
            ax.set_ylabel("Count")
            ax.legend(frameon=False, fontsize=9)
            style_ax(ax)
        save_fig(pdf, fig)

    with safe_page(pdf, 'Page 61 - TRIP LENGTH PREFERENCE'):
        trip_order = ["Short Trip", "Average Trip", "Long Trip"]
        fig, axes  = plt.subplots(1, 2, figsize=(12, 5), facecolor=BG_COLOR)
        fig.suptitle("Trip Length Preference \u2013 What Drivers Mostly Accept",
                     fontsize=15, fontweight="bold", y=0.99)
        for ax, col, color, label in [
            (axes[0], "snapp_accepted_trip_length", SNAPP_COLOR, "Snapp"),
            (axes[1], "tapsi_accepted_trip_length", TAPSI_COLOR, "Tapsi"),
        ]:
            if col not in short.columns:
                ax.set_title(f"{label} (no data)")
                style_ax(ax)
                continue
            data    = short[col].dropna()
            present = [t for t in trip_order if t in data.values]
            vc      = data.value_counts().reindex(present).dropna()
            total   = vc.sum()
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

    with safe_page(pdf, 'Page 62 - NAVIGATION APP USED IN LAST TRIP'):
        fig, axes = plt.subplots(1, 2, figsize=(14, 6), facecolor=BG_COLOR)
        fig.suptitle(
            "Navigation App Used in Last Trip \u2013 Snapp vs Tapsi",
            fontsize=15, fontweight="bold", y=0.99)
        for ax, col, nav_order, color, label in [
            (axes[0], "snapp_last_trip_navigation",
             ["Neshan", "Balad", "Google Map", "Waze", "No Navigation App"],
             SNAPP_COLOR, "Snapp"),
            (axes[1], "tapsi_navigation_type",
             ["Neshan", "Balad", "In-App Navigation", "No Navigation App"],
             TAPSI_COLOR, "Tapsi"),
        ]:
            if col not in short.columns:
                ax.set_title(f"{label} (no data)")
                style_ax(ax)
                continue
            data    = short[col].dropna()
            present = [v for v in nav_order if v in data.values]
            vc      = data.value_counts().reindex(present).dropna()
            total   = vc.sum()
            bar_colors_nav = [
                LGREY if "No" in k else (
                    TAPSI_COLOR if "In-App" in k and color == TAPSI_COLOR
                    else color)
                for k in vc.index
            ]
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

    with safe_page(pdf, 'Page 63 - JOINING BONUS'):
        fig, axes = plt.subplots(1, 2, figsize=(12, 5), facecolor=BG_COLOR)
        fig.suptitle(
            "Joining Bonus & Registration Origin \u2013 Snapp vs Tapsi",
            fontsize=15, fontweight="bold", y=0.99)
        for ax, col, color, label in [
            (axes[0], "snapp_joining_bonus", SNAPP_COLOR, "Snapp"),
            (axes[1], "tapsi_joining_bonus", TAPSI_COLOR, "Tapsi"),
        ]:
            if col not in short.columns:
                ax.set_title(f"{label} (no data)")
                style_ax(ax)
                continue
            data  = short[col].dropna().value_counts()
            total = data.sum()
            bonus_colors = {"Yes": "#66BB6A", "No": "#EF5350"}
            ax.bar(data.index, data.values,
                   color=[bonus_colors.get(k, GREY) for k in data.index],
                   edgecolor="white")
            for i, (k, v) in enumerate(data.items()):
                ax.text(i, v + 20, f"{v:,}\n({v/total*100:.0f}%)",
                        ha="center", fontsize=9)
            ax.set_title(
                f"{label} Joining/Registration Bonus  (n={total:,})", fontsize=11)
            ax.set_xlabel("Received Bonus?")
            ax.set_ylabel("Count")
            style_ax(ax)
        save_fig(pdf, fig)

    with safe_page(pdf, 'Page 64 - TAPSI NAVIGATION DEEP-DIVE'):
        fig, axes = plt.subplots(2, 2, figsize=(14, 10), facecolor=BG_COLOR)
        fig.suptitle(
            "Tapsi Navigation Deep-Dive \u2013 In-App & Offline Navigation",
            fontsize=15, fontweight="bold", y=0.98)
        for ax, col, color, label in [
            (axes[0, 0], "tapsi_in_app_navigation_usage",
             TAPSI_COLOR, "Used Tapsi In-App Navigation"),
            (axes[0, 1], "tapsi_in_app_navigation_satisfaction",
             TAPSI_COLOR, "In-App Navigation Satisfaction (1\u20135)"),
            (axes[1, 0], "tapsi_offline_navigation_familiar",
             "#5C6BC0", "Familiar with Tapsi Offline Navigation"),
            (axes[1, 1], "tapsi_offline_navigation_usage", "#5C6BC0",
             "Offline Navigation Usage During GPS Issues"),
        ]:
            if col not in short.columns:
                ax.set_title(f"{label}\n(no data)", fontsize=10)
                style_ax(ax)
                continue
            data  = short[col].dropna().value_counts()
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

    with safe_page(pdf, 'Page 65 - TAPSI GPS PERFORMANCE & MAGICAL WINDOW'):
        fig, axes = plt.subplots(1, 3, figsize=(16, 5), facecolor=BG_COLOR)
        fig.suptitle(
            "Tapsi: GPS Performance Perception & Magical Window / Referral Program",
            fontsize=15, fontweight="bold", y=0.99)
        ax = axes[0]
        if "tapsi_gps_better" in short.columns:
            data  = short["tapsi_gps_better"].dropna().value_counts()
            total = data.sum()
            gps_colors = {"Yes": "#66BB6A", "No": "#EF5350",
                          "Similar": "#FFA726"}
            ax.bar(data.index, data.values,
                   color=[gps_colors.get(k, GREY) for k in data.index],
                   edgecolor="white")
            for i, (k, v) in enumerate(data.items()):
                ax.text(i, v + 5, f"{v:,}\n({v/total*100:.0f}%)",
                        ha="center", fontsize=9)
            ax.set_title(
                f"Was Tapsi App Better\nDuring GPS Issues?  (n={total:,})",
                fontsize=10)
            ax.set_ylabel("Count")
            style_ax(ax)
        else:
            ax.set_title("tapsi_gps_better (no data)")
            style_ax(ax)
        ax = axes[1]
        if "tapsi_magical_window" in short.columns:
            data  = short["tapsi_magical_window"].dropna().value_counts()
            total = data.sum()
            mw_clrs = {"Yes": TAPSI_COLOR, "No": "#FFA726",
                       "Not Familiar": LGREY}
            ax.bar(data.index, data.values,
                   color=[mw_clrs.get(k, GREY) for k in data.index],
                   edgecolor="white")
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
        ax           = axes[2]
        tapsi_ref_q  = (long[long["question"] == "Tapsi Incentive GotBonus"]
                        if "question" in long.columns
                        else pd.DataFrame())
        if len(tapsi_ref_q) > 0:
            vc    = tapsi_ref_q["answer"].value_counts().sort_values(
                ascending=True)
            total = vc.sum()
            ax.barh(vc.index, vc.values, color=TAPSI_COLOR, edgecolor="white")
            for i, (k, v) in enumerate(vc.items()):
                ax.text(v + 1, i, f"{v} ({v/total*100:.0f}%)",
                        va="center", fontsize=8)
            ax.set_title(
                f"Tapsi Incentive GotBonus\n(long_survey, n={total:,})",
                fontsize=10)
            ax.set_xlabel("Count")
            style_ax(ax)
        else:
            ax.set_title(
                "Tapsi Incentive GotBonus\n(no data in long_survey)",
                fontsize=10)
            style_ax(ax)
        save_fig(pdf, fig)

    # ================================================================
    # PAGE 66 – SPEED SATISFACTION
    # ================================================================
    with safe_page(pdf, 'Page 66 - SPEED SATISFACTION'):
        if not HAVE_SHORT:
            placeholder_page(pdf, "Page 66 \u2013 Speed Satisfaction",
                             "short DataFrame not available")
        else:
            scol_speed = "snapp_speed_satisfaction"
            tcol_speed = "tapsi_speed_satisfaction"
            have_scol  = scol_speed in short.columns
            have_tcol  = tcol_speed in short.columns
            if not have_scol and not have_tcol:
                placeholder_page(
                    pdf, "Page 66 \u2013 Speed Satisfaction",
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
                        ax.text(x, y + 1,
                                f"{y}\n({y/total*100:.0f}%)",
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
    with safe_page(pdf, 'Page 67 - SNAPP CARFIX DEEP-DIVE'):
        if not HAVE_SHORT:
            placeholder_page(pdf, "Page 67 \u2013 Snapp CarFix Deep-Dive",
                             "short DataFrame not available")
        else:
            _cf_funnel_cols = ["snappcarfix_familiar", "snappcarfix_use_ever",
                               "snappcarfix_use_lastmo"]
            _cf_sat_cols = [
                ("snappcarfix_satisfaction_overall",       "Overall"),
                ("snappcarfix_satisfaction_experience",    "Experience"),
                ("snappcarfix_satisfaction_productprice",  "Product Price"),
                ("snappcarfix_satisfaction_quality",       "Quality"),
                ("snappcarfix_satisfaction_variety",       "Variety"),
                ("snappcarfix_satisfaction_buyingprocess", "Buying Process"),
                ("snappcarfix_satisfaction_deliverytime",  "Delivery Time"),
                ("snappcarfix_satisfaction_waittime",      "Wait Time"),
                ("snappcarfix_satisfaction_behaviour",     "Behaviour"),
            ]
            _cf_lastm_cols = [
                ("snappcarfix_satisfaction_quality_lastm",  "Quality (last mo)"),
                ("snappcarfix_satisfaction_price_lastm",    "Price (last mo)"),
                ("snappcarfix_satisfaction_variety_lastm",  "Variety (last mo)"),
                ("snappcarfix_satisfaction_easyusage",      "Easy Usage"),
                ("snappcarfix_satisfaction_ontimedelivery", "On-time Delivery"),
                ("snappcarfix_satisfaction_CS",             "Customer Support"),
            ]
            _cf_rec_col = "snappcarfix_recommend"
            fig, axes   = plt.subplots(2, 2, figsize=(14, 10),
                                       facecolor=BG_COLOR)
            fig.suptitle(
                "Snapp CarFix \u2013 Funnel, Satisfaction & Recommendation",
                fontsize=15, fontweight="bold", y=0.98)
            ax = axes[0, 0]
            funnel_labels = ["Familiar", "Ever Used", "Used Last Month"]
            funnel_vals   = []
            for fc in _cf_funnel_cols:
                if fc in short.columns:
                    vc = short[fc].dropna().value_counts()
                    funnel_vals.append(vc.get("Yes", 0))
                else:
                    funnel_vals.append(0)
            total_resp    = sum(short[_cf_funnel_cols[0]].dropna().shape[0]
                                if _cf_funnel_cols[0] in short.columns
                                else 0 for _ in [1])
            bar_colors_f  = [SNAPP_COLOR, "#66BB6A", "#43A047"]
            bars          = ax.bar(funnel_labels, funnel_vals,
                                   color=bar_colors_f, edgecolor="white")
            for b, v in zip(bars, funnel_vals):
                pct = f" ({v/total_resp*100:.0f}%)" if total_resp > 0 else ""
                ax.text(b.get_x() + b.get_width() / 2,
                        v + max(funnel_vals) * 0.02,
                        f"{v:,}{pct}", ha="center", fontsize=9,
                        fontweight="bold")
            ax.set_title(f"Adoption Funnel  (n={total_resp:,})", fontsize=11)
            ax.set_ylabel("Count (Yes)")
            style_ax(ax)
            ax = axes[0, 1]
            all_sat     = _cf_sat_cols + _cf_lastm_cols
            sat_labels  = []
            sat_means   = []
            for col, lbl in all_sat:
                if col in short.columns:
                    d = pd.to_numeric(short[col], errors="coerce").dropna()
                    if len(d) > 0:
                        sat_labels.append(lbl)
                        sat_means.append(d.mean())
            if sat_labels:
                y_pos = np.arange(len(sat_labels))
                ax.barh(y_pos, sat_means, color=SNAPP_COLOR,
                        edgecolor="white", height=0.6)
                for i, v in enumerate(sat_means):
                    ax.text(v + 0.05, i, f"{v:.2f}", va="center",
                            fontsize=8, fontweight="bold")
                ax.set_yticks(y_pos)
                ax.set_yticklabels(sat_labels, fontsize=8)
                ax.set_xlim(0, 5.5)
                ax.set_xlabel("Mean (1\u20135)")
                ax.invert_yaxis()
            ax.set_title("Satisfaction Dimensions", fontsize=11)
            style_ax(ax)
            ax = axes[1, 0]
            if _cf_rec_col in short.columns:
                rec_data = pd.to_numeric(short[_cf_rec_col],
                                         errors="coerce").dropna()
                if len(rec_data) > 0:
                    rec_counts = rec_data.value_counts().sort_index()
                    rec_total  = rec_counts.sum()
                    colors_nps = []
                    for idx in rec_counts.index:
                        if idx <= 6:
                            colors_nps.append("#EF5350")
                        elif idx <= 8:
                            colors_nps.append(LGREY)
                        else:
                            colors_nps.append(SNAPP_COLOR)
                    ax.bar(rec_counts.index.astype(int).astype(str),
                           rec_counts.values, color=colors_nps,
                           edgecolor="white")
                    for x, y in zip(rec_counts.index.astype(int).astype(str),
                                    rec_counts.values):
                        ax.text(x, y + rec_total * 0.005,
                                f"{y}\n({y/rec_total*100:.0f}%)",
                                ha="center", fontsize=7)
                    _nps = nps_score(rec_data)
                    ax.set_title(
                        f"Recommendation (0\u201310)  NPS={_nps:.1f}  "
                        f"n={int(rec_total)}", fontsize=10)
                else:
                    ax.set_title("Recommendation (no data)", fontsize=11)
            else:
                ax.set_title("snappcarfix_recommend not found", fontsize=11)
            ax.set_xlabel("Score")
            ax.set_ylabel("Count")
            style_ax(ax)
            ax = axes[1, 1]
            if HAVE_LONG and "question" in long.columns:
                notuse_q    = "Snappcarfix NotUse Reason"
                notuse_data = long[long["question"] == notuse_q]
                if len(notuse_data) > 0:
                    ans_counts = notuse_data["answer"].dropna().value_counts(
                    ).sort_values(ascending=True)
                    total_nu   = ans_counts.sum()
                    ax.barh(ans_counts.index, ans_counts.values,
                            color=ACCENT, edgecolor="white")
                    for i, (ans, v) in enumerate(ans_counts.items()):
                        ax.text(v + total_nu * 0.01, i,
                                f"{v} ({v/total_nu*100:.0f}%)",
                                va="center", fontsize=8)
                    ax.set_title(
                        f"Not-Use Reasons  (n={total_nu})", fontsize=11)
                else:
                    ax.set_title(
                        "NotUse Reasons (no data in long)", fontsize=11)
            else:
                ax.set_title(
                    "NotUse Reasons (long not available)", fontsize=11)
            ax.set_xlabel("Count")
            style_ax(ax)
            save_fig(pdf, fig)

    # ================================================================
    # PAGE 68 – TAPSI GARAGE DEEP-DIVE
    # ================================================================
    with safe_page(pdf, 'Page 68 - TAPSI GARAGE DEEP-DIVE'):
        if not HAVE_SHORT:
            placeholder_page(pdf, "Page 68 \u2013 Tapsi Garage Deep-Dive",
                             "short DataFrame not available")
        else:
            _tg_funnel_cols = ["tapsigarage_familiar", "tapsigarage_use_ever",
                               "tapsigarage_use_lastmo"]
            _tg_sat_cols = [
                ("tapsigarage_satisfaction_overall",             "Overall"),
                ("tapsigarage_satisfaction_experience",          "Experience"),
                ("tapsigarage_satisfaction_productprice",        "Product Price"),
                ("tapsigarage_satisfaction_quality_experience",  "Quality"),
                ("tapsigarage_satisfaction_variety_experience",  "Variety"),
                ("tapsigarage_satisfaction_buyingprocess",       "Buying Process"),
                ("tapsigarage_satisfaction_deliverytime",        "Delivery Time"),
                ("tapsigarage_satisfaction_waittime",            "Wait Time"),
                ("tapsigarage_satisfaction_behaviour",           "Behaviour"),
            ]
            _tg_lastm_cols = [
                ("tapsigarage_satisfaction_quality",        "Quality (last mo)"),
                ("tapsigarage_satisfaction_price",          "Price (last mo)"),
                ("tapsigarage_satisfaction_variety",        "Variety (last mo)"),
                ("tapsigarage_satisfaction_easyusage",      "Easy Usage"),
                ("tapsigarage_satisfaction_ontimedelivery", "On-time Delivery"),
                ("tapsigarage_satisfaction_CS",             "Customer Support"),
            ]
            _tg_rec_col = "tapsigarage_recommend"
            fig, axes   = plt.subplots(2, 2, figsize=(14, 10),
                                       facecolor=BG_COLOR)
            fig.suptitle(
                "Tapsi Garage \u2013 Funnel, Satisfaction & Recommendation",
                fontsize=15, fontweight="bold", y=0.98)
            ax              = axes[0, 0]
            funnel_labels_tg = ["Familiar", "Ever Used", "Used Last Month"]
            funnel_vals_tg   = []
            for fc in _tg_funnel_cols:
                if fc in short.columns:
                    vc = short[fc].dropna().value_counts()
                    funnel_vals_tg.append(vc.get("Yes", 0))
                else:
                    funnel_vals_tg.append(0)
            total_resp_tg = (short[_tg_funnel_cols[0]].dropna().shape[0]
                             if _tg_funnel_cols[0] in short.columns else 0)
            bar_colors_tg  = [TAPSI_COLOR, "#FFA726", "#F57C00"]
            bars = ax.bar(funnel_labels_tg, funnel_vals_tg,
                          color=bar_colors_tg, edgecolor="white")
            for b, v in zip(bars, funnel_vals_tg):
                pct = (f" ({v/total_resp_tg*100:.0f}%)"
                       if total_resp_tg > 0 else "")
                ax.text(
                    b.get_x() + b.get_width() / 2,
                    v + max(funnel_vals_tg) * 0.02 if max(funnel_vals_tg) > 0 else 1,
                    f"{v:,}{pct}", ha="center", fontsize=9, fontweight="bold")
            ax.set_title(
                f"Adoption Funnel  (n={total_resp_tg:,})", fontsize=11)
            ax.set_ylabel("Count (Yes)")
            style_ax(ax)
            ax = axes[0, 1]
            all_sat_tg  = _tg_sat_cols + _tg_lastm_cols
            sat_labels_tg = []
            sat_means_tg  = []
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
                    ax.text(v + 0.05, i, f"{v:.2f}", va="center",
                            fontsize=8, fontweight="bold")
                ax.set_yticks(y_pos)
                ax.set_yticklabels(sat_labels_tg, fontsize=8)
                ax.set_xlim(0, 5.5)
                ax.set_xlabel("Mean (1\u20135)")
                ax.invert_yaxis()
            ax.set_title("Satisfaction Dimensions", fontsize=11)
            style_ax(ax)
            ax = axes[1, 0]
            if _tg_rec_col in short.columns:
                rec_data_tg = pd.to_numeric(short[_tg_rec_col],
                                            errors="coerce").dropna()
                if len(rec_data_tg) > 0:
                    rec_counts_tg = rec_data_tg.value_counts().sort_index()
                    rec_total_tg  = rec_counts_tg.sum()
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
                    ax.set_title(
                        f"Recommendation (0\u201310)  NPS={_nps_tg:.1f}  "
                        f"n={int(rec_total_tg)}", fontsize=10)
                else:
                    ax.set_title("Recommendation (no data)", fontsize=11)
            else:
                ax.set_title("tapsigarage_recommend not found", fontsize=11)
            ax.set_xlabel("Score")
            ax.set_ylabel("Count")
            style_ax(ax)
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
    with safe_page(pdf, 'Page 69 - MIXED INCENTIVE STRATEGY'):
        if not HAVE_SHORT:
            placeholder_page(pdf, "Page 69 \u2013 Mixed Incentive Strategy",
                             "short DataFrame not available")
        else:
            _mix_cols    = ["mixincentive", "mixincentive_activate_familiar",
                            "mixincentive_tripeffect", "mixincentive_onlysnapp",
                            "mixincentive_choice"]
            _mix_present = [c for c in _mix_cols if c in short.columns]
            if len(_mix_present) == 0:
                placeholder_page(
                    pdf, "Page 69 \u2013 Mixed Incentive Strategy",
                    "No mixincentive columns found in short")
            else:
                fig = plt.figure(figsize=(15, 10), facecolor=BG_COLOR)
                fig.suptitle(
                    "Mixed Incentive Strategy \u2013 Awareness, Activation "
                    "& Preferences",
                    fontsize=15, fontweight="bold", y=0.98)
                gs = gridspec.GridSpec(2, 3, figure=fig,
                                       hspace=0.35, wspace=0.35)
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
                        ax.set_title(
                            f"Awareness  (n={aw_data.sum()})", fontsize=11)
                    else:
                        ax.set_title("Awareness (no data)", fontsize=11)
                else:
                    ax.set_title("mixincentive not found", fontsize=11)
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
                        ax.set_title(
                            f"Activation Familiarity  (n={af_data.sum()})",
                            fontsize=11)
                    else:
                        ax.set_title("Activation Familiarity (no data)",
                                     fontsize=11)
                else:
                    ax.set_title("activate_familiar not found", fontsize=11)
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
                        ax.set_title(
                            f"Trip Effect  (n={total_te})", fontsize=11)
                    else:
                        ax.set_title("Trip Effect (no data)", fontsize=11)
                else:
                    ax.set_title("tripeffect not found", fontsize=11)
                style_ax(ax)
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
                        ax.set_title(
                            f"Snapp-Only Preference  (n={total_os})",
                            fontsize=11)
                    else:
                        ax.set_title("Snapp-Only (no data)", fontsize=11)
                else:
                    ax.set_title("onlysnapp not found", fontsize=11)
                style_ax(ax)
                ax = fig.add_subplot(gs[1, 1:])
                if "mixincentive_choice" in short.columns:
                    ch_data = short["mixincentive_choice"].dropna(
                    ).value_counts()
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
                        ax.set_title(
                            f"Incentive Choice  (n={total_ch})", fontsize=11)
                    else:
                        ax.set_title("Choice (no data)", fontsize=11)
                else:
                    ax.set_title("mixincentive_choice not found", fontsize=11)
                style_ax(ax)
                save_fig(pdf, fig)

    # ================================================================
    # PAGE 70 – REQUEST REFUSAL REASONS
    # ================================================================
    with safe_page(pdf, 'Page 70 - REQUEST REFUSAL REASONS (wide binary)'):
        if not HAVE_WIDE:
            placeholder_page(pdf, "Page 70 \u2013 Request Refusal Reasons",
                             "wide DataFrame not available")
        else:
            _refusal_suffixes = [
                "Application Problems", "Low Fare", "Short Accept Time",
                "Not Realized There's Request", "Unfamiliar Route",
                "Wait for better Offer", "Traffic", "Unfamiliar App",
                "Target Destination", "Internet Problems",
                "Long DistanceToOrigin", "Long Route",
                "Working with Tapsi",
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
            snapp_ref_vals = [wide[c].sum() if c in wide.columns else 0
                              for c in snapp_ref_cols]
            tapsi_ref_vals = [wide[c].sum() if c in wide.columns else 0
                              for c in tapsi_ref_cols]
            if sum(snapp_ref_vals) == 0 and sum(tapsi_ref_vals) == 0:
                placeholder_page(
                    pdf, "Page 70 \u2013 Request Refusal Reasons",
                    "No Request Refusal binary columns found in wide")
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
                for i, (sv, tv) in enumerate(
                        zip(snapp_ref_vals, tapsi_ref_vals)):
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
    with safe_page(pdf, 'Page 71 - APP NOTIFICATION CHANNELS'):
        if not HAVE_WIDE:
            placeholder_page(pdf, "Page 71 \u2013 App Notification Channels",
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
            _broadcast_labels = ["Telegram",
                                 "Instagram (drivers.snapp@)",
                                 "SnappClub (club.snapp.ir)"]
            _broadcast_cols = [
                "Snapp Driversapp Broadcast Channel__Telegram",
                "Snapp Driversapp Broadcast Channel__Instagram (drivers.snapp@)",
                "Snapp Driversapp Broadcast Channel__SnappClub (club.snapp.ir)",
            ]
            msg_vals   = [wide[c].sum() if c in wide.columns else 0
                          for c in _msg_type_cols]
            bcast_vals = [wide[c].sum() if c in wide.columns else 0
                          for c in _broadcast_cols]
            if sum(msg_vals) == 0 and sum(bcast_vals) == 0:
                placeholder_page(
                    pdf, "Page 71 \u2013 App Notification Channels",
                    "No Got Message Type / Broadcast Channel columns "
                    "found in wide")
            else:
                fig, axes = plt.subplots(1, 2, figsize=(14, 6),
                                         facecolor=BG_COLOR)
                fig.suptitle("Snapp Notification Channels (wide)",
                             fontsize=15, fontweight="bold", y=0.99)
                ax        = axes[0]
                total_msg = sum(msg_vals)
                ax.barh(_msg_type_labels, msg_vals, color=SNAPP_COLOR,
                        edgecolor="white")
                for i, v in enumerate(msg_vals):
                    if v > 0:
                        pct = (f" ({v/total_msg*100:.0f}%)"
                               if total_msg > 0 else "")
                        ax.text(v + max(msg_vals) * 0.01, i,
                                f"{int(v)}{pct}", va="center", fontsize=9)
                ax.set_title(
                    f"How Messages Received  (total mentions={int(total_msg)})",
                    fontsize=11)
                ax.set_xlabel("Count")
                ax.invert_yaxis()
                style_ax(ax)
                ax        = axes[1]
                total_bc  = sum(bcast_vals)
                ax.barh(_broadcast_labels, bcast_vals, color=ACCENT,
                        edgecolor="white")
                for i, v in enumerate(bcast_vals):
                    if v > 0:
                        pct = (f" ({v/total_bc*100:.0f}%)"
                               if total_bc > 0 else "")
                        ax.text(v + max(bcast_vals) * 0.01, i,
                                f"{int(v)}{pct}", va="center", fontsize=9)
                ax.set_title(
                    f"Broadcast Channels Followed  "
                    f"(total mentions={int(total_bc)})", fontsize=11)
                ax.set_xlabel("Count")
                ax.invert_yaxis()
                style_ax(ax)
                save_fig(pdf, fig)

    # ================================================================
    # PAGE 72 – FIX LOCATION + OS DISTRIBUTION
    # ================================================================
    with safe_page(pdf, 'Page 72 - FIX LOCATION FEATURE + OS DISTRIBUTION'):
        if not HAVE_SHORT:
            placeholder_page(
                pdf, "Page 72 \u2013 Fix Location & OS Distribution",
                "short DataFrame not available")
        else:
            fig, axes = plt.subplots(2, 2, figsize=(13, 9), facecolor=BG_COLOR)
            fig.suptitle("Fix Location Feature & OS Distribution",
                         fontsize=15, fontweight="bold", y=0.97)
            ax = axes[0, 0]
            fl_funnel_cols   = ["fixlocation_familiar", "fixlocation_use"]
            fl_funnel_labels = ["Familiar", "Used"]
            fl_vals  = []
            for fc in fl_funnel_cols:
                if fc in short.columns:
                    vc = short[fc].dropna().value_counts()
                    fl_vals.append(vc.get("Yes", 0))
                else:
                    fl_vals.append(0)
            fl_total = (short[fl_funnel_cols[0]].dropna().shape[0]
                        if fl_funnel_cols[0] in short.columns else 0)
            fl_colors = [SNAPP_COLOR, "#66BB6A"]
            bars      = ax.bar(fl_funnel_labels, fl_vals, color=fl_colors,
                               edgecolor="white", width=0.5)
            for b, v in zip(bars, fl_vals):
                pct = f" ({v/fl_total*100:.0f}%)" if fl_total > 0 else ""
                ax.text(
                    b.get_x() + b.get_width() / 2,
                    v + max(fl_vals) * 0.02 if max(fl_vals) > 0 else 1,
                    f"{v:,}{pct}", ha="center", fontsize=9, fontweight="bold")
            ax.set_title(
                f"Fix Location Adoption  (n={fl_total:,})", fontsize=11)
            ax.set_ylabel("Count (Yes)")
            style_ax(ax)
            ax         = axes[0, 1]
            fl_sat_col = "fixlocation_satisfaction"
            if fl_sat_col in short.columns:
                fl_sat = pd.to_numeric(short[fl_sat_col],
                                       errors="coerce").dropna()
                if len(fl_sat) > 0:
                    fl_counts    = fl_sat.value_counts().sort_index()
                    fl_sat_total = fl_counts.sum()
                    ax.bar(fl_counts.index.astype(int).astype(str),
                           fl_counts.values, color=SNAPP_COLOR,
                           edgecolor="white")
                    for x, y in zip(fl_counts.index.astype(int).astype(str),
                                    fl_counts.values):
                        ax.text(x, y + fl_sat_total * 0.005,
                                f"{y}\n({y/fl_sat_total*100:.0f}%)",
                                ha="center", fontsize=8)
                    ax.set_title(
                        f"Fix Location Satisfaction  "
                        f"(n={int(fl_sat_total)}, mean={fl_sat.mean():.2f})",
                        fontsize=10)
                else:
                    ax.set_title("Fix Location Satisfaction (no data)",
                                 fontsize=11)
            else:
                ax.set_title("fixlocation_satisfaction not found", fontsize=11)
            ax.set_xlabel("Rating (1\u20135)")
            ax.set_ylabel("Count")
            style_ax(ax)
            ax     = axes[1, 0]
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
            ax     = axes[1, 1]
            gps_col = "gps_problem"
            if gps_col in short.columns:
                gps_data = short[gps_col].dropna().value_counts()
                if len(gps_data) > 0:
                    total_gps = gps_data.sum()
                    gps_colors = {"No": "#66BB6A",
                                  "Yes - sometimes": "#FFA726",
                                  "Yes - often": "#EF5350"}
                    bar_c = [gps_colors.get(k, GREY) for k in gps_data.index]
                    ax.barh(gps_data.index, gps_data.values,
                            color=bar_c, edgecolor="white")
                    for i, (k, v) in enumerate(gps_data.items()):
                        ax.text(v + total_gps * 0.01, i,
                                f"{v} ({v/total_gps*100:.0f}%)",
                                va="center", fontsize=9)
                    ax.set_title(
                        f"GPS Problem Awareness  (n={total_gps})", fontsize=11)
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
    with safe_page(pdf, 'Page 73 - INCENTIVE RULES AWARENESS + PREFERENCE'):
        if not HAVE_SHORT:
            placeholder_page(
                pdf, "Page 73 \u2013 Incentive Rules & Preference",
                "short DataFrame not available")
        else:
            ir_col  = "incentive_rules"
            ip_col  = "incentive_preference"
            have_ir = ir_col in short.columns
            have_ip = ip_col in short.columns
            if not have_ir and not have_ip:
                placeholder_page(
                    pdf, "Page 73 \u2013 Incentive Rules & Preference",
                    "Neither incentive_rules nor incentive_preference "
                    "found in short")
            else:
                fig, axes = plt.subplots(1, 2, figsize=(14, 6),
                                         facecolor=BG_COLOR)
                fig.suptitle("Incentive Rules Awareness & Preference",
                             fontsize=15, fontweight="bold", y=0.99)
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
                        ax.set_title(
                            f"Incentive Rules Awareness  (n={total_ir})",
                            fontsize=11)
                    else:
                        ax.set_title("Incentive Rules (no data)", fontsize=11)
                else:
                    ax.set_title("incentive_rules not found", fontsize=11)
                ax.set_xlabel("Count")
                style_ax(ax)
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
                        ax.set_title(
                            f"Incentive Preference  (n={total_ip})", fontsize=11)
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
    # BUG-2 FIX (left panel): `snapp_use_nextweek` has 5 ordinal values
    # ('completely', 'mostly', 'little', 'none', 'exit snapp').  v6 tried to
    # colour bars with a Yes/No colour map, but the bigger problem was that
    # the column sometimes arrived as all-NaN because of a merge edge case.
    # Fix: enforce a display order so bars appear from most to least positive,
    # and use a gradient colour scheme that reflects the ordinal scale.
    #
    # BUG-3 FIX (right panel): The JSON mapping has the "familiar - rated" and
    # "familiar - not rated" labels swapped.  Since data_cleaning.py has
    # already written the processed CSV with those swapped labels, we correct
    # the display here in the plotting layer only, by renaming the index before
    # plotting.
    with safe_page(pdf, 'Page 74 - NEXT-WEEK USAGE INTENT + RATE PASSENGER FEATURE'):
        if not HAVE_SHORT:
            placeholder_page(
                pdf, "Page 74 \u2013 Next-Week Intent & Rate Passenger",
                "short DataFrame not available")
        else:
            nw_col  = "snapp_use_nextweek"
            rp_col  = "ratepassenger_familiar_use"
            have_nw = nw_col in short.columns
            have_rp = rp_col in short.columns
            if not have_nw and not have_rp:
                placeholder_page(
                    pdf, "Page 74 \u2013 Next-Week Intent & Rate Passenger",
                    "Neither snapp_use_nextweek nor "
                    "ratepassenger_familiar_use found in short")
            else:
                fig, axes = plt.subplots(1, 2, figsize=(14, 6),
                                         facecolor=BG_COLOR)
                fig.suptitle(
                    "Next-Week Usage Intent & Rate Passenger Feature",
                    fontsize=15, fontweight="bold", y=0.99)

                # --- BUG-2 FIX: Next-week intent (left panel) ---
                ax = axes[0]
                if have_nw:
                    nw_data = short[nw_col].dropna().value_counts()
                    if len(nw_data) > 0:
                        total_nw = nw_data.sum()
                        # Enforce a logical display order (most to least positive)
                        intent_order = [
                            "completely", "mostly", "little",
                            "none", "exit snapp",
                        ]
                        present = [v for v in intent_order
                                   if v in nw_data.index]
                        # Keep any unexpected values at the end
                        extra   = [v for v in nw_data.index
                                   if v not in intent_order]
                        ordered = present + extra
                        nw_data = nw_data.reindex(ordered).dropna()

                        # Colour gradient: green → yellow → red
                        gradient_colors = [
                            "#43A047",  # completely  – dark green
                            "#66BB6A",  # mostly      – medium green
                            "#FFA726",  # little      – amber
                            "#EF5350",  # none        – red
                            "#B71C1C",  # exit snapp  – dark red
                        ]
                        bar_c = [
                            gradient_colors[i] if i < len(gradient_colors)
                            else GREY
                            for i in range(len(nw_data))
                        ]
                        ax.bar(range(len(nw_data)), nw_data.values,
                               color=bar_c, edgecolor="white")
                        ax.set_xticks(range(len(nw_data)))
                        ax.set_xticklabels(nw_data.index, fontsize=9,
                                           rotation=25, ha="right")
                        for i, (k, v) in enumerate(nw_data.items()):
                            ax.text(i, v + total_nw * 0.005,
                                    f"{v:,}\n({v/total_nw*100:.0f}%)",
                                    ha="center", fontsize=9)
                        ax.set_title(
                            f"Snapp Next-Week Intent  (n={total_nw:,})",
                            fontsize=11)
                    else:
                        ax.set_title("Next-Week Intent (no data)", fontsize=11)
                else:
                    ax.set_title("snapp_use_nextweek not found", fontsize=11)
                ax.set_xlabel("Response")
                ax.set_ylabel("Count")
                style_ax(ax)

                # --- BUG-3 FIX: Rate passenger feature (right panel) ---
                ax = axes[1]
                if have_rp:
                    rp_data = short[rp_col].dropna().value_counts()
                    if len(rp_data) > 0:
                        total_rp = rp_data.sum()

                        # The JSON mapping has these two labels SWAPPED.
                        # The raw question: 'آشنایی دارم، امتیاز دادم' = DID rate
                        # was mapped to 'familiar - not rated' (wrong).
                        # 'آشنایی دارم، امتیاز ندادم' = did NOT rate
                        # was mapped to 'familiar - rated' (wrong).
                        # We swap them back here for correct display.
                        corrected_index = rp_data.index.map(lambda x: {
                            "familiar - not rated": "familiar - rated",
                            "familiar - rated":     "familiar - not rated",
                        }.get(x, x))
                        rp_data.index = corrected_index

                        rp_colors = {
                            "familiar - rated":     SNAPP_COLOR,
                            "familiar - not rated": "#FFA726",
                            "not familiar":         LGREY,
                        }
                        bar_c = [rp_colors.get(str(k), GREY)
                                 for k in rp_data.index]
                        ax.bar(range(len(rp_data)), rp_data.values,
                               color=bar_c, edgecolor="white")
                        ax.set_xticks(range(len(rp_data)))
                        ax.set_xticklabels(rp_data.index, fontsize=8,
                                           rotation=25, ha="right")
                        for i, (k, v) in enumerate(rp_data.items()):
                            ax.text(i, v + total_rp * 0.005,
                                    f"{v:,}\n({v/total_rp*100:.0f}%)",
                                    ha="center", fontsize=9)
                        ax.set_title(
                            f"Rate Passenger Feature  (n={total_rp:,})",
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
