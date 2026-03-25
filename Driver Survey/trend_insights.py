"""
Driver Survey – Trend & Pattern Insights
=========================================
Generates a multi-page PDF with timeline-based insights mined from the
6 processed CSVs.  Focuses on patterns that evolve OVER TIME (yearweek),
complementing the existing v6 snapshot report.

Analyses included:
  1.  Satisfaction Rolling Average & Structural Breaks
  2.  Snapp–Tapsi Satisfaction Gap Timeline
  3.  NPS Decomposition (Promoters / Passives / Detractors stacked area)
  4.  Incentive ROI – Monetary Spend vs Satisfaction Correlation Over Time
  5.  Cohort Honeymoon Decay – Satisfaction by Tenure Cohort × Week
  6.  Commission-Free Ride Share Over Time
  7.  Joint Driver Rate vs Platform Satisfaction Gap
  8.  Ride Refusal Reasons Heatmap (time × reason)
  9.  Customer Support Category Shift (time × category)
  10. City-Level Satisfaction Divergence (std dev + top/bottom cities)
  11. Leading Indicator: Lagged Cross-Correlation Matrix
  12. Incentive Funnel Over Time (awareness → participation → satisfaction)
  13. Navigation App Adoption Timeline
  14. Satisfaction Heatmap – City × Week

Usage:
    python trend_insights.py
"""

from contextlib import contextmanager
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.gridspec as gridspec
import matplotlib.patches as mpatches
import matplotlib.colors as mcolors
import os
import warnings
import numpy as np
import pandas as pd
import json

matplotlib.use("Agg")
warnings.filterwarnings("ignore")

# ============================================================================
# CONFIGURATION
# ============================================================================
BASE = r"D:\Work\Driver Survey\processed"
SHORT_MAIN = os.path.join(BASE, "short_survey_main.csv")
SHORT_RARE = os.path.join(BASE, "short_survey_rare.csv")
WIDE_MAIN = os.path.join(BASE, "wide_survey_main.csv")
WIDE_RARE = os.path.join(BASE, "wide_survey_rare.csv")
LONG_MAIN = os.path.join(BASE, "long_survey_main.csv")
LONG_RARE = os.path.join(BASE, "long_survey_rare.csv")
OUTPUT_PDF = os.path.join(BASE, "trend_insights.pdf")

SNAPP = "#00C853"
TAPSI = "#FF6D00"
ACCENT = "#1565C0"
ACCENT2 = "#7B1FA2"
GREY = "#9E9E9E"
LGREY = "#E0E0E0"
BG = "#FAFAFA"
MIN_WEEK = 100
MAPPING_JSON = r"D:\Work\Driver Survey\DataSources\column_rename_mapping.json"
MIN_RESPONSES_COL = 20    # min non-NaN per week for a column to be "active"
MIN_ACTIVE_WEEKS_PCT = 0.15  # column must be active in >=15% of weeks to plot

# ============================================================================
# HELPERS
# ============================================================================


def new_fig(title, figsize=(14, 7)):
    fig, ax = plt.subplots(figsize=figsize, facecolor=BG)
    ax.set_facecolor(BG)
    fig.suptitle(title, fontsize=14, fontweight="bold", y=0.98)
    return fig, ax


def style_ax(ax):
    ax.set_facecolor(BG)
    ax.spines[["top", "right"]].set_visible(False)


def save_fig(pdf, fig):
    fig.tight_layout(rect=[0, 0, 1, 0.93])
    pdf.savefig(fig, facecolor=BG)
    plt.close(fig)


def nps_score(s):
    s = s.dropna()
    if len(s) == 0:
        return np.nan
    return (s >= 9).sum() / len(s) * 100 - (s <= 6).sum() / len(s) * 100


def placeholder_page(pdf, title, reason="Data not available"):
    fig, ax = plt.subplots(figsize=(14, 7), facecolor=BG)
    ax.set_facecolor(BG)
    fig.suptitle(title, fontsize=14, fontweight="bold", y=0.97)
    ax.text(0.5, 0.5, reason, transform=ax.transAxes, fontsize=16,
            color=GREY, ha="center", va="center", fontweight="bold",
            bbox=dict(boxstyle="round,pad=1", facecolor=LGREY,
                      edgecolor=GREY, alpha=0.5))
    ax.axis("off")
    save_fig(pdf, fig)


@contextmanager
def safe_page(pdf, title):
    n_before = set(plt.get_fignums())
    try:
        yield
    except (KeyError, TypeError, ValueError) as e:
        for fn in plt.get_fignums():
            if fn not in n_before:
                plt.close(plt.figure(fn))
        print(f"[WARN] Skipping '{title}': {e}")
        placeholder_page(pdf, title, f"Skipped – missing column: {e}")


def safe_col(df, col):
    """Return df[col] if it exists, else NaN series."""
    if col in df.columns:
        return pd.to_numeric(df[col], errors="coerce")
    return pd.Series(np.nan, index=df.index, name=col)


def shade_nan_gaps(ax, x_positions, values, color="#FFD54F", alpha=0.18,
                   label_once="no data"):
    """Add translucent vertical bands wherever `values` is NaN.

    Parameters
    ----------
    ax : matplotlib Axes
    x_positions : array-like of numeric x-coordinates (same length as values)
    values : array-like (NaN = gap)
    color, alpha : shading style
    label_once : legend label (only added for the first gap band)
    """
    values = np.asarray(values, dtype=float)
    x_positions = np.asarray(x_positions, dtype=float)
    is_nan = np.isnan(values)
    if not is_nan.any():
        return
    labelled = False
    in_gap = False
    gap_start = None
    for i in range(len(values)):
        if is_nan[i] and not in_gap:
            gap_start = x_positions[i] - 0.5
            in_gap = True
        elif not is_nan[i] and in_gap:
            gap_end = x_positions[i] - 0.5
            lbl = label_once if not labelled else None
            ax.axvspan(gap_start, gap_end, color=color, alpha=alpha, label=lbl,
                       zorder=0)
            labelled = True
            in_gap = False
    if in_gap:  # gap extends to the end
        lbl = label_once if not labelled else None
        ax.axvspan(gap_start, x_positions[-1] + 0.5, color=color, alpha=alpha,
                   label=lbl, zorder=0)


def plot_gapped_line(ax, x, y, **kwargs):
    """Plot a line that breaks (disconnects) at NaN values instead of
    interpolating through them.  Uses masked arrays so matplotlib leaves
    a visible gap.
    """
    y = np.asarray(y, dtype=float)
    masked_y = np.ma.masked_invalid(y)
    ax.plot(x, masked_y, **kwargs)


def _rare_week_mask(df, col, weeks):
    """Return a boolean Series indexed by `weeks`: True if the column was
    actually asked that week.

    Checks both:
      - At least 5 non-NaN responses (standard check)
      - At least one row with value == 1 for binary columns (catches 0-fill
        from weeks when question wasn't asked but column is filled with 0)
    """
    grp = df.groupby("yearweek")[col]
    counts = grp.apply(lambda s: s.notna().sum()).reindex(weeks, fill_value=0)
    has_data = counts >= 5
    # For binary (0/1) columns, also require at least one positive response
    # to distinguish "not asked (all 0)" from "asked, nobody selected"
    col_vals = df[col].dropna()
    is_binary = col_vals.isin([0, 1]).all() and len(col_vals) > 0
    if is_binary:
        has_positive = grp.apply(lambda s: (s == 1).any()).reindex(weeks, fill_value=False)
        has_data = has_data & has_positive
    return has_data


def _mask_rare_heatmap(heat_df, raw_df, cols, weeks):
    """For a heatmap DataFrame (weeks × reasons), replace values with NaN
    for weeks when the underlying question wasn't asked.

    Detection uses TWO methods (catches both NaN-style and 0-fill-style gaps):
      1. Weeks where ALL columns are NaN (or have <5 non-NaN responses)
      2. Weeks where the SUM of means across all binary columns is < 5%
         (catches 0-filled binary cols from wide data where question wasn't asked)
    """
    # Method 1: NaN-based detection (for columns that preserve NaN)
    active_nan = raw_df.groupby("yearweek")[cols].apply(
        lambda g: g.notna().any(axis=1).sum()
    ).reindex(weeks, fill_value=0)
    # Method 2: Sum-of-means detection (for 0-filled binary columns)
    # If the question wasn't asked, all binary cols will be 0 → sum ≈ 0
    weekly_means = raw_df.groupby("yearweek")[cols].mean().reindex(weeks, fill_value=0)
    row_sum = weekly_means.sum(axis=1) * 100  # as percentage
    # A week is inactive if EITHER method flags it
    inactive = (active_nan < 5) | (row_sum < 5)
    heat_df.loc[heat_df.index.isin(inactive[inactive].index)] = np.nan
    return heat_df


# --- Column frequency metadata from mapping JSON ---
_col_freq = {}
if os.path.isfile(MAPPING_JSON):
    with open(MAPPING_JSON, "r", encoding="utf-8") as _f:
        _mapping_data = json.load(_f)
    for _key, _info in _mapping_data.items():
        _col_freq[_key] = _info.get("freq", "unknown")
    print(f"  Loaded {len(_col_freq)} column freq entries from mapping JSON")


def col_is_periodic(col_name):
    """Return True if the column is not asked every survey week."""
    base = col_name.split("__")[0] if "__" in col_name else col_name
    return _col_freq.get(base, "unknown") not in ("always",)


def active_weeks_for_col(df, col, weeks, min_n=None):
    """Return list of weeks where col was actually asked (>=min_n non-NaN)."""
    if min_n is None:
        min_n = MIN_RESPONSES_COL
    counts = df.groupby("yearweek")[col].apply(lambda s: s.notna().sum())
    counts = counts.reindex(weeks, fill_value=0)
    return [w for w in weeks if counts.get(w, 0) >= min_n]


def col_has_enough_data(df, col, weeks, min_pct=None):
    """Return True if col has data in enough weeks to be worth plotting."""
    if min_pct is None:
        min_pct = MIN_ACTIVE_WEEKS_PCT
    active = active_weeks_for_col(df, col, weeks)
    return len(active) / max(len(weeks), 1) >= min_pct


def mask_inactive_weeks(series, df, col, weeks, min_n=None):
    """Set weeks where col wasn't actively asked to NaN in a weekly Series."""
    active = set(active_weeks_for_col(df, col, weeks, min_n))
    result = series.copy()
    for w in result.index:
        if w not in active:
            result.at[w] = np.nan
    return result


# ============================================================================
# DATA LOADING
# ============================================================================
print("Loading data...")


def safe_load(path):
    if not os.path.isfile(path):
        print(f"  [WARN] Not found: {path}")
        return None
    return pd.read_csv(path, encoding="utf-8-sig", low_memory=False)


def merge_main_rare(main_df, rare_df, key="recordID"):
    if main_df is None:
        return rare_df
    if rare_df is None:
        return main_df
    main_cols = set(main_df.columns)
    rare_extra = [c for c in rare_df.columns if c not in main_cols or c == key]
    return main_df.merge(rare_df[rare_extra], on=key, how="left")


short_main = safe_load(SHORT_MAIN)
short_rare = safe_load(SHORT_RARE)
wide_main = safe_load(WIDE_MAIN)
wide_rare = safe_load(WIDE_RARE)
long_main = safe_load(LONG_MAIN)
long_rare = safe_load(LONG_RARE)

short = merge_main_rare(short_main, short_rare)
wide = merge_main_rare(wide_main, wide_rare)
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
    print("[WARN] No short data – report will be empty.")

# Remap old snapp_age / tapsi_age values to canonical tenure buckets
_tenure_remap = {
    "less_than_1_month": "less_than_3_months",
    "1_to_3_months": "less_than_3_months",
    "6_to_12_months": "6_months_to_1_year",
    "1_to_2_years": "1_to_3_years",
    "2_to_3_years": "1_to_3_years",
    "3_to_4_years": "3_to_5_years",
    "more_than_4_years": "5_to_7_years",
}
for df in [short, wide]:
    if df is None:
        continue
    for age_col in ["snapp_age", "tapsi_age"]:
        if age_col in df.columns:
            df[age_col] = df[age_col].replace(_tenure_remap)

# Rename CS columns for compatibility
for df in [short, wide]:
    if df is None:
        continue
    rn = {}
    if "snapp_CS" in df.columns:
        rn["snapp_CS"] = "snapp_customer_support"
    if "tapsi_CS_" in df.columns:
        rn["tapsi_CS_"] = "tapsi_customer_support"
    if rn:
        df.rename(columns=rn, inplace=True)

# Build yearweek
dfs_all = [short]
if wide is not None:
    dfs_all.append(wide)
if long is not None:
    dfs_all.append(long)
for df in dfs_all:
    df["datetime_parsed"] = pd.to_datetime(df["datetime"], errors="coerce")
    df["year"] = df["datetime_parsed"].dt.year
    df["weeknumber"] = pd.to_numeric(df.get("weeknumber", pd.Series(dtype=float)),
                                     errors="coerce")
    df["yearweek"] = (
        (df["year"] % 100) * 100 + df["weeknumber"]
    ).where(df["weeknumber"].notna() & df["year"].notna()).astype("Int64")

# Drop thin weeks
wc = short.groupby("yearweek").size()
valid_weeks = wc[wc >= MIN_WEEK].index
short = short[short["yearweek"].isin(valid_weeks)].copy()
if wide is not None:
    wide = wide[wide["yearweek"].isin(valid_weeks)].copy()
if long is not None:
    long = long[long["yearweek"].isin(valid_weeks)].copy()

# Driver type
if "tapsi_ride" in short.columns:
    short["driver_type"] = np.where(
        short["tapsi_ride"] == 0, "Snapp Exclusive", "Joint")
if wide is not None and "tapsi_ride" in wide.columns:
    wide["driver_type"] = np.where(
        wide["tapsi_ride"] == 0, "Snapp Exclusive", "Joint")

short.sort_values("yearweek", inplace=True)

weeks_sorted = sorted(short["yearweek"].dropna().unique())
week_labels = [str(w) for w in weeks_sorted]
n_weeks = len(weeks_sorted)

HAVE_SHORT = len(short) > 0
HAVE_WIDE = wide is not None and len(wide) > 0
HAVE_LONG = long is not None and len(long) > 0

print(f"Ready: {len(short):,} rows, {n_weeks} weeks, "
      f"wide={'yes' if HAVE_WIDE else 'no'}, long={'yes' if HAVE_LONG else 'no'}")

# ============================================================================
# PDF GENERATION
# ============================================================================
with PdfPages(OUTPUT_PDF) as pdf:

    # ================================================================
    # PAGE 1 – SATISFACTION ROLLING AVERAGE & STRUCTURAL BREAKS
    # ================================================================
    with safe_page(pdf, "Page 1 – Satisfaction Trend & Structural Breaks"):
        sat_cols = {
            "Snapp Fare":     "snapp_fare_satisfaction",
            "Snapp Income":   "snapp_income_satisfaction",
            "Snapp Req-Count": "snapp_req_count_satisfaction",
            "Tapsi Fare":     "tapsi_fare_satisfaction",
            "Tapsi Income":   "tapsi_income_satisfaction",
            "Tapsi Req-Count": "tapsi_req_count_satisfaction",
        }
        available = {k: v for k, v in sat_cols.items() if v in short.columns}
        weekly = short.groupby("yearweek")[list(available.values())].mean()
        weekly = weekly.reindex(weeks_sorted)

        fig, axes = plt.subplots(2, 1, figsize=(
            16, 10), facecolor=BG, sharex=True)
        fig.suptitle("Satisfaction Trends with 4-Week Rolling Average & Structural Breaks",
                     fontsize=14, fontweight="bold", y=0.98)

        x_pos = np.arange(len(weeks_sorted))

        # Top panel: Snapp
        ax = axes[0]
        snapp_keys = [k for k in available if k.startswith("Snapp")]
        colors_s = [SNAPP, "#00E676", "#69F0AE"]
        gap_shaded_s = False
        for i, k in enumerate(snapp_keys):
            col = available[k]
            raw = weekly[col]
            roll = raw.rolling(4, min_periods=4).mean()
            plot_gapped_line(ax, x_pos, raw.values, alpha=0.3,
                             color=colors_s[i % len(colors_s)], linewidth=1)
            plot_gapped_line(ax, x_pos, roll.values,
                             color=colors_s[i % len(colors_s)],
                             linewidth=2.5, label=k)
            # Mark structural breaks (>2σ from rolling mean)
            residual = raw - roll
            sigma = residual.std()
            breaks = residual.abs() > 2 * sigma
            if breaks.any():
                ax.scatter(x_pos[breaks], raw[breaks],
                           color="red", zorder=5, s=60, marker="v", alpha=0.8)
            if not gap_shaded_s:
                shade_nan_gaps(ax, x_pos, raw.values)
                gap_shaded_s = True
        ax.set_xticks(x_pos)
        ax.set_xticklabels(week_labels, fontsize=7, rotation=45)
        ax.set_ylabel("Mean Satisfaction (1–5)")
        ax.set_ylim(1, 5)
        ax.legend(fontsize=8, ncol=3, loc="lower left", frameon=False)
        ax.set_title("Snapp – raw (faded) + 4-week rolling (solid), ▼ = structural break",
                     fontsize=10)
        style_ax(ax)

        # Bottom panel: Tapsi
        ax = axes[1]
        tapsi_keys = [k for k in available if k.startswith("Tapsi")]
        colors_t = [TAPSI, "#FF9100", "#FFAB40"]
        gap_shaded_t = False
        for i, k in enumerate(tapsi_keys):
            col = available[k]
            raw = weekly[col]
            roll = raw.rolling(4, min_periods=4).mean()
            plot_gapped_line(ax, x_pos, raw.values, alpha=0.3,
                             color=colors_t[i % len(colors_t)], linewidth=1)
            plot_gapped_line(ax, x_pos, roll.values,
                             color=colors_t[i % len(colors_t)],
                             linewidth=2.5, label=k)
            residual = raw - roll
            sigma = residual.std()
            breaks = residual.abs() > 2 * sigma
            if breaks.any():
                ax.scatter(x_pos[breaks], raw[breaks],
                           color="red", zorder=5, s=60, marker="v", alpha=0.8)
            if not gap_shaded_t:
                shade_nan_gaps(ax, x_pos, raw.values)
                gap_shaded_t = True
        ax.set_xticks(x_pos)
        ax.set_xticklabels(week_labels, fontsize=7, rotation=45)
        ax.set_ylabel("Mean Satisfaction (1–5)")
        ax.set_ylim(1, 5)
        ax.set_xlabel("Year-Week")
        ax.legend(fontsize=8, ncol=3, loc="lower left", frameon=False)
        ax.set_title("Tapsi", fontsize=10)
        style_ax(ax)
        save_fig(pdf, fig)

    # ================================================================
    # PAGE 2 – SNAPP vs TAPSI SATISFACTION GAP OVER TIME
    # ================================================================
    with safe_page(pdf, "Page 2 – Satisfaction Gap Timeline"):
        pairs = [
            ("snapp_fare_satisfaction",      "tapsi_fare_satisfaction",      "Fare"),
            ("snapp_income_satisfaction",
             "tapsi_income_satisfaction",    "Income"),
            ("snapp_req_count_satisfaction",
             "tapsi_req_count_satisfaction", "Req-Count"),
        ]
        avail_pairs = [(s, t, lbl) for s, t, lbl in pairs
                       if s in short.columns and t in short.columns]
        weekly_gap = pd.DataFrame(index=weeks_sorted)
        for scol, tcol, lbl in avail_pairs:
            g = short.groupby("yearweek").agg(
                s=(scol, "mean"), t=(tcol, "mean"))
            weekly_gap[lbl] = g["s"] - g["t"]
        weekly_gap = weekly_gap.reindex(weeks_sorted)

        fig, ax = new_fig("Snapp – Tapsi Satisfaction Gap Over Time  (positive = Snapp higher)",
                          figsize=(16, 7))
        colors = [ACCENT, ACCENT2, GREY]
        for i, col in enumerate(weekly_gap.columns):
            vals = weekly_gap[col]
            ax.plot(weekly_gap.index.astype(str), vals, marker="o", markersize=4,
                    linewidth=2, label=col, color=colors[i % len(colors)])
            # Rolling average
            roll = vals.rolling(4, min_periods=2).mean()
            ax.plot(weekly_gap.index.astype(str), roll, linewidth=3, alpha=0.4,
                    color=colors[i % len(colors)])
        ax.axhline(0, color="black", linewidth=1, linestyle="--", alpha=0.5)
        ax.fill_between(range(len(weeks_sorted)),
                        [0]*len(weeks_sorted),
                        weekly_gap.mean(axis=1).reindex(weeks_sorted).values,
                        alpha=0.08, color=SNAPP,
                        where=weekly_gap.mean(axis=1).reindex(weeks_sorted).values >= 0)
        ax.fill_between(range(len(weeks_sorted)),
                        [0]*len(weeks_sorted),
                        weekly_gap.mean(axis=1).reindex(weeks_sorted).values,
                        alpha=0.08, color=TAPSI,
                        where=weekly_gap.mean(axis=1).reindex(weeks_sorted).values < 0)
        ax.set_xticks(range(len(weeks_sorted)))
        ax.set_xticklabels(week_labels, rotation=45, fontsize=8)
        ax.set_ylabel("Gap (Snapp mean – Tapsi mean)")
        ax.set_xlabel("Year-Week")
        ax.legend(frameon=False, fontsize=9)
        style_ax(ax)
        # Annotate average gap
        avg_gap = weekly_gap.mean().mean()
        ax.text(0.98, 0.02, f"Overall avg gap: {avg_gap:+.3f}",
                transform=ax.transAxes, ha="right", fontsize=10,
                color=SNAPP if avg_gap > 0 else TAPSI, fontweight="bold")
        save_fig(pdf, fig)

    # ================================================================
    # PAGE 3 – NPS DECOMPOSITION OVER TIME (STACKED AREA)
    # ================================================================
    with safe_page(pdf, "Page 3 – NPS Decomposition Timeline"):
        nps_cols = {
            "Snapp": "snapp_recommend",
            "Tapsi": "tapsidriver_tapsi_recommend",
        }
        avail_nps = {k: v for k, v in nps_cols.items() if v in short.columns}

        fig, axes = plt.subplots(1, len(avail_nps), figsize=(16, 7),
                                 facecolor=BG, sharey=True)
        if len(avail_nps) == 1:
            axes = [axes]
        fig.suptitle("NPS Decomposition – Promoters / Passives / Detractors Over Time",
                     fontsize=14, fontweight="bold", y=0.98)

        for ax, (platform, col) in zip(axes, avail_nps.items()):
            # NPS columns are from short_rare → periodical.
            # Only include weeks where the question was actually asked.
            active_mask = _rare_week_mask(short, col, weeks_sorted)
            active_weeks = [
                w for w in weeks_sorted if active_mask.get(w, False)]

            if not active_weeks:
                ax.text(0.5, 0.5, "No data (periodical question)",
                        transform=ax.transAxes, ha="center", fontsize=14, color=GREY)
                color_plt = SNAPP if platform == "Snapp" else TAPSI
                ax.set_title(f"{platform}", fontsize=12,
                             color=color_plt, fontweight="bold")
                style_ax(ax)
                continue

            pct_data = []
            nps_vals = []
            active_labels = []
            for w in active_weeks:
                wk = short[short["yearweek"] == w][col].dropna()
                n = len(wk)
                if n == 0:
                    continue
                prom = (wk >= 9).sum() / n * 100
                det = (wk <= 6).sum() / n * 100
                pas = 100 - prom - det
                pct_data.append((prom, pas, det))
                nps_vals.append(prom - det)
                active_labels.append(str(w))

            if not pct_data:
                ax.text(0.5, 0.5, "Insufficient data",
                        transform=ax.transAxes, ha="center", fontsize=14, color=GREY)
                style_ax(ax)
                continue

            prom_arr = [p[0] for p in pct_data]
            pas_arr = [p[1] for p in pct_data]
            det_arr = [p[2] for p in pct_data]
            x = np.arange(len(pct_data))
            ax.stackplot(x, det_arr, pas_arr, prom_arr,
                         colors=["#EF5350", "#BDBDBD", "#66BB6A"],
                         labels=[
                             "Detractors (0-6)", "Passives (7-8)", "Promoters (9-10)"],
                         alpha=0.8)
            # Overlay NPS line
            ax2 = ax.twinx()
            ax2.plot(x, nps_vals, color="black", linewidth=2.5, marker="o",
                     markersize=3, label="NPS", zorder=5)
            ax2.set_ylabel("NPS", fontsize=10)
            ax2.spines[["top"]].set_visible(False)
            color_plt = SNAPP if platform == "Snapp" else TAPSI
            ax.set_title(f"{platform}  ({len(active_labels)} active weeks)", fontsize=12,
                         color=color_plt, fontweight="bold")
            tick_step = max(1, len(x) // 10)
            ax.set_xticks(x[::tick_step])
            ax.set_xticklabels(active_labels[::tick_step],
                               rotation=45, fontsize=8)
            ax.set_ylabel("% of Respondents")
            ax.set_xlabel("Year-Week")
            if ax == axes[0]:
                ax.legend(loc="upper left", fontsize=7, frameon=False)
                ax2.legend(loc="upper right", fontsize=7, frameon=False)
            style_ax(ax)
        save_fig(pdf, fig)

    # ================================================================
    # PAGE 4 – INCENTIVE ROI: SPEND vs SATISFACTION CORRELATION
    # ================================================================
    with safe_page(pdf, "Page 4 – Incentive ROI Over Time"):
        fig, axes = plt.subplots(1, 2, figsize=(16, 7), facecolor=BG)
        fig.suptitle("Incentive ROI – Does Higher Spending Improve Satisfaction?",
                     fontsize=14, fontweight="bold", y=0.98)

        for ax, (plat, inc_col, sat_col, color) in zip(axes, [
            ("Snapp", "snapp_incentive", "snapp_overall_satisfaction_snapp", SNAPP),
            ("Tapsi", "tapsi_incentive", "tapsi_overall_incentive_satisfaction", TAPSI),
        ]):
            if inc_col not in short.columns or sat_col not in short.columns:
                ax.text(0.5, 0.5, "Data not available", transform=ax.transAxes,
                        ha="center", fontsize=14, color=GREY)
                ax.set_title(plat)
                style_ax(ax)
                continue
            # Check if either column has enough data to plot
            if not col_has_enough_data(short, sat_col, weeks_sorted):
                ax.text(0.5, 0.5, f"Insufficient data\n({sat_col} active in too few weeks)",
                        transform=ax.transAxes, ha="center", fontsize=12, color=GREY)
                ax.set_title(plat)
                style_ax(ax)
                continue
            wk = short.groupby("yearweek").agg(
                inc=(inc_col, "mean"), sat=(sat_col, "mean")).reindex(weeks_sorted)
            # Mask weeks where periodic columns weren't asked
            if col_is_periodic(sat_col):
                wk["sat"] = mask_inactive_weeks(wk["sat"], short, sat_col, weeks_sorted)
            if col_is_periodic(inc_col):
                wk["inc"] = mask_inactive_weeks(wk["inc"], short, inc_col, weeks_sorted)
            # Drop NaN rows (weeks where data is missing)
            wk_valid = wk.dropna()
            if len(wk_valid) < 3:
                ax.text(0.5, 0.5, f"Too few active weeks ({len(wk_valid)})",
                        transform=ax.transAxes, ha="center", fontsize=12, color=GREY)
                ax.set_title(plat)
                style_ax(ax)
                continue
            # Normalize time for color gradient
            norm = plt.Normalize(0, len(wk_valid)-1)
            cmap = plt.cm.viridis
            sc = ax.scatter(wk_valid["inc"] / 1e6, wk_valid["sat"],
                            c=np.arange(len(wk_valid)), cmap=cmap, s=80,
                            edgecolors="white", linewidth=0.5, zorder=5)
            # Trend line
            valid = wk_valid
            if len(valid) > 2:
                z = np.polyfit(valid["inc"] / 1e6, valid["sat"], 1)
                p = np.poly1d(z)
                x_line = np.linspace(valid["inc"].min() / 1e6,
                                     valid["inc"].max() / 1e6, 50)
                ax.plot(x_line, p(x_line), "--",
                        color=color, linewidth=2, alpha=0.7)
                corr = valid["inc"].corr(valid["sat"])
                ax.text(0.05, 0.95, f"r = {corr:.3f}", transform=ax.transAxes,
                        fontsize=11, fontweight="bold",
                        color="green" if corr > 0.3 else ("red" if corr < -0.3 else GREY))
            ax.set_xlabel("Avg Incentive (M Rials)")
            ax.set_ylabel("Avg Satisfaction (1–5)")
            ax.set_title(
                f"{plat}  ({len(valid)} active weeks, color = time)", fontsize=10)
            plt.colorbar(sc, ax=ax, label="Week index", shrink=0.6)
            style_ax(ax)
        save_fig(pdf, fig)

    # ================================================================
    # PAGE 5 – COHORT HONEYMOON DECAY
    # ================================================================
    with safe_page(pdf, "Page 5 – Cohort Honeymoon Decay"):
        tenure_order = ["less_than_3_months", "3_to_6_months", "6_months_to_1_year",
                        "1_to_3_years", "3_to_5_years", "5_to_7_years", "more_than_7_years"]
        tenure_labels = {"less_than_3_months": "<3m", "3_to_6_months": "3-6m",
                         "6_months_to_1_year": "6m-1y", "1_to_3_years": "1-3y",
                         "3_to_5_years": "3-5y", "5_to_7_years": "5-7y",
                         "more_than_7_years": ">7y"}
        # snapp_overall_satisfaction_snapp is periodical (mostly NaN).
        # Prefer snapp_fare_satisfaction which is always-asked.
        sat_col = "snapp_fare_satisfaction"
        if sat_col not in short.columns:
            sat_col = "snapp_overall_satisfaction_snapp"
        # Final check: if chosen column has <20% non-NaN, try the other
        if sat_col in short.columns:
            fill_rate = short[sat_col].notna().mean()
            if fill_rate < 0.2:
                alt = ("snapp_fare_satisfaction" if sat_col != "snapp_fare_satisfaction"
                       else "snapp_overall_satisfaction_snapp")
                if alt in short.columns and short[alt].notna().mean() > fill_rate:
                    sat_col = alt

        fig, ax = new_fig(
            "Honeymoon Decay – Satisfaction by Driver Tenure Cohort Over Time",
            figsize=(16, 8))
        cmap = plt.cm.coolwarm_r
        avail_tenures = [
            t for t in tenure_order if t in short["snapp_age"].dropna().unique()]
        x_pos = np.arange(len(weeks_sorted))
        gap_shaded = False
        plotted = 0
        for i, tenure in enumerate(avail_tenures):
            cohort = short[short["snapp_age"] == tenure]
            # Skip very small cohorts (< 50 total responses)
            if len(cohort) < 50:
                continue
            wk_sat = cohort.groupby("yearweek")[
                sat_col].mean().reindex(weeks_sorted)
            # Mask weeks with too few responses for this cohort
            cohort_wk_ct = cohort.groupby("yearweek")[sat_col].apply(
                lambda s: s.notna().sum()).reindex(weeks_sorted, fill_value=0)
            wk_sat[cohort_wk_ct < 10] = np.nan
            roll = wk_sat.rolling(3, min_periods=3).mean()
            c = cmap(i / max(len(avail_tenures)-1, 1))
            plot_gapped_line(ax, x_pos, roll.values, linewidth=2.5,
                             label=tenure_labels.get(tenure, tenure), color=c,
                             marker="o", markersize=3)
            if not gap_shaded:
                shade_nan_gaps(ax, x_pos, wk_sat.values)
                gap_shaded = True
            plotted += 1
        ax.set_xticks(x_pos)
        ax.set_xticklabels(week_labels, rotation=45, fontsize=7)
        ax.set_ylabel("Mean Satisfaction (1–5)")
        ax.set_xlabel("Year-Week")
        ax.set_ylim(1, 5)
        ax.legend(title="Tenure", fontsize=8, title_fontsize=9,
                  loc="lower left", frameon=False, ncol=4)
        style_ax(ax)
        # Annotate insight
        if len(avail_tenures) >= 2:
            newest = short[short["snapp_age"] ==
                           avail_tenures[0]][sat_col].mean()
            oldest = short[short["snapp_age"] ==
                           avail_tenures[-1]][sat_col].mean()
            diff = newest - oldest
            ax.text(0.98, 0.02,
                    f"Newest vs oldest cohort gap: {diff:+.2f}",
                    transform=ax.transAxes, ha="right", fontsize=10,
                    fontweight="bold", color=ACCENT)
        save_fig(pdf, fig)

    # ================================================================
    # PAGE 6 – COMMISSION-FREE RIDE SHARE OVER TIME
    # ================================================================
    with safe_page(pdf, "Page 6 – Commission-Free Ride Share Over Time"):
        fig, axes = plt.subplots(1, 2, figsize=(16, 7), facecolor=BG)
        fig.suptitle("Commission-Free Rides as % of Total – Does It Erode Revenue?",
                     fontsize=14, fontweight="bold", y=0.98)

        for ax, (plat, ride_col, cf_col, sat_col, color) in zip(axes, [
            ("Snapp", "snapp_ride", "snapp_commfree",
             "snapp_fare_satisfaction", SNAPP),
            ("Tapsi", "tapsi_ride", "tapsi_commfree",
             "tapsi_fare_satisfaction", TAPSI),
        ]):
            if ride_col not in short.columns or cf_col not in short.columns:
                ax.text(0.5, 0.5, "Data not available", transform=ax.transAxes,
                        ha="center", fontsize=14, color=GREY)
                ax.set_title(plat)
                style_ax(ax)
                continue
            wk = short.groupby("yearweek").agg(
                rides=(ride_col, "mean"),
                cf=(cf_col, "mean"),
            ).reindex(weeks_sorted)
            wk["cf_pct"] = (wk["cf"] / wk["rides"].replace(0, np.nan)) * 100
            wk["cf_pct"] = wk["cf_pct"].clip(upper=100)  # cap at 100%

            ax.bar(np.arange(len(weeks_sorted)), wk["cf_pct"], color=color,
                   alpha=0.6, edgecolor="white", label="CF %")
            ax.set_ylabel("Commission-Free %", color=color)
            ax.set_ylim(0, min(wk["cf_pct"].max() * 1.3, 110)
                        if wk["cf_pct"].max() > 0 else 10)
            ax.set_xticks(range(0, len(weeks_sorted),
                          max(1, len(weeks_sorted)//10)))
            ax.set_xticklabels(
                [week_labels[i] for i in range(0, len(weeks_sorted),
                                               max(1, len(weeks_sorted)//10))],
                rotation=45, fontsize=8)

            # Overlay satisfaction
            if sat_col in short.columns:
                ax2 = ax.twinx()
                sat_wk = short.groupby("yearweek")[
                    sat_col].mean().reindex(weeks_sorted)
                ax2.plot(np.arange(len(weeks_sorted)), sat_wk.values,
                         color=ACCENT, linewidth=2.5, marker="s", markersize=3,
                         label="Fare Satisfaction")
                ax2.set_ylabel("Fare Satisfaction (1–5)", color=ACCENT)
                ax2.set_ylim(1, 5)
                ax2.spines[["top"]].set_visible(False)
                ax2.legend(loc="upper right", fontsize=8, frameon=False)
            ax.set_title(plat, fontsize=12, fontweight="bold", color=color)
            ax.set_xlabel("Year-Week")
            ax.legend(loc="upper left", fontsize=8, frameon=False)
            style_ax(ax)
        save_fig(pdf, fig)

    # ================================================================
    # PAGE 7 – JOINT DRIVER RATE vs SATISFACTION GAP
    # ================================================================
    with safe_page(pdf, "Page 7 – Joint Driver Rate vs Satisfaction Gap"):
        fig, ax = new_fig(
            "Joint Driver Rate & Snapp–Tapsi Fare Satisfaction Gap",
            figsize=(16, 7))

        if "active_joint" in short.columns:
            wk_joint = short.groupby("yearweek")["active_joint"].mean().reindex(
                weeks_sorted) * 100
            ax.bar(np.arange(len(weeks_sorted)), wk_joint.values,
                   color=ACCENT, alpha=0.35, label="% Joint Drivers")
            ax.set_ylabel("% Joint Drivers", color=ACCENT)

        ax2 = ax.twinx()
        if ("snapp_fare_satisfaction" in short.columns and
                "tapsi_fare_satisfaction" in short.columns):
            gap = (short.groupby("yearweek")["snapp_fare_satisfaction"].mean() -
                   short.groupby("yearweek")["tapsi_fare_satisfaction"].mean()
                   ).reindex(weeks_sorted)
            ax2.plot(np.arange(len(weeks_sorted)), gap.values,
                     color=SNAPP, linewidth=2.5, marker="o", markersize=4,
                     label="Fare Sat Gap (S−T)")
            ax2.axhline(0, color=GREY, linewidth=0.8, linestyle="--")
            ax2.set_ylabel("Satisfaction Gap (Snapp − Tapsi)")
            ax2.legend(loc="upper right", fontsize=9, frameon=False)
            ax2.spines[["top"]].set_visible(False)

        ax.set_xticks(range(0, len(weeks_sorted),
                      max(1, len(weeks_sorted)//10)))
        ax.set_xticklabels(
            [week_labels[i] for i in range(0, len(weeks_sorted),
                                           max(1, len(weeks_sorted)//10))],
            rotation=45, fontsize=8)
        ax.set_xlabel("Year-Week")
        ax.legend(loc="upper left", fontsize=9, frameon=False)
        style_ax(ax)
        save_fig(pdf, fig)

    # ================================================================
    # PAGE 8 – RIDE REFUSAL REASONS HEATMAP (TIME × REASON)
    # ================================================================
    with safe_page(pdf, "Page 8 – Ride Refusal Reasons Heatmap"):
        fig, axes = plt.subplots(2, 1, figsize=(16, 12), facecolor=BG)
        fig.suptitle("Ride Refusal Reasons Over Time  (% of respondents per week)",
                     fontsize=14, fontweight="bold", y=0.98)

        for ax_idx, (prefix, title, color_map) in enumerate([
            ("Snapp Ride Refusal Reasons__", "Snapp", "Greens"),
            ("Tapsi Ride Refusal Reasons__", "Tapsi", "Oranges"),
        ]):
            ax = axes[ax_idx]
            if wide is None:
                ax.text(0.5, 0.5, "Wide data not available", transform=ax.transAxes,
                        ha="center", fontsize=14, color=GREY)
                ax.set_title(title)
                style_ax(ax)
                continue
            refusal_cols = [c for c in wide.columns if c.startswith(prefix)]
            if not refusal_cols:
                ax.text(0.5, 0.5, "No refusal columns found", transform=ax.transAxes,
                        ha="center", fontsize=14, color=GREY)
                ax.set_title(title)
                style_ax(ax)
                continue
            # Build heatmap data: weeks × reasons
            heat = wide.groupby("yearweek")[
                refusal_cols].mean().reindex(weeks_sorted) * 100
            # Mask weeks when the question wasn't asked (rare/periodical)
            heat = _mask_rare_heatmap(heat, wide, refusal_cols, weeks_sorted)
            heat.columns = [c.replace(prefix, "") for c in heat.columns]
            # Drop reasons with <2% average or active in too few weeks
            col_active_pct = heat.notna().mean()
            heat = heat.loc[:, (heat.mean(skipna=True) > 2) & (col_active_pct >= MIN_ACTIVE_WEEKS_PCT)]
            # Also check if heatmap has enough active weeks overall
            active_row_count = heat.notna().any(axis=1).sum()
            if heat.empty or active_row_count < 3:
                ax.text(0.5, 0.5, f"Insufficient data\n(active in {active_row_count} weeks)",
                        transform=ax.transAxes, ha="center", fontsize=14, color=GREY)
                ax.set_title(f"{title} Ride Refusal Reasons", fontsize=11, fontweight="bold")
                ax.axis("off")
                continue
            # Use a colormap that shows NaN as light grey
            cmap_obj = plt.cm.get_cmap(color_map).copy()
            cmap_obj.set_bad(color="#F5F5F5")
            im = ax.imshow(np.ma.masked_invalid(heat.T.values),
                           aspect="auto", cmap=cmap_obj,
                           interpolation="nearest")
            ax.set_yticks(range(len(heat.columns)))
            ax.set_yticklabels(heat.columns, fontsize=8)
            ax.set_xticks(range(0, len(weeks_sorted),
                          max(1, len(weeks_sorted)//12)))
            ax.set_xticklabels(
                [week_labels[i] for i in range(0, len(weeks_sorted),
                                               max(1, len(weeks_sorted)//12))],
                rotation=45, fontsize=8)
            ax.set_title(f"{title} Ride Refusal Reasons", fontsize=11,
                         fontweight="bold")
            plt.colorbar(im, ax=ax, label="% of drivers", shrink=0.7)
        save_fig(pdf, fig)

    # ================================================================
    # PAGE 9 – CUSTOMER SUPPORT CATEGORY SHIFT
    # ================================================================
    with safe_page(pdf, "Page 9 – CS Category Shift Over Time"):
        fig, axes = plt.subplots(2, 1, figsize=(16, 12), facecolor=BG)
        fig.suptitle("Customer Support Category Distribution Over Time",
                     fontsize=14, fontweight="bold", y=0.98)

        for ax_idx, (prefix, title, cmap_name) in enumerate([
            ("Snapp Customer Support Category__", "Snapp CS Categories", "Greens"),
            ("Tapsi Customer Support Category__",
             "Tapsi CS Categories", "Oranges"),
        ]):
            ax = axes[ax_idx]
            if wide is None:
                ax.text(0.5, 0.5, "Wide data not available", transform=ax.transAxes,
                        ha="center", fontsize=14, color=GREY)
                ax.set_title(title)
                style_ax(ax)
                continue
            cs_cols = [c for c in wide.columns if c.startswith(prefix)]
            if not cs_cols:
                ax.text(0.5, 0.5, "No CS category columns found", transform=ax.transAxes,
                        ha="center", fontsize=14, color=GREY)
                ax.set_title(title)
                style_ax(ax)
                continue
            heat = wide.groupby("yearweek")[
                cs_cols].mean().reindex(weeks_sorted) * 100
            # Mask weeks when the question wasn't asked (rare/periodical)
            heat = _mask_rare_heatmap(heat, wide, cs_cols, weeks_sorted)
            heat.columns = [c.replace(prefix, "") for c in heat.columns]
            # Drop categories with <1% average or active in too few weeks
            col_active_pct = heat.notna().mean()
            heat = heat.loc[:, (heat.mean(skipna=True) > 1) & (col_active_pct >= MIN_ACTIVE_WEEKS_PCT)]
            active_row_count = heat.notna().any(axis=1).sum()
            if heat.empty or active_row_count < 3:
                ax.text(0.5, 0.5, f"Insufficient data\n(active in {active_row_count} weeks)",
                        transform=ax.transAxes, ha="center", fontsize=14, color=GREY)
                ax.set_title(title, fontsize=11, fontweight="bold")
                ax.axis("off")
                continue
            cmap_obj = plt.cm.get_cmap(cmap_name).copy()
            cmap_obj.set_bad(color="#F5F5F5")
            im = ax.imshow(np.ma.masked_invalid(heat.T.values),
                           aspect="auto", cmap=cmap_obj,
                           interpolation="nearest")
            ax.set_yticks(range(len(heat.columns)))
            ax.set_yticklabels(heat.columns, fontsize=8)
            ax.set_xticks(range(0, len(weeks_sorted),
                          max(1, len(weeks_sorted)//12)))
            ax.set_xticklabels(
                [week_labels[i] for i in range(0, len(weeks_sorted),
                                               max(1, len(weeks_sorted)//12))],
                rotation=45, fontsize=8)
            ax.set_title(title, fontsize=11, fontweight="bold")
            plt.colorbar(im, ax=ax, label="% of drivers", shrink=0.7)
        save_fig(pdf, fig)

    # ================================================================
    # PAGE 10 – CITY-LEVEL SATISFACTION DIVERGENCE
    # ================================================================
    with safe_page(pdf, "Page 10 – City Satisfaction Divergence"):
        sat_col = "snapp_fare_satisfaction"
        if sat_col not in short.columns:
            raise KeyError(sat_col)

        top_cities = short["city"].value_counts().head(10).index.tolist()
        city_df = short[short["city"].isin(top_cities)]

        fig, axes = plt.subplots(2, 1, figsize=(16, 10), facecolor=BG)
        fig.suptitle("City-Level Satisfaction Divergence Over Time",
                     fontsize=14, fontweight="bold", y=0.98)

        # Top: std dev of city means per week
        ax = axes[0]
        city_week_sat = city_df.groupby(["yearweek", "city"])[
            sat_col].mean().unstack()
        city_week_sat = city_week_sat.reindex(weeks_sorted)
        week_std = city_week_sat.std(axis=1)
        ax.fill_between(np.arange(len(weeks_sorted)), week_std.values,
                        alpha=0.3, color=ACCENT)
        ax.plot(np.arange(len(weeks_sorted)), week_std.values,
                color=ACCENT, linewidth=2)
        roll_std = week_std.rolling(4, min_periods=2).mean()
        ax.plot(np.arange(len(weeks_sorted)), roll_std.values,
                color="red", linewidth=2.5, linestyle="--", label="4-wk rolling avg")
        ax.set_ylabel("Std Dev of City-Level Satisfaction")
        ax.set_title(
            "Cross-City Satisfaction Spread  (higher = more divergence)", fontsize=10)
        ax.legend(frameon=False, fontsize=9)
        ax.set_xticks(range(0, len(weeks_sorted),
                      max(1, len(weeks_sorted)//10)))
        ax.set_xticklabels(
            [week_labels[i] for i in range(0, len(weeks_sorted),
                                           max(1, len(weeks_sorted)//10))],
            rotation=45, fontsize=8)
        style_ax(ax)

        # Bottom: spaghetti plot of top 5 cities
        ax = axes[1]
        top5 = short["city"].value_counts().head(5).index.tolist()
        city_colors = plt.cm.Set2(np.linspace(0, 1, len(top5)))
        for i, city in enumerate(top5):
            city_sat = city_df[city_df["city"] == city].groupby("yearweek")[
                sat_col].mean()
            city_sat = city_sat.reindex(weeks_sorted)
            roll = city_sat.rolling(3, min_periods=3).mean()
            plot_gapped_line(ax, np.arange(len(weeks_sorted)), roll.values,
                             linewidth=2, label=city, color=city_colors[i],
                             marker="o", markersize=3)
        shade_nan_gaps(ax, np.arange(len(weeks_sorted)),
                       city_df.groupby("yearweek")[sat_col].mean().reindex(
                           weeks_sorted).values)
        ax.set_ylabel("Mean Fare Satisfaction (1–5)")
        ax.set_xlabel("Year-Week")
        ax.set_title(
            "Top 5 Cities – Fare Satisfaction Trend (3-wk rolling)", fontsize=10)
        ax.legend(frameon=False, fontsize=8, ncol=5, loc="lower left")
        ax.set_xticks(range(0, len(weeks_sorted),
                      max(1, len(weeks_sorted)//10)))
        ax.set_xticklabels(
            [week_labels[i] for i in range(0, len(weeks_sorted),
                                           max(1, len(weeks_sorted)//10))],
            rotation=45, fontsize=8)
        style_ax(ax)
        save_fig(pdf, fig)

    # ================================================================
    # PAGE 11 – LEADING INDICATOR: LAGGED CROSS-CORRELATIONS
    # ================================================================
    with safe_page(pdf, "Page 11 – Leading Indicator Correlations"):
        # Build weekly metric series
        metric_cols = {
            "Snapp Incentive (Rials)": "snapp_incentive",
            "Snapp CF Rides": "snapp_commfree",
            "Snapp Fare Sat": "snapp_fare_satisfaction",
            "Snapp Income Sat": "snapp_income_satisfaction",
            "Snapp Req-Count Sat": "snapp_req_count_satisfaction",
            "Snapp Overall Sat": "snapp_overall_satisfaction_snapp",
            "Joint Rate": "active_joint",
        }
        # Only include metrics that have enough data (skip periodic w/ too few weeks)
        available_metrics = {k: v for k, v in metric_cols.items()
                             if v in short.columns and col_has_enough_data(short, v, weeks_sorted, min_pct=0.5)}
        weekly_metrics = pd.DataFrame(index=weeks_sorted)
        for label, col in available_metrics.items():
            wm = short.groupby("yearweek")[col].mean().reindex(weeks_sorted)
            # Mask inactive weeks for periodic columns
            if col_is_periodic(col):
                wm = mask_inactive_weeks(wm, short, col, weeks_sorted)
            weekly_metrics[label] = wm

        if weekly_metrics.shape[1] < 3:
            raise ValueError("Not enough metrics for correlation analysis")

        # Compute lagged correlations: metric at week N vs satisfaction at week N+lag
        lags = [0, 1, 2, 3, 4]
        target = "Snapp Fare Sat"
        if target not in weekly_metrics.columns:
            target = list(weekly_metrics.columns)[-1]
        predictors = [c for c in weekly_metrics.columns if c != target]

        fig, ax = new_fig(
            f"Lagged Cross-Correlations: Metric at Week N → '{target}' at Week N+lag",
            figsize=(14, 7))
        x = np.arange(len(lags))
        width = 0.8 / max(len(predictors), 1)
        bar_colors = plt.cm.tab10(np.linspace(0, 0.8, len(predictors)))

        for i, pred in enumerate(predictors):
            corrs = []
            for lag in lags:
                if lag == 0:
                    corrs.append(weekly_metrics[pred].corr(
                        weekly_metrics[target]))
                else:
                    corrs.append(weekly_metrics[pred].iloc[:-lag].reset_index(drop=True)
                                 .corr(weekly_metrics[target].iloc[lag:].reset_index(drop=True)))
            ax.bar(x + i * width, corrs, width, label=pred, color=bar_colors[i],
                   edgecolor="white")

        ax.set_xticks(x + width * len(predictors) / 2)
        ax.set_xticklabels([f"Lag {l}" for l in lags])
        ax.set_ylabel("Pearson Correlation")
        ax.axhline(0, color=GREY, linewidth=0.8, linestyle="--")
        ax.legend(fontsize=7, ncol=2, frameon=False, loc="upper right")
        style_ax(ax)
        ax.set_xlabel("Lag (weeks)")
        save_fig(pdf, fig)

    # ================================================================
    # PAGE 12 – INCENTIVE FUNNEL OVER TIME
    # ================================================================
    with safe_page(pdf, "Page 12 – Incentive Funnel Over Time"):
        funnel_cols = {
            "Got Message": "snapp_gotmessage_text_incentive",
            "Participation": "snapp_incentive_participation",
            "Incentive Sat": "snapp_overall_incentive_satisfaction",
        }
        avail_funnel = {k: v for k, v in funnel_cols.items()
                        if v in short.columns and col_has_enough_data(short, v, weeks_sorted)}

        fig, ax = new_fig("Snapp Incentive Funnel – Awareness → Participation → Satisfaction",
                          figsize=(16, 7))
        funnel_colors = [SNAPP, ACCENT, ACCENT2, TAPSI]
        x_pos = np.arange(len(weeks_sorted))
        gap_shaded_funnel = False
        for i, (label, col) in enumerate(avail_funnel.items()):
            if label == "Got Message":
                wk = short.groupby("yearweek").apply(
                    lambda g: (g[col].dropna() == "Yes").mean() * 100
                    if col in g.columns else np.nan
                ).reindex(weeks_sorted)
            elif label == "Participation":
                wk = short.groupby("yearweek").apply(
                    lambda g: (g[col].dropna() == "Yes").mean() * 100
                    if col in g.columns else np.nan
                ).reindex(weeks_sorted)
            else:
                wk = short.groupby("yearweek")[
                    col].mean().reindex(weeks_sorted)
                wk = wk * 20  # 1-5 scale → 20-100

            # Mask weeks where column wasn't asked (some "always" cols
            # still have gaps in practice due to survey changes)
            wk = mask_inactive_weeks(wk, short, col, weeks_sorted)

            roll = wk.rolling(3, min_periods=2).mean()
            plot_gapped_line(ax, x_pos, roll.values,
                             linewidth=2.5, label=label, color=funnel_colors[i],
                             marker="o", markersize=3)
            ax.fill_between(x_pos, np.nan_to_num(roll.values, nan=0),
                            alpha=0.1, color=funnel_colors[i],
                            where=~np.isnan(roll.values))
            if not gap_shaded_funnel:
                shade_nan_gaps(ax, x_pos, wk.values)
                gap_shaded_funnel = True

        ax.set_ylabel("% or Scaled Score")
        ax.set_xlabel("Year-Week")
        ax.set_xticks(range(0, len(weeks_sorted),
                      max(1, len(weeks_sorted)//10)))
        ax.set_xticklabels(
            [week_labels[i] for i in range(0, len(weeks_sorted),
                                           max(1, len(weeks_sorted)//10))],
            rotation=45, fontsize=8)
        ax.legend(frameon=False, fontsize=9)
        style_ax(ax)
        save_fig(pdf, fig)

    # ================================================================
    # PAGE 13 – NAVIGATION APP ADOPTION TIMELINE
    # ================================================================
    with safe_page(pdf, "Page 13 – Navigation App Adoption Timeline"):
        # Try multiple naming conventions for navigation columns
        nav_prefixes = {
            "Google Map": ["Navigation Used__Google Map", "Navigation Familiarity__Google Map"],
            "Waze":       ["Navigation Used__Waze", "Navigation Familiarity__Waze"],
            "Neshan":     ["Navigation Used__Neshan", "Navigation Familiarity__Neshan"],
            "Balad":      ["Navigation Used__Balad", "Navigation Familiarity__Balad"],
        }
        if wide is None:
            raise ValueError("Wide data not available")

        avail_nav = {}
        for app, candidates in nav_prefixes.items():
            for cand in candidates:
                if cand in wide.columns:
                    avail_nav[app] = cand
                    break
        if not avail_nav:
            # Fallback: find any Navigation binary column
            for c in wide.columns:
                if "Navigation" in c and "__" in c and any(
                        k in c for k in ["Familiarity", "Used", "Installed"]):
                    short_name = c.split("__")[-1]
                    avail_nav[short_name] = c
            if not avail_nav:
                raise KeyError("No navigation columns found")

        # Determine if data is "Familiarity" or "Used"
        _nav_source = "Familiarity" if any("Familiarity" in v for v in avail_nav.values()) else "Usage"
        fig, ax = new_fig(f"Navigation App {_nav_source} Over Time  (% per week)",
                          figsize=(16, 7))
        nav_colors = {"Google Map": "#4285F4", "Waze": "#33CCFF",
                      "Neshan": "#E91E63", "Balad": "#FF9800"}
        default_colors = plt.cm.Set2(np.linspace(0, 1, len(avail_nav)))

        gap_shaded_nav = False
        for i, (app, col) in enumerate(avail_nav.items()):
            wk = wide.groupby("yearweek")[
                col].mean().reindex(weeks_sorted) * 100
            # Navigation is periodical (rare) — convert structural zeros to NaN
            active = _rare_week_mask(wide, col, weeks_sorted)
            wk[~active] = np.nan
            roll = wk.rolling(3, min_periods=3).mean()
            c = nav_colors.get(app, default_colors[i])
            x_pos = np.arange(len(weeks_sorted))
            plot_gapped_line(ax, x_pos, roll.values,
                             linewidth=2.5, marker="o", markersize=4,
                             label=app, color=c)
            if not gap_shaded_nav:
                shade_nan_gaps(ax, x_pos, wk.values)
                gap_shaded_nav = True
            # Annotate latest non-NaN value
            last_val = roll.dropna(
            ).iloc[-1] if not roll.dropna().empty else np.nan
            if not np.isnan(last_val):
                last_idx = roll.dropna().index[-1]
                pos = list(weeks_sorted).index(
                    last_idx) if last_idx in weeks_sorted else len(weeks_sorted)-1
                ax.annotate(f"{last_val:.0f}%",
                            (pos, last_val),
                            textcoords="offset points", xytext=(10, 0),
                            fontsize=9, fontweight="bold", color=c)

        ax.set_ylabel("% of Drivers Using")
        ax.set_xlabel("Year-Week")
        ax.set_xticks(range(0, len(weeks_sorted),
                      max(1, len(weeks_sorted)//10)))
        ax.set_xticklabels(
            [week_labels[i] for i in range(0, len(weeks_sorted),
                                           max(1, len(weeks_sorted)//10))],
            rotation=45, fontsize=8)
        ax.legend(frameon=False, fontsize=10)
        style_ax(ax)
        save_fig(pdf, fig)

    # ================================================================
    # PAGE 14 – SATISFACTION HEATMAP: CITY × WEEK
    # ================================================================
    with safe_page(pdf, "Page 14 – Satisfaction Heatmap City × Week"):
        sat_col = "snapp_fare_satisfaction"
        if sat_col not in short.columns:
            raise KeyError(sat_col)

        top_cities = short["city"].value_counts().head(15).index.tolist()
        city_df = short[short["city"].isin(top_cities)]
        pivot = city_df.groupby(["city", "yearweek"])[sat_col].mean().unstack()
        pivot = pivot.reindex(columns=weeks_sorted)
        # Sort cities by overall mean
        pivot = pivot.loc[pivot.mean(
            axis=1).sort_values(ascending=False).index]

        fig, ax = plt.subplots(figsize=(18, 8), facecolor=BG)
        ax.set_facecolor(BG)
        fig.suptitle("Snapp Fare Satisfaction – Top 15 Cities × Year-Week",
                     fontsize=14, fontweight="bold", y=0.98)

        im = ax.imshow(pivot.values, aspect="auto", cmap="RdYlGn",
                       vmin=1, vmax=5, interpolation="nearest")
        ax.set_yticks(range(len(pivot.index)))
        ax.set_yticklabels(pivot.index, fontsize=9)
        step = max(1, len(weeks_sorted) // 15)
        ax.set_xticks(range(0, len(weeks_sorted), step))
        ax.set_xticklabels(
            [week_labels[i] for i in range(0, len(weeks_sorted), step)],
            rotation=45, fontsize=8)
        ax.set_xlabel("Year-Week")
        plt.colorbar(im, ax=ax, label="Satisfaction (1–5)", shrink=0.6)

        # Annotate cells with values
        for i in range(pivot.shape[0]):
            for j in range(pivot.shape[1]):
                val = pivot.iloc[i, j]
                if not np.isnan(val):
                    color = "white" if val < 2.5 or val > 4.0 else "black"
                    ax.text(j, i, f"{val:.1f}", ha="center", va="center",
                            fontsize=5.5, color=color)
        save_fig(pdf, fig)

    # ================================================================
    # PAGE 15 – COOPERATION TYPE MIX OVER TIME
    # ================================================================
    with safe_page(pdf, "Page 15 – Cooperation Type & Demographics Shift"):
        fig, axes = plt.subplots(2, 2, figsize=(16, 10), facecolor=BG)
        fig.suptitle("Driver Demographics Evolution Over Time",
                     fontsize=14, fontweight="bold", y=0.98)

        # Panel 1: Full-Time vs Part-Time mix
        ax = axes[0, 0]
        if "cooperation_type" in short.columns:
            coop = short.groupby("yearweek")["cooperation_type"].apply(
                lambda x: (x == "Full-Time").mean() * 100).reindex(weeks_sorted)
            roll = coop.rolling(3, min_periods=1).mean()
            ax.fill_between(np.arange(len(weeks_sorted)), roll.values,
                            alpha=0.3, color=ACCENT)
            ax.plot(np.arange(len(weeks_sorted)), roll.values,
                    color=ACCENT, linewidth=2)
            ax.set_ylabel("% Full-Time")
        ax.set_title("Full-Time Driver Share", fontsize=10)
        ax.set_xticks(range(0, len(weeks_sorted),
                      max(1, len(weeks_sorted)//8)))
        ax.set_xticklabels(
            [week_labels[i] for i in range(0, len(weeks_sorted),
                                           max(1, len(weeks_sorted)//8))],
            rotation=45, fontsize=7)
        style_ax(ax)

        # Panel 2: Joint driver rate
        ax = axes[0, 1]
        if "active_joint" in short.columns:
            joint = short.groupby("yearweek")["active_joint"].mean().reindex(
                weeks_sorted) * 100
            roll = joint.rolling(3, min_periods=1).mean()
            ax.fill_between(np.arange(len(weeks_sorted)), roll.values,
                            alpha=0.3, color=TAPSI)
            ax.plot(np.arange(len(weeks_sorted)), roll.values,
                    color=TAPSI, linewidth=2)
            ax.set_ylabel("% Joint Drivers")
        ax.set_title("Joint (Multi-Platform) Driver Share", fontsize=10)
        ax.set_xticks(range(0, len(weeks_sorted),
                      max(1, len(weeks_sorted)//8)))
        ax.set_xticklabels(
            [week_labels[i] for i in range(0, len(weeks_sorted),
                                           max(1, len(weeks_sorted)//8))],
            rotation=45, fontsize=7)
        style_ax(ax)

        # Panel 3: Average rides per week
        ax = axes[1, 0]
        for col, label, color in [
            ("snapp_ride", "Snapp", SNAPP), ("tapsi_ride", "Tapsi", TAPSI)
        ]:
            if col in short.columns:
                wk = short.groupby("yearweek")[
                    col].mean().reindex(weeks_sorted)
                roll = wk.rolling(3, min_periods=1).mean()
                ax.plot(np.arange(len(weeks_sorted)), roll.values,
                        color=color, linewidth=2, label=label)
        ax.set_ylabel("Avg Rides / Driver")
        ax.set_title("Average Weekly Ride Count", fontsize=10)
        ax.legend(frameon=False, fontsize=8)
        ax.set_xticks(range(0, len(weeks_sorted),
                      max(1, len(weeks_sorted)//8)))
        ax.set_xticklabels(
            [week_labels[i] for i in range(0, len(weeks_sorted),
                                           max(1, len(weeks_sorted)//8))],
            rotation=45, fontsize=7)
        style_ax(ax)

        # Panel 4: Response count per week
        ax = axes[1, 1]
        wk_counts = short.groupby("yearweek").size().reindex(weeks_sorted)
        ax.bar(np.arange(len(weeks_sorted)), wk_counts.values,
               color=GREY, alpha=0.6, edgecolor="white")
        ax.set_ylabel("Responses")
        ax.set_title("Weekly Response Count", fontsize=10)
        ax.set_xticks(range(0, len(weeks_sorted),
                      max(1, len(weeks_sorted)//8)))
        ax.set_xticklabels(
            [week_labels[i] for i in range(0, len(weeks_sorted),
                                           max(1, len(weeks_sorted)//8))],
            rotation=45, fontsize=7)
        style_ax(ax)
        save_fig(pdf, fig)

    # ================================================================
    # PAGE 16 – INCENTIVE TYPE MIX EVOLUTION (STACKED AREA)
    # ================================================================
    with safe_page(pdf, "Page 16 – Incentive Type Mix Evolution"):
        if wide is None:
            raise ValueError("Wide data not available")

        fig, axes = plt.subplots(1, 2, figsize=(16, 7), facecolor=BG)
        fig.suptitle("Incentive Type Mix Over Time  (% of drivers receiving each type)",
                     fontsize=14, fontweight="bold", y=0.98)

        for ax, (prefix, title, cmap_name) in zip(axes, [
            ("Snapp Incentive Type__", "Snapp", "Greens"),
            ("Tapsi Incentive Type__", "Tapsi", "Oranges"),
        ]):
            inc_cols = [c for c in wide.columns if c.startswith(prefix)]
            if not inc_cols:
                ax.text(0.5, 0.5, "No incentive type columns", transform=ax.transAxes,
                        ha="center", fontsize=14, color=GREY)
                ax.set_title(title)
                style_ax(ax)
                continue
            heat = wide.groupby("yearweek")[
                inc_cols].mean().reindex(weeks_sorted) * 100
            # Mask weeks when question wasn't asked (periodic)
            heat = _mask_rare_heatmap(heat, wide, inc_cols, weeks_sorted)
            heat.columns = [c.replace(prefix, "") for c in heat.columns]
            # Drop types with very low signal
            heat = heat.loc[:, heat.mean(skipna=True) > 0.5]
            # Only plot active weeks (stackplot can't handle NaN)
            active_mask = heat.notna().any(axis=1)
            heat_active = heat[active_mask].fillna(0)
            if heat_active.empty or len(heat_active) < 3:
                ax.text(0.5, 0.5, f"Insufficient data",
                        transform=ax.transAxes, ha="center", fontsize=14, color=GREY)
                ax.set_title(title)
                ax.axis("off")
                continue
            active_wk_labels = [str(w) for w in heat_active.index]
            n_active = len(heat_active)
            colors = plt.cm.get_cmap(cmap_name)(
                np.linspace(0.3, 0.9, len(heat_active.columns)))
            ax.stackplot(np.arange(n_active),
                         *[heat_active[c].values for c in heat_active.columns],
                         labels=heat_active.columns, colors=colors, alpha=0.8)
            ax.set_title(f"{title}  ({n_active} active weeks)", fontsize=12, fontweight="bold")
            ax.set_ylabel("% of Drivers")
            ax.set_xlabel("Year-Week")
            ax.legend(fontsize=6, loc="upper right", frameon=False)
            tick_step = max(1, n_active // 8)
            ax.set_xticks(range(0, n_active, tick_step))
            ax.set_xticklabels(
                [active_wk_labels[i] for i in range(0, n_active, tick_step)],
                rotation=45, fontsize=7)
            style_ax(ax)
        save_fig(pdf, fig)

    # ================================================================
    # PAGE 17 – UNSATISFACTION REASONS TREND
    # ================================================================
    with safe_page(pdf, "Page 17 – Incentive Unsatisfaction Reasons Trend"):
        if wide is None:
            raise ValueError("Wide data not available")

        fig, axes = plt.subplots(1, 2, figsize=(16, 7), facecolor=BG)
        fig.suptitle("Incentive Unsatisfaction Reasons Over Time",
                     fontsize=14, fontweight="bold", y=0.98)

        for ax, (prefix, title, color) in zip(axes, [
            ("Snapp Last Incentive Unsatisfaction__", "Snapp", SNAPP),
            ("Tapsi Last Incentive Unsatisfaction__", "Tapsi", TAPSI),
        ]):
            unsat_cols = [c for c in wide.columns if c.startswith(prefix)]
            if not unsat_cols:
                ax.text(0.5, 0.5, "No data", transform=ax.transAxes,
                        ha="center", fontsize=14, color=GREY)
                ax.set_title(title)
                ax.axis("off")
                continue
            wk_data = wide.groupby("yearweek")[
                unsat_cols].mean().reindex(weeks_sorted) * 100
            # Unsatisfaction is periodical — mask weeks when not asked
            wk_data = _mask_rare_heatmap(wk_data, wide, unsat_cols, weeks_sorted)

            wk_data.columns = [c.replace(prefix, "") for c in wk_data.columns]
            # Drop reasons with very low signal or too few active weeks
            col_active_pct = wk_data.notna().mean()
            wk_data = wk_data.loc[:, (wk_data.mean(skipna=True) > 1) & (col_active_pct >= MIN_ACTIVE_WEEKS_PCT)]
            active_wk_count = wk_data.notna().any(axis=1).sum()
            if wk_data.empty or active_wk_count < 3:
                ax.text(0.5, 0.5, f"Insufficient data\n(active in {active_wk_count} weeks)",
                        transform=ax.transAxes, ha="center", fontsize=14, color=GREY)
                ax.set_title(title, fontsize=12, fontweight="bold")
                ax.axis("off")
                continue
            line_colors = plt.cm.tab10(
                np.linspace(0, 0.7, len(wk_data.columns)))
            x_pos = np.arange(len(weeks_sorted))
            shaded_unsat = False
            for i, reason in enumerate(wk_data.columns):
                roll = wk_data[reason].rolling(3, min_periods=3).mean()
                plot_gapped_line(ax, x_pos, roll.values,
                                 linewidth=2, label=reason, color=line_colors[i],
                                 marker="o", markersize=2)
                if not shaded_unsat:
                    shade_nan_gaps(ax, x_pos, wk_data[reason].values)
                    shaded_unsat = True
            ax.set_ylabel("% of Drivers")
            ax.set_xlabel("Year-Week")
            ax.set_title(title, fontsize=12, fontweight="bold")
            ax.legend(fontsize=7, frameon=False, loc="upper right")
            ax.set_xticks(range(0, len(weeks_sorted),
                          max(1, len(weeks_sorted)//8)))
            ax.set_xticklabels(
                [week_labels[i] for i in range(0, len(weeks_sorted),
                                               max(1, len(weeks_sorted)//8))],
                rotation=45, fontsize=7)
            style_ax(ax)
        save_fig(pdf, fig)

    # ================================================================
    # PAGE 18 – GPS PROBLEM SEVERITY OVER TIME
    # ================================================================
    with safe_page(pdf, "Page 18 – GPS Problem Impact Over Time"):
        fig, axes = plt.subplots(1, 2, figsize=(16, 7), facecolor=BG)
        fig.suptitle("GPS Issues Over Time – Stage Distribution & Impact",
                     fontsize=14, fontweight="bold", y=0.98)

        # Panel 1: GPS stage distribution over time (periodical/rare)
        ax = axes[0]
        if "snapp_gps_stage" in short.columns and col_has_enough_data(short, "snapp_gps_stage", weeks_sorted):
            gps_active = _rare_week_mask(
                short, "snapp_gps_stage", weeks_sorted)
            # Only consider stages with meaningful frequency
            stage_counts = short["snapp_gps_stage"].value_counts(dropna=True)
            stages = [s for s in stage_counts.index if stage_counts[s] >= 50]
            x_pos = np.arange(len(weeks_sorted))
            gps_shaded = False
            gps_colors = plt.cm.Set2(np.linspace(0, 1, max(len(stages), 1)))
            for si, stage in enumerate(stages):
                wk = short.groupby("yearweek").apply(
                    lambda g, _s=stage: (g["snapp_gps_stage"] == _s).mean() * 100
                ).reindex(weeks_sorted)
                wk[~gps_active] = np.nan
                roll = wk.rolling(3, min_periods=3).mean()
                plot_gapped_line(ax, x_pos, roll.values,
                                 linewidth=2, label=str(stage)[:25],
                                 marker="o", markersize=2, color=gps_colors[si])
                if not gps_shaded:
                    shade_nan_gaps(ax, x_pos, wk.values)
                    gps_shaded = True
            active_count = gps_active.sum()
            ax.legend(fontsize=7, frameon=False, loc="upper right")
        else:
            ax.text(0.5, 0.5, "Insufficient GPS data",
                    transform=ax.transAxes, ha="center", fontsize=14, color=GREY)
            ax.axis("off")
            active_count = 0
        ax.set_ylabel("% of Drivers")
        ax.set_title(f"Snapp GPS Stage Distribution ({active_count} active weeks)", fontsize=10)
        ax.set_xticks(range(0, len(weeks_sorted),
                      max(1, len(weeks_sorted)//8)))
        ax.set_xticklabels(
            [week_labels[i] for i in range(0, len(weeks_sorted),
                                           max(1, len(weeks_sorted)//8))],
            rotation=45, fontsize=7)
        style_ax(ax)

        # Panel 2: GPS glitch time distribution heatmap
        ax = axes[1]
        if wide is not None:
            glitch_cols = [
                c for c in wide.columns if c.startswith("GPS Glitch Time__")]
            if glitch_cols:
                heat = wide.groupby("yearweek")[glitch_cols].mean().reindex(
                    weeks_sorted) * 100
                # GPS is periodical — mask weeks when not asked
                heat = _mask_rare_heatmap(
                    heat, wide, glitch_cols, weeks_sorted)
                heat.columns = [c.replace("GPS Glitch Time__", "")
                                for c in heat.columns]
                cmap_gps = plt.cm.get_cmap("YlOrRd").copy()
                cmap_gps.set_bad(color="#F5F5F5")
                im = ax.imshow(np.ma.masked_invalid(heat.T.values),
                               aspect="auto", cmap=cmap_gps,
                               interpolation="nearest")
                ax.set_yticks(range(len(heat.columns)))
                ax.set_yticklabels(heat.columns, fontsize=8)
                plt.colorbar(im, ax=ax, label="% of drivers", shrink=0.7)
            else:
                ax.text(0.5, 0.5, "No GPS glitch columns", transform=ax.transAxes,
                        ha="center", fontsize=14, color=GREY)
        ax.set_title("GPS Glitch Time Distribution", fontsize=10)
        ax.set_xticks(range(0, len(weeks_sorted),
                      max(1, len(weeks_sorted)//8)))
        ax.set_xticklabels(
            [week_labels[i] for i in range(0, len(weeks_sorted),
                                           max(1, len(weeks_sorted)//8))],
            rotation=45, fontsize=7)
        save_fig(pdf, fig)

    # ================================================================
    # PAGE 19 – KEY METRICS CORRELATION OVER TIME (ROLLING)
    # ================================================================
    with safe_page(pdf, "Page 19 – Rolling Correlation Matrix"):
        metric_pairs = [
            ("snapp_incentive", "snapp_fare_satisfaction", "Incentive → Fare Sat"),
            ("snapp_commfree", "snapp_fare_satisfaction", "CF Rides → Fare Sat"),
            ("snapp_ride", "snapp_income_satisfaction", "Ride Count → Income Sat"),
            ("active_joint", "snapp_fare_satisfaction", "Joint Rate → Fare Sat"),
        ]
        avail_mp = [(a, b, lbl) for a, b, lbl in metric_pairs
                    if a in short.columns and b in short.columns]

        fig, ax = new_fig(
            "8-Week Rolling Correlation Between Key Metrics",
            figsize=(16, 7))
        line_colors = [SNAPP, TAPSI, ACCENT, ACCENT2]
        for i, (col_a, col_b, label) in enumerate(avail_mp):
            wk_a = short.groupby("yearweek")[
                col_a].mean().reindex(weeks_sorted)
            wk_b = short.groupby("yearweek")[
                col_b].mean().reindex(weeks_sorted)
            combined = pd.DataFrame({"a": wk_a, "b": wk_b})
            rolling_corr = combined["a"].rolling(
                8, min_periods=4).corr(combined["b"])
            ax.plot(np.arange(len(weeks_sorted)), rolling_corr.values,
                    linewidth=2.5, label=label, color=line_colors[i % len(line_colors)])

        ax.axhline(0, color=GREY, linewidth=1, linestyle="--")
        ax.axhline(0.5, color="green", linewidth=0.5, linestyle=":", alpha=0.5)
        ax.axhline(-0.5, color="red", linewidth=0.5, linestyle=":", alpha=0.5)
        ax.set_ylabel("Pearson Correlation (8-wk rolling window)")
        ax.set_xlabel("Year-Week")
        ax.set_ylim(-1, 1)
        ax.set_xticks(range(0, len(weeks_sorted),
                      max(1, len(weeks_sorted)//10)))
        ax.set_xticklabels(
            [week_labels[i] for i in range(0, len(weeks_sorted),
                                           max(1, len(weeks_sorted)//10))],
            rotation=45, fontsize=8)
        ax.legend(frameon=False, fontsize=9)
        style_ax(ax)
        save_fig(pdf, fig)

    # ================================================================
    # PAGE 20 – SUMMARY INSIGHTS
    # ================================================================
    with safe_page(pdf, "Page 20 – Summary of Key Findings"):
        fig = plt.figure(figsize=(14, 9), facecolor=BG)
        fig.suptitle("Trend Insights – Summary of Key Findings",
                     fontsize=16, fontweight="bold", y=0.97)
        ax = fig.add_axes([0.05, 0.05, 0.9, 0.85])
        ax.set_facecolor(BG)
        ax.axis("off")

        # Compute summary stats
        insights = []

        # 1. Overall satisfaction trend
        if "snapp_fare_satisfaction" in short.columns:
            first_half = short[short["yearweek"] <= short["yearweek"].median()]
            second_half = short[short["yearweek"] > short["yearweek"].median()]
            s1 = first_half["snapp_fare_satisfaction"].mean()
            s2 = second_half["snapp_fare_satisfaction"].mean()
            direction = "improving" if s2 > s1 else "declining"
            insights.append(
                f"▸ Snapp Fare Satisfaction is {direction}: "
                f"{s1:.2f} (first half) → {s2:.2f} (second half), "
                f"Δ = {s2-s1:+.3f}")

        # 2. Platform gap
        if ("snapp_fare_satisfaction" in short.columns and
                "tapsi_fare_satisfaction" in short.columns):
            gap = (short["snapp_fare_satisfaction"].mean() -
                   short["tapsi_fare_satisfaction"].mean())
            insights.append(
                f"▸ Average Snapp–Tapsi fare satisfaction gap: {gap:+.3f}  "
                f"({'Snapp leads' if gap > 0 else 'Tapsi leads'})")

        # 3. Joint driver trend
        if "active_joint" in short.columns:
            first_q = short[short["yearweek"] <=
                            short["yearweek"].quantile(0.25)]
            last_q = short[short["yearweek"] >=
                           short["yearweek"].quantile(0.75)]
            j1 = first_q["active_joint"].mean() * 100
            j2 = last_q["active_joint"].mean() * 100
            insights.append(
                f"▸ Joint driver rate: {j1:.0f}% (earliest quarter) → "
                f"{j2:.0f}% (latest quarter), Δ = {j2-j1:+.1f}pp")

        # 4. Incentive spending
        if "snapp_incentive" in short.columns:
            inc_first = short[short["yearweek"] <= short["yearweek"].median()][
                "snapp_incentive"].mean() / 1e6
            inc_second = short[short["yearweek"] > short["yearweek"].median()][
                "snapp_incentive"].mean() / 1e6
            insights.append(
                f"▸ Snapp avg incentive: {inc_first:.2f}M → {inc_second:.2f}M Rials  "
                f"(Δ = {inc_second-inc_first:+.2f}M)")

        # 5. Honeymoon effect size
        if "snapp_age" in short.columns and "snapp_fare_satisfaction" in short.columns:
            new_sat = short[short["snapp_age"] == "less_than_3_months"][
                "snapp_fare_satisfaction"].mean()
            vet_sat = short[short["snapp_age"].isin([
                "3_to_5_years", "5_to_7_years", "more_than_7_years"])][
                "snapp_fare_satisfaction"].mean()
            insights.append(
                f"▸ Honeymoon effect: new drivers ({new_sat:.2f}) vs veterans "
                f"({vet_sat:.2f}), gap = {new_sat-vet_sat:+.2f}")

        # 6. Commission-free trend
        if "snapp_commfree" in short.columns and "snapp_ride" in short.columns:
            cf_pct = (short["snapp_commfree"] / short["snapp_ride"].replace(0, np.nan)
                      ).mean() * 100
            insights.append(
                f"▸ Commission-free rides average {cf_pct:.1f}% of total Snapp rides")

        # 7. NPS
        if "snapp_recommend" in short.columns:
            overall_nps = nps_score(short["snapp_recommend"])
            insights.append(f"▸ Overall Snapp NPS: {overall_nps:+.1f}")

        if "tapsidriver_tapsi_recommend" in short.columns:
            tapsi_nps = nps_score(short["tapsidriver_tapsi_recommend"])
            insights.append(f"▸ Overall Tapsi NPS: {tapsi_nps:+.1f}")

        # 8. Week count
        insights.append(
            f"▸ Data spans {n_weeks} valid weeks with {len(short):,} total responses")

        for i, ins in enumerate(insights):
            y_pos = 0.95 - i * 0.085
            color = "#333333"
            if "declining" in ins.lower():
                color = "#D32F2F"
            elif "improving" in ins.lower():
                color = "#2E7D32"
            ax.text(0.02, y_pos, ins, fontsize=11, transform=ax.transAxes,
                    va="top", color=color, fontweight="bold" if i == 0 else "normal")
        save_fig(pdf, fig)

print(f"\n{'='*60}")
print(f"Done! Report saved to: {OUTPUT_PDF}")
print(f"{'='*60}")
