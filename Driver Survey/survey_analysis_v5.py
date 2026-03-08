"""
Driver Survey – Visual Analysis v5
====================================
New in v5 (vs v4):
  - PAGE 55: Incentive Full Funnel (notification→participation) Snapp vs Tapsi
  - PAGE 56: Incentive Time Window Snapp vs Tapsi (how long active)
  - PAGE 57: Tapsi Re-activation Timing (dormancy before incentive response)
  - PAGE 58: App-Level NPS vs Platform NPS (refer_others vs recommend)
  - PAGE 59: Commission Knowledge × Satisfaction cross-tab
  - PAGE 60: Unpaid Fare Follow-up Satisfaction (Snapp vs Tapsi)
  - PAGE 61: Trip Length Preference by Platform
  - PAGE 62: Navigation Actually Used in Last Trip (Snapp vs Tapsi)
  - PAGE 63: Joining Bonus / Registration Origin (snapp_joining_bonus vs tapsi_joining_bonus)
  - PAGE 64: Tapsi In-App & Offline Navigation Deep-Dive
  - PAGE 65: Tapsi Magical Window Income + Referral Program
Usage:
    python survey_analysis_v5.py
"""

from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.ticker as mticker
import matplotlib.gridspec as gridspec
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.patheffects as pe
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
OUTPUT_PDF = r"D:\Work\Driver Survey\processed\driver_survey_analysis_v5.pdf"

SNAPP_COLOR = "#00C853"
TAPSI_COLOR = "#FF6D00"
ACCENT = "#1565C0"
ACCENT2 = "#7B1FA2"
GREY = "#9E9E9E"
LGREY = "#E0E0E0"
BG_COLOR = "#FAFAFA"
PLATFORM_COLORS = {"Snapp": SNAPP_COLOR, "Tapsi": TAPSI_COLOR}
MIN_WEEK_RESPONSES = 100

# ── Load data ────────────────────────────────────────────────────────
short = pd.read_csv(SHORT_PATH, encoding="utf-8-sig", low_memory=False)
wide = pd.read_csv(WIDE_PATH,  encoding="utf-8-sig", low_memory=False)
long = pd.read_csv(LONG_PATH,  encoding="utf-8-sig", low_memory=False)

before = len(short)
short = short[short["snapp_age"].notna() & (short["snapp_age"] != "")].copy()
dropped_age = before - len(short)
if dropped_age > 0:
    print(f"Dropped {dropped_age} records with missing snapp_age")

valid_ids = set(short["recordID"].unique())
wide = wide[wide["recordID"].isin(valid_ids)].copy()
long = long[long["recordID"].isin(valid_ids)].copy()


def parse_datetime_safe(series: pd.Series) -> pd.Series:
    """
    Parse a datetime column that may contain:
    - Excel serial date floats (e.g. 45771.864...)
    - Python datetime / Timestamp objects
    - Date strings in various formats (e.g. '2025/01/15', '01/15/2025')
    - NaN / None / NaT
    """
    def parse_one(val):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return pd.NaT
        # Already a proper datetime
        if isinstance(val, (pd.Timestamp,)):
            return val
        # Python native datetime
        if hasattr(val, "year") and hasattr(val, "month"):
            return pd.Timestamp(val)
        # Excel serial number (float or int, but NOT a year-like 4-digit int)
        if isinstance(val, (int, float)):
            numeric = float(val)
            # Excel serials for modern dates are typically 40000–60000
            if 1000 < numeric < 100000:
                return pd.Timestamp("1899-12-30") + pd.Timedelta(days=numeric)
            # Fallback: try treating as Unix timestamp in seconds
            return pd.Timestamp(numeric, unit="s", tz=None)
        # String: try mixed format parse
        if isinstance(val, str):
            val = val.strip()
            if not val:
                return pd.NaT
            return pd.to_datetime(val, format="mixed", errors="coerce")
        # Catch-all
        return pd.to_datetime(val, errors="coerce")

    return series.apply(parse_one)


for df in [short, wide, long]:
    df["datetime_parsed"] = parse_datetime_safe(df["datetime"])
    df["year"] = df["datetime_parsed"].dt.year
    df["weeknumber"] = pd.to_numeric(df["weeknumber"], errors="coerce")
    df["yearweek"] = (
        (df["year"] % 100) * 100 + df["weeknumber"]
    ).where(df["weeknumber"].notna() & df["year"].notna()).astype("Int64")

# ── DEBUG: put them HERE ──
print("datetime sample:", short["datetime"].head(5).tolist())
print("datetime_parsed sample:", short["datetime_parsed"].head(5).tolist())
print("year nulls:", short["year"].isna().sum(), "of", len(short))
print("weeknumber nulls:", short["weeknumber"].isna().sum(), "of", len(short))
print("yearweek nulls:", short["yearweek"].isna().sum(), "of", len(short))
print("yearweek value_counts:")
print(short["yearweek"].value_counts().head(10))


week_counts_all = short.groupby("yearweek").size()
valid_weeks = week_counts_all[week_counts_all >= MIN_WEEK_RESPONSES].index
dropped_weeks = week_counts_all[week_counts_all < MIN_WEEK_RESPONSES]
if len(dropped_weeks) > 0:
    print(
        f"Dropping {len(dropped_weeks)} week(s) with <{MIN_WEEK_RESPONSES} responses")

short = short[short["yearweek"].isin(valid_weeks)].copy()
wide = wide[wide["yearweek"].isin(valid_weeks)].copy()
long = long[long["yearweek"].isin(valid_weeks)].copy()

print(
    f"Remaining: {len(short)} short, {len(wide)} wide, {len(long)} long, {len(valid_weeks)} weeks")

short["driver_type"] = np.where(
    short["tapsi_ride"] == 0, "Snapp Exclusive", "Joint")
wide["driver_type"] = np.where(
    wide["tapsi_ride"] == 0, "Snapp Exclusive", "Joint")
long["driver_type"] = np.where(
    long["tapsi_ride"] == 0, "Snapp Exclusive", "Joint")

for df in [short, wide, long]:
    df.sort_values("yearweek", inplace=True)

TENURE_ORDER = ["less_than_3_months", "3_to_6_months", "6_months_to_1_year",
                "1_to_3_years", "3_to_5_years", "5_to_7_years", "more_than_7_years"]
TENURE_LABELS = ["<3 m", "3–6 m", "6m–1y", "1–3 y", "3–5 y", "5–7 y", ">7 y"]

SAT_PAIRS = [
    ("snapp_fare_satisfaction",      "tapsi_fare_satisfaction",      "Fare"),
    ("snapp_income_satisfaction",    "tapsi_income_satisfaction",    "Income"),
    ("snapp_req_count_satisfaction", "tapsi_req_count_satisfaction", "Request Count"),
]

# ── Helpers ──────────────────────────────────────────────────────────


def new_fig(title, figsize=(12, 6)):
    fig, ax = plt.subplots(figsize=figsize, facecolor=BG_COLOR)
    ax.set_facecolor(BG_COLOR)
    fig.suptitle(title, fontsize=15, fontweight="bold", y=0.97)
    return fig, ax


def bar_label(ax, fmt="{:.0f}"):
    for container in ax.containers:
        labels = [fmt.format(v.get_height()) if v.get_height()
                  > 0 else "" for v in container]
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
    axes[0].set_ylabel("Mean Satisfaction (1–5)")
    save_fig(pdf, fig)


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


# ── Build PDF ────────────────────────────────────────────────────────
with PdfPages(OUTPUT_PDF) as pdf:

    # ================================================================
    # PAGE 1 – COVER / KEY KPI SUMMARY
    # ================================================================
    n_total = len(short)
    n_weeks = short["yearweek"].nunique()
    n_cities = short["city"].nunique()
    n_joint_pct = (short["driver_type"] == "Joint").mean() * 100
    n_fulltime_pct = (short["cooperation_type"] == "Full-Time").mean() * 100
    snapp_sat_mean = short["snapp_overall_satisfaction"].mean()
    tapsi_sat_mean = short["tapsi_overall_satisfaction"].mean()
    snapp_nps_val = nps_score(short["snapp_recommend"])
    tapsi_nps_val = nps_score(short["tapsi_recommend"])
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
    nps_weekly = short.groupby("yearweek").agg(
        snapp_nps=("snapp_recommend", nps_score), tapsi_nps=("tapsi_recommend", nps_score)).dropna(how="all")
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
    unsat_types = {
        "Snapp": {"Improper Amount": "Snapp Incentive Unsatisfaction__Improper Amount",
                  "Difficult": "Snapp Incentive Unsatisfaction__difficult",
                  "No Time": "Snapp Incentive Unsatisfaction__No Time todo",
                  "Not Available": "Snapp Incentive Unsatisfaction__No Available Time",
                  "Non Payment": "Snapp Incentive Unsatisfaction__Non Payment"},
        "Tapsi": {"Improper Amount": "Tapsi Incentive Unsatisfaction__Improper Amount",
                  "Difficult": "Tapsi Incentive Unsatisfaction__difficult",
                  "Not Available": "Tapsi Incentive Unsatisfaction__Not Available",
                  "No Time": "Tapsi Incentive Unsatisfaction__No Time todo",
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
    # PAGE 27 – COLLABORATION REASONS
    # ================================================================
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

    # ================================================================
    # PAGE 28 – INCOME SOURCE PREFERENCE
    # ================================================================
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

    # ================================================================
    # PAGE 29 – SATISFACTION BY YEARWEEK
    # ================================================================
    fig, axes = plt.subplots(1, 3, figsize=(
        16, 5), facecolor=BG_COLOR, sharey=True)
    fig.suptitle("Avg Satisfaction by Year-Week: Snapp vs Tapsi",
                 fontsize=15, fontweight="bold", y=0.99)
    for ax, (scol, tcol, label) in zip(axes, SAT_PAIRS):
        weekly_sat = short.groupby("yearweek").agg(
            snapp=(scol, "mean"), tapsi=(tcol, "mean"))
        ax.plot(weekly_sat.index.astype(
            str), weekly_sat["snapp"], marker="o", color=SNAPP_COLOR, linewidth=2, label="Snapp")
        ax.plot(weekly_sat.index.astype(
            str), weekly_sat["tapsi"], marker="s", color=TAPSI_COLOR, linewidth=2, label="Tapsi")
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

    # ================================================================
    # PAGE 30 – SATISFACTION BY COOPERATION TYPE
    # ================================================================
    plot_sat_by_group(pdf, short, "cooperation_type", "Cooperation Type")
    # ================================================================
    # PAGE 31 – SATISFACTION BY CITY (top 10 cities)
    # ================================================================
    plot_sat_by_group(pdf, short, "city", "City (Top 10)",
                      top_n=10, min_group_size=20)

    # ================================================================
    # PAGE 32 – SATISFACTION BY DRIVER TYPE
    # ================================================================
    plot_sat_by_group(pdf, short, "driver_type",
                      "Driver Type (Snapp Exclusive vs Joint)")

    # ================================================================
    # PAGE 33 – SATISFACTION HONEYMOON EFFECT (by Snapp tenure)
    # ================================================================
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
    ax.plot(x, sat_by_tenure["snapp_fare"],    marker="s", color=ACCENT,
            linewidth=2,   markersize=7, linestyle="--", label="Fare Sat.")
    ax.plot(x, sat_by_tenure["snapp_income"],  marker="^", color=ACCENT2,
            linewidth=2,   markersize=7, linestyle="--", label="Income Sat.")
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

    # ================================================================
    # PAGE 34 – SATISFACTION BY AGE GROUP
    # ================================================================
    age_order = ["<18", "18_25", "26_35", "36_45", "46_55", "56_65", ">65"]
    plot_sat_by_group(pdf, short, "age", "Age Group", order=age_order)

    # ================================================================
    # PAGE 35 – SATISFACTION BY ACTIVE TIME
    # ================================================================
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
        tapsi_rec=("tapsi_recommend", "mean"),
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
    ax.plot(xlabels, active_sat["snapp_rec"], marker="o", color=SNAPP_COLOR,
            linewidth=2.5, label="Snapp Recommend")
    ax.plot(xlabels, active_sat["tapsi_rec"], marker="s", color=TAPSI_COLOR,
            linewidth=2.5, label="Tapsi Recommend")
    ax.set_ylim(0, 10)
    ax.set_title("Recommendation Score (0–10)", fontsize=11)
    ax.tick_params(axis="x", rotation=15)
    ax.legend(frameon=False, fontsize=9)
    style_ax(ax)
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 36 – SATISFACTION BY OCCUPATION (top 12 jobs with n≥100)
    # ================================================================
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
    x = np.arange(len(jobs))
    w = 0.35
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
    bar_colors = [SNAPP_COLOR if v >= job_sat["snapp_rec"].median() else GREY
                  for v in job_sat["snapp_rec"]]
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

    # ================================================================
    # PAGE 37 – TAPSI CARPOOLING FULL ANALYSIS
    # ================================================================
    fig, axes = plt.subplots(2, 2, figsize=(13, 9), facecolor=BG_COLOR)
    fig.suptitle("Tapsi Carpooling – Familiarity, Adoption, Refusal & Satisfaction",
                 fontsize=15, fontweight="bold", y=0.97)

    ax = axes[0, 0]
    fam = short["tapsi_carpooling_familiar"].value_counts()
    ax.pie(fam.values, labels=fam.index, autopct="%1.0f%%",
           colors=[TAPSI_COLOR, LGREY], startangle=90, wedgeprops={"edgecolor": "white"})
    ax.set_title("Carpooling Familiarity", fontsize=11)

    ax = axes[0, 1]
    offer_data = short["tapsi_carpooling_gotoffer_accepted"].dropna(
    ).value_counts()
    offer_colors = {"No": LGREY, "got offer - rejected": "#FF6D00",
                    "got offer - accepted": "#66BB6A"}
    ax.barh(offer_data.index, offer_data.values,
            color=[offer_colors.get(k, GREY) for k in offer_data.index], edgecolor="white")
    total = offer_data.sum()
    for i, v in enumerate(offer_data.values):
        ax.text(v + 5, i, f"{v} ({v/total*100:.0f}%)", va="center", fontsize=9)
    ax.set_title("Carpooling Offer Outcome", fontsize=11)
    style_ax(ax)

    ax = axes[1, 0]
    # Carpooling refusal reasons from wide
    carp_refusal = {
        "Canceled by Passenger": "Tapsi Carpooling Refusion__Canceled by Passenger",
        "Long Wait Time":        "Tapsi Carpooling Refusion__Long Wait Time",
        "Passenger Distance":    "Tapsi Carpooling Refusion__Passenger Distance",
        "Not Familiar":          "Tapsi Carpooling Refusion__Not Familiar",
    }
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
    ax.bar(sat_counts.index.astype(int).astype(str), sat_counts.values,
           color=TAPSI_COLOR, edgecolor="white")
    total = sat_counts.sum()
    for xi, v in zip(sat_counts.index.astype(int).astype(str), sat_counts.values):
        ax.text(xi, v + 1, f"{v} ({v/total*100:.0f}%)",
                ha="center", fontsize=9)
    ax.set_title(
        f"Carpooling Satisfaction (1–5, mean={carp_sat.mean():.2f})", fontsize=11)
    ax.set_xlabel("Rating")
    style_ax(ax)
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 38 – ECOPLUS & MAGICAL WINDOW ADOPTION
    # ================================================================
    fig, axes = plt.subplots(1, 2, figsize=(13, 5), facecolor=BG_COLOR)
    fig.suptitle("Feature Adoption – Snapp EcoPlus & Tapsi Magical Window",
                 fontsize=15, fontweight="bold", y=0.99)

    ax = axes[0]
    ecoplus_familiar = short["snapp_ecoplus_familiar"].dropna().value_counts()
    ecoplus_usage = short["snapp_ecoplus_access_usage"].dropna().value_counts()
    cats = ["Familiar", "Has Access\n& Uses",
            "Has Access\n& Not Using", "Not Familiar"]
    n_familiar = ecoplus_familiar.get("Yes", 0)
    n_not_familiar = ecoplus_familiar.get("No", 0)
    n_uses = ecoplus_usage.get("Yes-Yes", 0)
    n_access_noUse = ecoplus_usage.get("Yes-No", 0)
    vals = [n_familiar, n_uses, n_access_noUse, n_not_familiar]
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
    ax.bar(mw.index, mw.values,
           color=[mw_colors.get(k, GREY) for k in mw.index], edgecolor="white")
    total_mw = mw.sum()
    for i, (k, v) in enumerate(mw.items()):
        ax.text(i, v + 10, f"{v:,} ({v/total_mw*100:.0f}%)",
                ha="center", fontsize=9)
    ax.set_title("Tapsi Magical Window Awareness", fontsize=11)
    style_ax(ax)
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 39 – DRIVER PRIVACY & PARTICIPATION FEELINGS
    # ================================================================
    fig, axes = plt.subplots(1, 2, figsize=(13, 5), facecolor=BG_COLOR)
    fig.suptitle("Driver Privacy & Participation Attitudes (Snapp)",
                 fontsize=15, fontweight="bold", y=0.99)

    ax = axes[0]
    feeling = short["snapp_participate_feeling"].dropna().value_counts()
    total_f = feeling.sum()
    feel_colors = {"no difference": SNAPP_COLOR, "no worry": "#66BB6A",
                   "talk to some people": ACCENT,
                   "prefer not to talk": "#FFA726", "no talk at all": "#EF5350"}
    ax.barh(feeling.index, feeling.values,
            color=[feel_colors.get(k, GREY) for k in feeling.index], edgecolor="white")
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

    # ================================================================
    # PAGE 40 – DEMAND & MISSED DEMAND ANALYSIS
    # ================================================================
    fig, axes = plt.subplots(1, 3, figsize=(15, 5), facecolor=BG_COLOR)
    fig.suptitle("Demand & Supply – How Much Demand Do Drivers Process?",
                 fontsize=15, fontweight="bold", y=0.99)

    for ax, col, color, label in [
        (axes[0], "demand_process", ACCENT,  "% of Demand Processed"),
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

    # ================================================================
    # PAGE 41 – COMMISSION-FREE RIDES VS TOTAL RIDES
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

    # ================================================================
    # PAGE 42 – TOP 15 CITIES
    # ================================================================
    city_counts = short["city"].value_counts().head(15)
    fig, ax = new_fig("Top 15 Cities by Response Count")
    ax.barh(city_counts.index[::-1], city_counts.values[::-1],
            color=ACCENT, edgecolor="white")
    for i, v in enumerate(city_counts.values[::-1]):
        ax.text(v + 1, i, f"{v:,}", va="center", fontsize=9)
    ax.set_xlabel("Responses")
    style_ax(ax)
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 43 – CITY SATISFACTION COMPARISON
    # ================================================================
    top_cities = short["city"].value_counts().head(12).index
    city_sat = (
        short[short["city"].isin(top_cities)]
        .groupby("city")
        .agg(snapp_sat=("snapp_overall_satisfaction", "mean"),
             tapsi_sat=("tapsi_overall_satisfaction", "mean"),
             snapp_rec=("snapp_recommend", "mean"),
             tapsi_rec=("tapsi_recommend", "mean"),
             n=("snapp_overall_satisfaction", "count"))
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

    # ================================================================
    # PAGE 44 – REGISTRATION TYPE & REFERRAL FUNNEL
    # ================================================================
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

    # ================================================================
    # PAGE 45 – ★ LONG_SURVEY: INCENTIVE TYPE (Snapp vs Tapsi)
    # ================================================================
    plot_long_snapp_vs_tapsi(pdf, long,
                             "Snapp Incentive Type", "Tapsi Incentive Type",
                             "Incentive Type – long_survey")

    # ================================================================
    # PAGE 46 – ★ LONG_SURVEY: INCENTIVE GOT BONUS (Snapp vs Tapsi)
    # ================================================================
    plot_long_snapp_vs_tapsi(pdf, long,
                             "Snapp Incentive GotBonus", "Tapsi Incentive GotBonus",
                             "Incentive Got Bonus – long_survey")

    # ================================================================
    # PAGE 47 – ★ LONG_SURVEY: CUSTOMER SUPPORT (Snapp vs Tapsi)
    # ================================================================
    plot_long_snapp_vs_tapsi(pdf, long,
                             "Snapp Customer Support Category", "Tapsi Customer Support Category",
                             "Customer Support Categories – long_survey")

    # ================================================================
    # PAGE 48 – ★ LONG_SURVEY: SNAPP NAVIGATION UNSATISFACTION
    # ================================================================
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

    # ================================================================
    # PAGE 49 – ★ LONG_SURVEY: DECLINE REASON & SNAPPDRIVER APP MENU
    # ================================================================
    fig, axes = plt.subplots(1, 2, figsize=(14, 6), facecolor=BG_COLOR)
    fig.suptitle("Decline Reason & SnappDriver App Menu Usage – long_survey",
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

    # ================================================================
    # PAGE 50 – ★ LONG_SURVEY: TAPSI-ONLY QUESTIONS
    # ================================================================
    fig, axes = plt.subplots(1, 2, figsize=(14, 5), facecolor=BG_COLOR)
    fig.suptitle("Tapsi-Only Questions – long_survey",
                 fontsize=15, fontweight="bold", y=0.99)

    for ax, question, color, label in [
        (axes[0], "Tapsi Incentive Unsatisfaction",
         TAPSI_COLOR, "Incentive Unsatisfaction"),
        (axes[1], "Tapsi Carpooling Refusion",
         TAPSI_COLOR, "Carpooling Refusion"),
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

    # ================================================================
    # PAGE 51 – ★ LONG_SURVEY: CUSTOMER SUPPORT BY DRIVER TYPE
    # ================================================================
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

    # ================================================================
    # PAGE 53 – ★ LONG_SURVEY: SNAPP USAGE APP & ECOPLUS REFUSAL
    # ================================================================
    fig, axes = plt.subplots(1, 2, figsize=(14, 5), facecolor=BG_COLOR)
    fig.suptitle("Snapp App Usage & EcoPlus Refusal – long_survey",
                 fontsize=15, fontweight="bold", y=0.99)

    for ax, question, color, label in [
        (axes[0], "Snapp Usage app",       SNAPP_COLOR, "Snapp App Usage"),
        (axes[1], "Snapp Ecoplus Refusal",
         ACCENT2,     "EcoPlus Refusal Reasons"),
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

    # ================================================================
    # PAGE 54 – DRIVER TYPE KEY METRICS SUMMARY
    # ================================================================
    metrics_by_dt = short.groupby("driver_type").agg(
        n=("snapp_overall_satisfaction", "count"),
        snapp_sat=("snapp_overall_satisfaction", "mean"),
        tapsi_sat=("tapsi_overall_satisfaction", "mean"),
        snapp_ride=("snapp_ride", "mean"),
        tapsi_ride=("tapsi_ride", "mean"),
        snapp_inc=("snapp_incentive", "mean"),
        tapsi_inc=("tapsi_incentive", "mean"),
        snapp_rec=("snapp_recommend", "mean"),
        tapsi_rec=("tapsi_recommend", "mean"),
    )

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

    # ================================================================
    # PAGE 55 – INCENTIVE FULL FUNNEL (Snapp vs Tapsi)
    # Notification received → Participated → Reward Type
    # ================================================================
    fig, axes = plt.subplots(1, 2, figsize=(14, 6), facecolor=BG_COLOR)
    fig.suptitle("Incentive Full Funnel – Notification → Participation",
                 fontsize=15, fontweight="bold", y=0.99)

    # Left: Snapp incentive notification & participation funnel
    ax = axes[0]
    notif_col = "incentive_got_messsnapp_age"
    partic_col = "snapp_incentive_message_participation"
    notif_vc = short[notif_col].dropna().value_counts()
    partic_vc = short[partic_col].dropna().value_counts()
    n_yes_notif = notif_vc.get("Yes", 0)
    n_no_notif = notif_vc.get("No",  0)
    n_yes_partic = partic_vc.get("Yes", 0)
    n_no_partic = partic_vc.get("No",  0)
    funnel_labels = ["Got Notification\n(Yes)", "Got Notification\n(No)",
                     "Participated\n(Yes)", "Participated\n(No)"]
    funnel_vals = [n_yes_notif, n_no_notif, n_yes_partic, n_no_partic]
    funnel_colors = [SNAPP_COLOR, LGREY, "#66BB6A", "#EF5350"]
    bars = ax.bar(funnel_labels, funnel_vals,
                  color=funnel_colors, edgecolor="white")
    total_n = notif_vc.sum()
    total_p = partic_vc.sum()
    totals_ = [total_n, total_n, total_p, total_p]
    for i, (b, v, tot) in enumerate(zip(bars, funnel_vals, totals_)):
        ax.text(b.get_x() + b.get_width()/2, v + 30,
                f"{v:,}\n({v/tot*100:.0f}%)", ha="center", fontsize=9)
    ax.set_title("Snapp Incentive Funnel", fontsize=11)
    ax.set_ylabel("Count")
    style_ax(ax)

    # Right: Tapsi incentive notification & participation funnel
    ax = axes[1]
    tapsi_notif_col = "incentive_got_messtapsi_age"
    tapsi_partic_col = "tapsi_incentive_message_participation"
    if tapsi_notif_col in short.columns:
        t_notif_vc = short[tapsi_notif_col].dropna().value_counts()
    else:
        t_notif_vc = pd.Series(dtype=int)
    if tapsi_partic_col in short.columns:
        t_partic_vc = short[tapsi_partic_col].dropna().value_counts()
    else:
        t_partic_vc = pd.Series(dtype=int)
    t_yes_notif = t_notif_vc.get("Yes", 0)
    t_no_notif = t_notif_vc.get("No",  0)
    t_yes_partic = t_partic_vc.get("Yes", 0)
    t_no_partic = t_partic_vc.get("No",  0)
    t_funnel_vals = [t_yes_notif, t_no_notif, t_yes_partic, t_no_partic]
    bars2 = ax.bar(funnel_labels, t_funnel_vals,
                   color=funnel_colors, edgecolor="white")
    t_total_n = t_notif_vc.sum() if len(t_notif_vc) else 1
    t_total_p = t_partic_vc.sum() if len(t_partic_vc) else 1
    t_totals_ = [t_total_n, t_total_n, t_total_p, t_total_p]
    for i, (b, v, tot) in enumerate(zip(bars2, t_funnel_vals, t_totals_)):
        if tot > 0:
            ax.text(b.get_x() + b.get_width()/2, v + 30,
                    f"{v:,}\n({v/tot*100:.0f}%)", ha="center", fontsize=9)
    ax.set_title("Tapsi Incentive Funnel", fontsize=11)
    ax.set_ylabel("Count")
    style_ax(ax)
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 56 – INCENTIVE TIME WINDOW (Snapp vs Tapsi)
    # How long was the incentive scheme active?
    # ================================================================
    time_order = ["Few Hours", "1 Day", "1_6 Days", "7 Days", ">7 Days"]
    snapp_tw_col = "snapp_incentive_time_limitation"
    tapsi_tw_col = "tapsi_incentive_active_duration"

    fig, axes = plt.subplots(1, 2, figsize=(14, 5), facecolor=BG_COLOR)
    fig.suptitle("Incentive Active Duration – Snapp vs Tapsi",
                 fontsize=15, fontweight="bold", y=0.99)

    for ax, col, color, label in [
        (axes[0], snapp_tw_col, SNAPP_COLOR, "Snapp"),
        (axes[1], tapsi_tw_col, TAPSI_COLOR, "Tapsi"),
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

    # ================================================================
    # PAGE 57 – TAPSI RE-ACTIVATION TIMING
    # How long had drivers been inactive before responding to incentive?
    # ================================================================
    inact_col = "tapsi_inactive_b4_incentive"
    inact_order = ["Same Day", "1_3 Day Before", "4_7 Day Before",
                   "1_4 Week Before", "1_3 Month Before",
                   "3_6 Month Before", ">6 Month Before"]

    fig, ax = plt.subplots(figsize=(12, 6), facecolor=BG_COLOR)
    fig.suptitle("Tapsi Re-activation Timing: Inactivity Before Incentive Response",
                 fontsize=15, fontweight="bold", y=1.01)

    if inact_col in short.columns:
        data = short[inact_col].dropna()
        present = [v for v in inact_order if v in data.values]
        vc = data.value_counts().reindex(present).dropna()
        total = vc.sum()
        bar_colors = [TAPSI_COLOR if i == 0 else
                      ("#FFA726" if i <= 2 else LGREY)
                      for i in range(len(vc))]
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

    # ================================================================
    # PAGE 58 – APP NPS vs PLATFORM NPS
    # snapp_refer_others (App NPS) vs snapp_recommend (Platform NPS)
    # tapsi_refer_others (App NPS) vs tapsi_recommend (Platform NPS)
    # ================================================================
    fig, axes = plt.subplots(2, 2, figsize=(14, 10), facecolor=BG_COLOR)
    fig.suptitle("App NPS vs Platform NPS – Snapp & Tapsi",
                 fontsize=15, fontweight="bold", y=0.98)

    nps_pairs = [
        (axes[0, 0], "snapp_refer_others", SNAPP_COLOR,
         "Snapp APP NPS\n(recommend the SnappDriver app)"),
        (axes[0, 1], "snapp_recommend",    SNAPP_COLOR,
         "Snapp PLATFORM NPS\n(recommend driving on Snapp)"),
        (axes[1, 0], "tapsi_refer_others", TAPSI_COLOR,
         "Tapsi APP NPS\n(recommend the Tapsi Driver app)"),
        (axes[1, 1], "tapsi_recommend",    TAPSI_COLOR,
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
        neutrals = ((data >= 7) & (data <= 8)).sum()
        total = len(data)
        vc = data.value_counts().sort_index()
        bar_clr = ["#EF5350" if s <= 6 else ("#B0BEC5" if s <= 8 else "#66BB6A")
                   for s in vc.index]
        ax.bar(vc.index.astype(str), vc.values,
               color=bar_clr, edgecolor="white")
        for xi, (k, v) in enumerate(vc.items()):
            ax.text(xi, v + 3, f"{v}", ha="center", fontsize=7)
        ax.set_title(f"{label}\nNPS={nps:+.0f}  |  P={promoters/total*100:.0f}% D={detractors/total*100:.0f}% (n={total:,})",
                     fontsize=9)
        ax.set_xlabel("Score (0–10)")
        ax.set_ylabel("Count")
        style_ax(ax)
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 59 – COMMISSION KNOWLEDGE × SATISFACTION CROSS-TAB
    # ================================================================
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
        bars = ax.barh(comm_groups.index, comm_groups.values,
                       color=color, edgecolor="white", alpha=0.85)
        for i, (k, v) in enumerate(comm_groups.items()):
            n = n_per_group[k]
            ax.text(v + 0.02, i, f"{v:.2f}  (n={n:,})",
                    va="center", fontsize=8)
        ax.set_xlim(0, 5.5)
        ax.axvline(sub[sat_col].mean(), color="black", linestyle="--", linewidth=1,
                   label=f"Overall mean: {sub[sat_col].mean():.2f}")
        ax.set_title(f"{label}: Sat. by Commission Knowledge", fontsize=11)
        ax.set_xlabel("Mean Satisfaction (1–5)")
        ax.legend(frameon=False, fontsize=9)
        style_ax(ax)
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 60 – UNPAID FARE FOLLOW-UP SATISFACTION
    # snapp_satisfaction_followup_overall / _time vs Tapsi equivalents
    # ================================================================
    fig, axes = plt.subplots(1, 2, figsize=(14, 6), facecolor=BG_COLOR)
    fig.suptitle("Unpaid Fare Follow-up Satisfaction – Snapp vs Tapsi",
                 fontsize=15, fontweight="bold", y=0.99)

    followup_pairs = [
        ("snapp_satisfaction_followup_overall", "tapsi_satisfaction_followup_overall",
         "Overall Satisfaction with Follow-up"),
        ("snapp_satisfaction_followup_time",    "tapsi_satisfaction_followup_time",
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

    # ================================================================
    # PAGE 61 – TRIP LENGTH PREFERENCE BY PLATFORM
    # snapp_accepted_trip_length vs tapsi_accepted_trip_length
    # ================================================================
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

    # ================================================================
    # PAGE 62 – NAVIGATION ACTUALLY USED IN LAST TRIP
    # snapp_last_trip_navigation vs tapsi_navigation_type
    # ================================================================
    snapp_nav_order = ["Neshan", "Balad",
                       "Google Map", "Waze", "No Navigation App"]
    tapsi_nav_order = ["Neshan", "Balad",
                       "In-App Navigation", "No Navigation App"]

    fig, axes = plt.subplots(1, 2, figsize=(14, 6), facecolor=BG_COLOR)
    fig.suptitle("Navigation App Used in Last Trip – Snapp vs Tapsi",
                 fontsize=15, fontweight="bold", y=0.99)

    for ax, col, nav_order, color, label in [
        (axes[0], "snapp_last_trip_navigation",
         snapp_nav_order, SNAPP_COLOR, "Snapp"),
        (axes[1], "tapsi_navigation_type",
         tapsi_nav_order, TAPSI_COLOR, "Tapsi"),
    ]:
        if col not in short.columns:
            ax.set_title(f"{label} (no data)")
            style_ax(ax)
            continue
        data = short[col].dropna()
        present = [v for v in nav_order if v in data.values]
        vc = data.value_counts().reindex(present).dropna()
        total = vc.sum()
        bar_colors_nav = []
        for k in vc.index:
            if "No" in k:
                bar_colors_nav.append(LGREY)
            elif "In-App" in k:
                bar_colors_nav.append(
                    TAPSI_COLOR if color == TAPSI_COLOR else ACCENT2)
            else:
                bar_colors_nav.append(color)
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

    # ================================================================
    # PAGE 63 – JOINING BONUS / REGISTRATION ORIGIN
    # snapp_joining_bonus vs tapsi_joining_bonus
    # ================================================================
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
        ax.bar(data.index, data.values,
               color=[bonus_colors.get(k, GREY) for k in data.index], edgecolor="white")
        for i, (k, v) in enumerate(data.items()):
            ax.text(i, v + 20, f"{v:,}\n({v/total*100:.0f}%)",
                    ha="center", fontsize=9)
        ax.set_title(
            f"{label} Joining/Registration Bonus  (n={total:,})", fontsize=11)
        ax.set_xlabel("Received Bonus?")
        ax.set_ylabel("Count")
        style_ax(ax)
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 64 – TAPSI IN-APP & OFFLINE NAVIGATION DEEP-DIVE
    # ================================================================
    fig, axes = plt.subplots(2, 2, figsize=(14, 10), facecolor=BG_COLOR)
    fig.suptitle("Tapsi Navigation Deep-Dive – In-App & Offline Navigation",
                 fontsize=15, fontweight="bold", y=0.98)

    nav_items = [
        (axes[0, 0], "tapsi_in_app_navigation_usage",
         TAPSI_COLOR, "Used Tapsi In-App Navigation"),
        (axes[0, 1], "tapsi_in_app_navigation_satisfaction",
         TAPSI_COLOR, "In-App Navigation Satisfaction (1–5)"),
        (axes[1, 0], "tapsi_offline_navigation_familiar",
         "#5C6BC0",   "Familiar with Tapsi Offline Navigation"),
        (axes[1, 1], "tapsi_offline_navigation_usage",
         "#5C6BC0",   "Offline Navigation Usage During GPS Issues"),
    ]

    for ax, col, color, label in nav_items:
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

    # Add GPS improvement sub-chart in corner if space
    save_fig(pdf, fig)

    # ================================================================
    # PAGE 65 – TAPSI GPS BETTER + MAGICAL WINDOW INCOME & REFERRAL
    # ================================================================
    fig, axes = plt.subplots(1, 3, figsize=(16, 5), facecolor=BG_COLOR)
    fig.suptitle("Tapsi: GPS Performance Perception & Magical Window / Referral Program",
                 fontsize=15, fontweight="bold", y=0.99)

    # Panel 1: Was Tapsi app better during GPS issues?
    ax = axes[0]
    gps_col = "tapsi_gps_better"
    if gps_col in short.columns:
        data = short[gps_col].dropna().value_counts()
        total = data.sum()
        gps_colors = {"Yes": "#66BB6A", "No": "#EF5350", "Similar": "#FFA726"}
        ax.bar(data.index, data.values,
               color=[gps_colors.get(k, GREY) for k in data.index], edgecolor="white")
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

    # Panel 2: Tapsi Magical Window awareness & income
    ax = axes[1]
    mw_col = "tapsi_magical_window"
    if mw_col in short.columns:
        data = short[mw_col].dropna().value_counts()
        total = data.sum()
        mw_clrs = {"Yes": TAPSI_COLOR, "No": "#FFA726", "Not Familiar": LGREY}
        ax.bar(data.index, data.values,
               color=[mw_clrs.get(k, GREY) for k in data.index], edgecolor="white")
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

    # Panel 3: Tapsi Referral Program (from long survey)
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

print(f"\nReport saved to {OUTPUT_PDF}")
