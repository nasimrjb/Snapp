import pandas as pd
import numpy as np

# ===============================
# PATHS
# ===============================
INPUT_CLEANED_SURVEY = r"D:\OneDrive\Work\Driver Survey\Outputs\cleaned_survey.xlsx"
OUTPUT_FINAL = r"D:\OneDrive\Work\Driver Survey\Outputs\survey_raw_database.xlsx"

# ===============================
# LOAD DATA
# ===============================
df = pd.read_excel(INPUT_CLEANED_SURVEY)

# ===============================
# BASIC FLAGS
# ===============================
df["joint_by_signup"] = np.where(df["age_tapsi"] == "Not Registered", 0, 1)

df["active_joint"] = np.where(
    (df["age_tapsi"] == "Not Registered") | (df["trip_count_tapsi"] == "0"),
    0,
    1
)

# ===============================
# TRIP COUNT MAPPINGS
# ===============================
# ride_map_fa = {
#     "کمتر از 5 سفر": 2.5,
#     "بین 5 تا 10 سفر": 7.5,
#     "بین 11 تا 20 سفر": 15,
#     "بین 21 تا 30 سفر": 25,
#     "بین 31 تا 40 سفر": 35,
#     "بین 41 تا 50 سفر": 45,
#     "بین 51 تا 60 سفر": 55,
#     "بین 61 تا 70 سفر": 65,
#     "بین 71 تا 80 سفر": 75,
#     "بیش از 80 سفر": 80,
# }
ride_map_fa = {
    "<5": 2.5,
    "5_10": 7.5,
    "11_20": 15,
    "21_30": 25,
    "31_40": 35,
    "41_50": 45,
    "51_60": 55,
    "61_70": 65,
    "71_80": 75,
    ">80": 80,
}

df["ride_snapp"] = df["trip_count_snapp"].map(ride_map_fa)
df["ride_tapsi"] = df["trip_count_tapsi"].map(ride_map_fa)

# ===============================
# COMMISSION-FREE RIDE MAPPING
# ===============================
commfree_map = {
    "<5": 2.5,
    "5_10": 7.5,
    "11_20": 15,
    "21_30": 25,
    "31_40": 35,
    "41_50": 45,
    "51_60": 55,
    "61_70": 65,
    "71_80": 75,
    ">80": 80,
}

df["commfree_disc_ride_tapsi"] = df["trip_count_commfree_discount_tapsi"].map(
    commfree_map)
df["commfree_disc_ride_snapp"] = df["trip_count_commfree_discount_snapp"].map(
    commfree_map)

# ===============================
# DIFFERENCES
# ===============================
df["diff_commfree_snapp"] = df["ride_snapp"] - df["commfree_disc_ride_snapp"]
df["diff_commfree_tapsi"] = df["ride_tapsi"] - df["commfree_disc_ride_tapsi"]

# ===============================
# FINAL COMMISSION-FREE VALUE
# ===============================
df["commfree_snapp"] = np.where(
    df["diff_commfree_snapp"] < 0,
    df["ride_snapp"],
    df["commfree_disc_ride_snapp"]
)

df["commfree_tapsi"] = np.where(
    df["diff_commfree_tapsi"] < 0,
    df["ride_tapsi"],
    df["commfree_disc_ride_tapsi"]
)

# ===============================
# INCENTIVE (RIAL) MAPPING
# ===============================
incentive_map = {
    "<100k": 500_000,
    "100_200k": 1_500_000,
    "200_400k": 3_000_000,
    "400_600k": 5_000_000,
    "600_800k": 7_000_000,
    "800k_1m": 9_000_000,
    "1m_1.25m": 11_250_000,
    "1.25m_1.5m": 13_750_000,
    ">1.5m": 17_500_000,
    "250_500k": 3_750_000,
    "100_250k": 1_750_000,
    "500_750k": 6_250_000,
    "750k_1m": 8_750_000,
    "1m_1.25m": 11_250_000,
}

df["incentive_snapp"] = df["incentive_rial_details_snapp"].map(incentive_map)
df["incentive_tapsi"] = df["incentive_rial_details_tapsi"].map(incentive_map)

# ===============================
# WHEEL
# ===============================
wheel_map = {
    "<20k": 150_000,
    "20_40k": 300_000,
    "40_60k": 500_000,
    "60_80k": 700_000,
    "80_100k": 900_000,
    "100_150k": 1_250_000,
    "150_200k": 1_750_000,
    ">200k": 2_000_000,
}

df["wheel"] = df["magical_window_income_tapsi"].map(wheel_map)

# ===============================
# COOPERATION TYPE
# ===============================
coop_map = {
    "few hours/month": "Part-Time",
    "<20hour/mo": "Part-Time",
    "5_20hour/week": "Part-Time",
    "20_40h/week": "Part-Time",
    ">40h/week": "Full-Time",
    "8_12hour/day": "Full-Time",
    ">12h/day": "Full-Time",
}

df["cooperation_type"] = df["active_time"].map(coop_map)

# ===============================
# LOC (LENGTH OF COOPERATION)
# ===============================
loc_map = {
    "Not Registered": 0,
    "less_than_1_month": 0.5,
    "1_to_3_months": 2,
    "less_than_3_months": 2,
    "less_than_5_trips": 2.5,
    "3_to_6_months": 4.5,
    "5_and_10_trips": 7.5,
    "6_to_12_months": 9,
    "6_months_to_1_year": 9,
    "10_and_20_trips": 15,
    "1_to_2_years": 18,
    "1_to_3_years": 24,
    "20_and_30_trips": 25,
    "2_to_3_years": 30,
    "30_and_40_trips": 35,
    "3_to_4_years": 42,
    "40_and_50_trips": 45,
    "3_to_5_years": 48,
    "more_than_4_years": 54,
    "50_and_60_trips": 55,
    "60_and_70_trips": 65,
    "5_to_7_years": 72,
    "70_and_80_trips": 75,
    "more_than_80_trips": 80,
    "more_than_7_years": 96,
}

df["snapp_LOC"] = df["age_snapp"].map(loc_map)
df["tapsi_LOC"] = df["age_tapsi"].map(loc_map)

# ===============================
# AGE GROUP
# ===============================
age_group_map = {
    "<18": "18_to_35",
    "18_25": "18_to_35",
    "26_35": "18_to_35",
    "36_45": "more_than_35",
    "46_55": "more_than_35",
    "56_65": "more_than_35",
    ">65": "more_than_35",
}

df["age_group"] = df["age"].map(age_group_map)

# ===============================
# EDUCATION
# ===============================
edu_map = {
    "HighSchool_Diploma": 0,
    "College Degree": 1,
    "Bachelors": 1,
    "Masters": 1,
    "MD/PhD": 1,
}

df["edu"] = df["education"].map(edu_map)

# ===============================
# MARITAL STATUS
# ===============================
marr_map = {
    "Single": 0,
    "Married": 1,
}

df["marr_stat"] = df["marital_status"].map(marr_map)

# ===============================
# INCENTIVE CATEGORY FUNCTION
# ===============================


def build_incentive_category(df, platform):
    money_cols = [
        f"incentive_type_pay_after_ride_{platform}",
        f"incentive_type_inc_guarantee_{platform}",
    ]

    commfree_cols = [
        f"incentive_type_ride_based_commfree_{platform}",
        f"incentive_type_earning_based_commfree_{platform}",
    ]

    money_used = df[money_cols].replace("", np.nan).notna().any(axis=1)
    commfree_used = df[commfree_cols].replace("", np.nan).notna().any(axis=1)

    return np.select(
        [
            money_used & commfree_used,
            money_used,
            commfree_used,
        ],
        [
            "Money & Free-commission",
            "Money",
            "Free-Commission",
        ],
        default=""
    )


df["incentive_category_snapp"] = build_incentive_category(df, "snapp")
df["incentive_category_tapsi"] = build_incentive_category(df, "tapsi")

# ===============================
# SAVE FINAL OUTPUT
# ===============================
df.to_excel(OUTPUT_FINAL, index=False)
print(f"Final dataset saved to: {OUTPUT_FINAL}")
