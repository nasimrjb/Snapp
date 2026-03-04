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
df["joint_by_signup"] = np.where(df["tapsi_age"] == "Not Registered", 0, 1)

df["active_joint"] = np.where(
    (df["tapsi_age"] == "Not Registered") | (df["tapsi_trip_count"] == "0"),
    0,
    1
)


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

df["snapp_ride"] = df["snapp_trip_count"].map(ride_map_fa)
df["tapsi_ride"] = df["tapsi_trip_count"].map(ride_map_fa)

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

df["tapsi_commfree_disc_ride"] = df["tapsi_trip_count_commfree_discount"].map(
    commfree_map)
df["snapp_commfree_disc_ride"] = df["snapp_trip_count_commfree_discount"].map(
    commfree_map)

# ===============================
# DIFFERENCES
# ===============================
df["snapp_diff_commfree"] = df["snapp_ride"] - df["snapp_commfree_disc_ride"]
df["tapsi_diff_commfree"] = df["tapsi_ride"] - df["tapsi_commfree_disc_ride"]

# ===============================
# FINAL COMMISSION-FREE VALUE
# ===============================
df["snapp_commfree"] = np.where(
    df["snapp_diff_commfree"] < 0,
    df["snapp_ride"],
    df["snapp_commfree_disc_ride"]
)

df["tapsi_commfree"] = np.where(
    df["tapsi_diff_commfree"] < 0,
    df["tapsi_ride"],
    df["tapsi_commfree_disc_ride"]
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

df["snapp_incentive"] = df["snapp_incentive_rial_details"].map(incentive_map)
df["tapsi_incentive"] = df["tapsi_incentive_rial_details"].map(incentive_map)

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

df["wheel"] = df["tapsi_magical_window_income"].map(wheel_map)

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

df["snapp_LOC"] = df["snapp_age"].map(loc_map)
df["tapsi_LOC"] = df["tapsi_age"].map(loc_map)

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
        f"{platform}_incentive_type_pay_after_ride",
        f"{platform}_incentive_type_inc_guarantee",
    ]

    commfree_cols = [
        f"{platform}_incentive_type_ride_based_commfree",
        f"{platform}_incentive_type_earning_based_commfree",
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


df["snapp_incentive_category"] = build_incentive_category(df, "snapp")
df["tapsi_incentive_category"] = build_incentive_category(df, "tapsi")

# ===============================
# SAVE FINAL OUTPUT
# ===============================
df.to_excel(OUTPUT_FINAL, index=False)
print(f"Final dataset saved to: {OUTPUT_FINAL}")
