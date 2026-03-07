import pandas as pd
import numpy as np
import warnings
import sys
import io
warnings.filterwarnings('ignore')
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

short = pd.read_csv('processed/short_survey.csv', encoding='utf-8-sig', low_memory=False)
short = short[short['snapp_age'].notna() & (short['snapp_age'] != '')].copy()
short['datetime_parsed'] = pd.to_datetime(short['datetime'], format='mixed')
short['year'] = short['datetime_parsed'].dt.year
short['weeknumber'] = short['weeknumber'].astype(int)
short['yearweek'] = (short['year'] % 100) * 100 + short['weeknumber']
week_counts_all = short.groupby('yearweek').size()
valid_weeks = week_counts_all[week_counts_all >= 100].index
short = short[short['yearweek'].isin(valid_weeks)].copy()
short['driver_type'] = np.where(short['tapsi_ride'] == 0, 'Snapp Exclusive', 'Joint')

cols_to_check = [
    'tapsi_collab_reason','snapp_better_income','tapsi_better_income',
    'tapsi_magical_window','snapp_ecoplus_familiar','snapp_ecoplus_access_usage',
    'demand_process','missed_demand_per_10','max_demand',
    'snapp_accepted_trip_length','tapsi_accepted_trip_length',
    'driver_type','cooperation_type','gender','snapp_participate_feeling',
    'snapp_CS_solved','tapsi_CS_solved',
    'snapp_unpaid_by_passenger_followup','tapsi_unpaid_by_passenger_followup',
    'snapp_compensate_unpaid_by_passenger','tapsi_compensate_unpaid_by_passenger',
    'snapp_customer_support','tapsi_customer_support',
    'tapsi_offline_navigation_usage','tapsi_gps_better','better_offline_navigation',
    'tapsi_invite_to_reg','tapsi_in_app_navigation_usage',
    'snapp_comm_info','tapsi_comm_info','snapp_tax_info','tapsi_tax_info',
    'snapp_not_talking_reason','snapp_gps_stage','tapsi_gps_stage',
    'tapsi_carpooling_familiar','tapsi_carpooling_gotoffer_accepted',
    'tapsi_carpooling_satisfaction_overall'
]

for col in cols_to_check:
    if col in short.columns:
        vc = short[col].value_counts()
        print(f'\n=== {col} ===')
        print(vc.head(10).to_string())

print('\n=== CS satisfaction means ===')
cs_cols = ['snapp_CS_satisfaction_overall','snapp_CS_satisfaction_waittime',
           'snapp_CS_satisfaction_solution','snapp_CS_satisfaction_behaviour',
           'snapp_CS_satisfaction_relevance',
           'tapsi_CS_satisfaction_overall','tapsi_CS_satisfaction_waittime',
           'tapsi_CS_satisfaction_solution','tapsi_CS_satisfaction_behaviour',
           'tapsi_CS_satisfaction_relevance']
for c in cs_cols:
    if c in short.columns:
        print(f'  {c}: mean={short[c].mean():.2f}  n={short[c].notna().sum()}')

print('\n=== navigation ratings means ===')
nav_cols = ['recommendation_googlemap','recommendation_waze','recommendation_neshan','recommendation_balad',
            'tapsi_in_app_navigation_satisfaction','snapp_navigation_app_satisfaction']
for c in nav_cols:
    if c in short.columns:
        print(f'  {c}: mean={short[c].mean():.2f}  n={short[c].notna().sum()}')

print('\n=== job-satisfaction correlation ===')
job_sat = short.groupby('original_job').agg(
    n=('snapp_overall_satisfaction','size'),
    snapp_sat=('snapp_overall_satisfaction','mean'),
    tapsi_sat=('tapsi_overall_satisfaction','mean'),
    snapp_rec=('snapp_recommend','mean'),
    tapsi_rec=('tapsi_recommend','mean')
).query('n >= 50').sort_values('snapp_sat')
print(job_sat.to_string())

print('\n=== active_time x satisfaction ===')
print(short.groupby('active_time')[['snapp_overall_satisfaction','tapsi_overall_satisfaction','snapp_recommend','tapsi_recommend']].mean().round(2).to_string())

print('\n=== age x satisfaction ===')
print(short.groupby('age')[['snapp_overall_satisfaction','tapsi_overall_satisfaction','snapp_ride','tapsi_ride']].mean().round(2).to_string())

print('\n=== snapp_age x satisfaction ===')
print(short.groupby('snapp_age')[['snapp_overall_satisfaction','snapp_fare_satisfaction','snapp_income_satisfaction','snapp_recommend']].mean().round(2).to_string())

print('\n=== driver_type x key metrics ===')
print(short.groupby('driver_type')[['snapp_overall_satisfaction','tapsi_overall_satisfaction','snapp_ride','tapsi_ride','snapp_incentive','tapsi_incentive','snapp_recommend','tapsi_recommend']].mean().round(2).to_string())

print('\n=== city x satisfaction (top 10) ===')
top_cities = short['city'].value_counts().head(10).index
city_sat = short[short['city'].isin(top_cities)].groupby('city')[['snapp_overall_satisfaction','tapsi_overall_satisfaction','snapp_recommend','tapsi_recommend']].mean().round(2)
print(city_sat.to_string())

print('\n=== carpooling satisfaction ===')
print(short.groupby('tapsi_carpooling_familiar')[['tapsi_carpooling_satisfaction_overall','tapsi_overall_satisfaction']].mean().round(2).to_string())
