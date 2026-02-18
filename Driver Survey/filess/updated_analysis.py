"""
Updated Survey Analysis - Including All New Screenshot Analyses
Generates analyses matching Screenshots 1-8 (incentive amounts, payment timing, etc.)
"""

import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path

# Database and output paths
DB_FILE = '/home/claude/survey_analysis.db'
OUTPUT_DIR = Path('/mnt/user-data/outputs')
OUTPUT_DIR.mkdir(exist_ok=True)

def get_incentive_amount_distribution(conn, platform='snapp'):
    """
    Analysis matching Screenshot 1 and 8
    Distribution of incentive amounts received by drivers
    """
    print(f"\nGenerating incentive amount distribution for {platform}...")
    
    if platform == 'snapp':
        column = 'incentive_rial_details_snapp'
        message_col = 'incentive_got_message_snapp'
    else:
        column = 'incentive_rial_details_tapsi'
        message_col = 'incentive_got_message_tapsi'
    
    query = f"""
    SELECT 
        '{platform.upper()}' as platform,
        COUNT(DISTINCT recordID) as total_respondents,
        -- Calculate percentages for each amount range
        ROUND(COUNT(CASE WHEN {column} LIKE '%< 100%' OR {column} LIKE '%<100%' THEN 1 END) * 100.0 / COUNT(*), 1) as "< 100K",
        ROUND(COUNT(CASE WHEN {column} LIKE '%100%200%' OR {column} LIKE '%100_200%' OR {column} LIKE '%100k%200k%' THEN 1 END) * 100.0 / COUNT(*), 1) as "100K-200K",
        ROUND(COUNT(CASE WHEN {column} LIKE '%200%400%' OR {column} LIKE '%200_400%' OR {column} LIKE '%200k%400k%' THEN 1 END) * 100.0 / COUNT(*), 1) as "200K-400K",
        ROUND(COUNT(CASE WHEN {column} LIKE '%400%600%' OR {column} LIKE '%400_600%' OR {column} LIKE '%400k%600k%' THEN 1 END) * 100.0 / COUNT(*), 1) as "400K-600K",
        ROUND(COUNT(CASE WHEN {column} LIKE '%600%800%' OR {column} LIKE '%600_800%' OR {column} LIKE '%600k%800k%' THEN 1 END) * 100.0 / COUNT(*), 1) as "600K-800K",
        ROUND(COUNT(CASE WHEN {column} LIKE '%800%1%' OR {column} LIKE '%800k%1m%' THEN 1 END) * 100.0 / COUNT(*), 1) as "800K-1M",
        ROUND(COUNT(CASE WHEN {column} LIKE '%1m%1.25%' OR {column} LIKE '%1M%1.25M%' THEN 1 END) * 100.0 / COUNT(*), 1) as "1M-1.25M",
        ROUND(COUNT(CASE WHEN {column} LIKE '%1.25%1.5%' THEN 1 END) * 100.0 / COUNT(*), 1) as "1.25M-1.5M",
        ROUND(COUNT(CASE WHEN {column} LIKE '%>1.5%' OR {column} LIKE '%> 1.5%' THEN 1 END) * 100.0 / COUNT(*), 1) as ">1.5M",
        ROUND(COUNT(CASE WHEN {column} IS NULL OR {column} = '' THEN 1 END) * 100.0 / COUNT(*), 1) as "Nothing"
    FROM survey_short
    WHERE {message_col} = 'Yes'
    """
    
    return pd.read_sql(query, conn)

def get_payment_received_analysis(conn):
    """
    Analysis matching Screenshot 2
    Whether drivers received payment in last 3 days
    """
    print("\nGenerating payment received analysis...")
    
    # This analysis requires the long format table with payment questions
    query = """
    SELECT 
        'T30/Tapsi Payment Status' as analysis,
        COUNT(DISTINCT l.recordID) as total_responses,
        ROUND(COUNT(DISTINCT CASE WHEN l.sub_question LIKE '%No%not have%' OR l.sub_question LIKE '%hasn%t%' THEN l.recordID END) * 100.0 / 
              COUNT(DISTINCT l.recordID), 1) as "No, has not have",
        ROUND(COUNT(DISTINCT CASE WHEN l.sub_question LIKE '%Yes%didn%' OR l.sub_question LIKE '%but didn%t do%' THEN l.recordID END) * 100.0 / 
              COUNT(DISTINCT l.recordID), 1) as "Yes, but didnt do",
        ROUND(COUNT(DISTINCT CASE WHEN l.sub_question LIKE '%less than 50K%' OR l.sub_question LIKE '%< 50%' THEN l.recordID END) * 100.0 / 
              COUNT(DISTINCT l.recordID), 1) as "Less than 50K"
    FROM survey_long l
    WHERE l.main_question LIKE '%payment%'
      AND l.main_question LIKE '%last 3 days%'
      AND l.answer IS NOT NULL
    """
    
    return pd.read_sql(query, conn)

def get_inactive_period_analysis(conn):
    """
    Analysis matching Screenshot 3
    How long drivers were inactive before receiving incentive
    """
    print("\nGenerating inactive period analysis...")
    
    query = """
    SELECT 
        inactive_b4_incentive_tapsi as inactive_period,
        COUNT(*) as driver_count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as percentage
    FROM survey_short
    WHERE inactive_b4_incentive_tapsi IS NOT NULL
      AND inactive_b4_incentive_tapsi != ''
      AND incentive_got_message_tapsi = 'Yes'
    GROUP BY inactive_b4_incentive_tapsi
    ORDER BY driver_count DESC
    """
    
    return pd.read_sql(query, conn)

def get_incentive_type_distribution(conn):
    """
    Analysis matching Screenshot 4 and 5
    Types of incentives received (pay per ride, commission free, etc.)
    """
    print("\nGenerating incentive type distribution...")
    
    query = """
    SELECT 
        incentive_category_snapp,
        incentive_category_tapsi,
        COUNT(*) as driver_count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as percentage,
        ROUND(AVG(overall_incentive_satisfaction_snapp), 2) as avg_satisfaction_snapp,
        ROUND(AVG(overall_incentive_satisfaction_tapsi), 2) as avg_satisfaction_tapsi
    FROM survey_short
    WHERE incentive_category_snapp IS NOT NULL OR incentive_category_tapsi IS NOT NULL
    GROUP BY incentive_category_snapp, incentive_category_tapsi
    ORDER BY driver_count DESC
    """
    
    return pd.read_sql(query, conn)

def get_time_limitation_analysis(conn):
    """
    Analysis matching Screenshot 6
    Time limitations on incentives and their impact
    """
    print("\nGenerating time limitation analysis...")
    
    query_snapp = """
    SELECT 
        'Snapp' as platform,
        incentive_time_limitation_snapp as time_limitation,
        COUNT(*) as driver_count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as percentage,
        ROUND(AVG(overall_incentive_satisfaction_snapp), 2) as avg_satisfaction,
        ROUND(COUNT(CASE WHEN incentive_message_participation_snapp = 'Yes' THEN 1 END) * 100.0 / COUNT(*), 1) as participation_rate
    FROM survey_short
    WHERE incentive_time_limitation_snapp IS NOT NULL
      AND incentive_got_message_snapp = 'Yes'
    GROUP BY incentive_time_limitation_snapp
    """
    
    query_tapsi = """
    SELECT 
        'Tapsi' as platform,
        incentive_active_duration_tapsi as time_limitation,
        COUNT(*) as driver_count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as percentage,
        ROUND(AVG(overall_incentive_satisfaction_tapsi), 2) as avg_satisfaction,
        ROUND(COUNT(CASE WHEN incentive_participation_message_tapsi = 'Yes' THEN 1 END) * 100.0 / COUNT(*), 1) as participation_rate
    FROM survey_short
    WHERE incentive_active_duration_tapsi IS NOT NULL
      AND incentive_got_message_tapsi = 'Yes'
    GROUP BY incentive_active_duration_tapsi
    """
    
    df_snapp = pd.read_sql(query_snapp, conn)
    df_tapsi = pd.read_sql(query_tapsi, conn)
    
    return pd.concat([df_snapp, df_tapsi], ignore_index=True)

def get_satisfaction_review(conn):
    """
    Analysis matching Screenshot 7
    Comprehensive satisfaction metrics
    """
    print("\nGenerating satisfaction review...")
    
    query = """
    SELECT 
        cooperation_type,
        CASE 
            WHEN joint_by_signup = 1 AND active_joint = 1 THEN 'Joint Active'
            WHEN joint_by_signup = 1 AND active_joint = 0 THEN 'Joint Inactive'
            ELSE 'Single Platform'
        END as platform_status,
        COUNT(*) as driver_count,
        -- Satisfaction scores
        ROUND(AVG(overall_satisfaction_snapp), 2) as overall_snapp,
        ROUND(AVG(overall_satisfaction_tapsi), 2) as overall_tapsi,
        ROUND(AVG(fare_satisfaction_snapp), 2) as fare_snapp,
        ROUND(AVG(fare_satisfaction_tapsi), 2) as fare_tapsi,
        ROUND(AVG(income_satisfaction_snapp), 2) as income_snapp,
        ROUND(AVG(income_satisfaction_tapsi), 2) as income_tapsi,
        ROUND(AVG(req_count_satisfaction_snapp), 2) as request_snapp,
        ROUND(AVG(req_count_satisfaction_tapsi), 2) as request_tapsi,
        -- Incentive satisfaction
        ROUND(AVG(overall_incentive_satisfaction_snapp), 2) as incentive_sat_snapp,
        ROUND(AVG(overall_incentive_satisfaction_tapsi), 2) as incentive_sat_tapsi
    FROM survey_short
    WHERE cooperation_type IS NOT NULL
    GROUP BY cooperation_type, platform_status
    ORDER BY driver_count DESC
    """
    
    return pd.read_sql(query, conn)

def get_comprehensive_incentive_analysis(conn):
    """
    Comprehensive analysis combining multiple dimensions
    """
    print("\nGenerating comprehensive incentive analysis...")
    
    query = """
    SELECT 
        cooperation_type,
        CASE 
            WHEN joint_by_signup = 1 AND active_joint = 1 THEN 'Joint Active'
            ELSE 'Other'
        END as driver_type,
        -- Message reach
        COUNT(*) as total_drivers,
        COUNT(CASE WHEN incentive_got_message_snapp = 'Yes' THEN 1 END) as snapp_msg_received,
        COUNT(CASE WHEN incentive_got_message_tapsi = 'Yes' THEN 1 END) as tapsi_msg_received,
        ROUND(COUNT(CASE WHEN incentive_got_message_snapp = 'Yes' THEN 1 END) * 100.0 / COUNT(*), 1) as snapp_reach_pct,
        ROUND(COUNT(CASE WHEN incentive_got_message_tapsi = 'Yes' THEN 1 END) * 100.0 / COUNT(*), 1) as tapsi_reach_pct,
        -- Participation
        COUNT(CASE WHEN incentive_message_participation_snapp = 'Yes' THEN 1 END) as snapp_participated,
        COUNT(CASE WHEN incentive_participation_message_tapsi = 'Yes' THEN 1 END) as tapsi_participated,
        ROUND(COUNT(CASE WHEN incentive_message_participation_snapp = 'Yes' THEN 1 END) * 100.0 / 
              NULLIF(COUNT(CASE WHEN incentive_got_message_snapp = 'Yes' THEN 1 END), 0), 1) as snapp_participation_rate,
        ROUND(COUNT(CASE WHEN incentive_participation_message_tapsi = 'Yes' THEN 1 END) * 100.0 / 
              NULLIF(COUNT(CASE WHEN incentive_got_message_tapsi = 'Yes' THEN 1 END), 0), 1) as tapsi_participation_rate,
        -- Amounts
        ROUND(AVG(incentive_snapp), 0) as avg_incentive_snapp,
        ROUND(AVG(incentive_tapsi), 0) as avg_incentive_tapsi,
        -- Impact on rides
        ROUND(AVG(ride_snapp), 1) as avg_rides_snapp,
        ROUND(AVG(ride_tapsi), 1) as avg_rides_tapsi,
        -- Satisfaction
        ROUND(AVG(overall_incentive_satisfaction_snapp), 2) as avg_incentive_sat_snapp,
        ROUND(AVG(overall_incentive_satisfaction_tapsi), 2) as avg_incentive_sat_tapsi
    FROM survey_short
    WHERE cooperation_type IS NOT NULL
    GROUP BY cooperation_type, driver_type
    ORDER BY total_drivers DESC
    """
    
    return pd.read_sql(query, conn)

def get_executive_summary(conn):
    """
    High-level executive summary
    """
    print("\nGenerating executive summary...")
    
    query = """
    SELECT 
        'Key Metrics' as category,
        COUNT(DISTINCT recordID) as total_respondents,
        -- Platform registration
        COUNT(DISTINCT CASE WHEN age_snapp != 'Not Registered' THEN recordID END) as snapp_drivers,
        COUNT(DISTINCT CASE WHEN age_tapsi != 'Not Registered' THEN recordID END) as tapsi_drivers,
        COUNT(DISTINCT CASE WHEN joint_by_signup = 1 AND active_joint = 1 THEN recordID END) as joint_active_drivers,
        -- Satisfaction
        ROUND(AVG(overall_satisfaction_snapp), 2) as avg_satisfaction_snapp,
        ROUND(AVG(overall_satisfaction_tapsi), 2) as avg_satisfaction_tapsi,
        -- Incentive reach
        ROUND(COUNT(CASE WHEN incentive_got_message_snapp = 'Yes' THEN 1 END) * 100.0 / COUNT(*), 1) as snapp_incentive_reach_pct,
        ROUND(COUNT(CASE WHEN incentive_got_message_tapsi = 'Yes' THEN 1 END) * 100.0 / COUNT(*), 1) as tapsi_incentive_reach_pct,
        -- Participation
        ROUND(COUNT(CASE WHEN incentive_message_participation_snapp = 'Yes' THEN 1 END) * 100.0 / 
              NULLIF(COUNT(CASE WHEN incentive_got_message_snapp = 'Yes' THEN 1 END), 0), 1) as snapp_participation_rate,
        ROUND(COUNT(CASE WHEN incentive_participation_message_tapsi = 'Yes' THEN 1 END) * 100.0 / 
              NULLIF(COUNT(CASE WHEN incentive_got_message_tapsi = 'Yes' THEN 1 END), 0), 1) as tapsi_participation_rate,
        -- Activity
        ROUND(AVG(ride_snapp), 1) as avg_rides_snapp,
        ROUND(AVG(ride_tapsi), 1) as avg_rides_tapsi,
        -- Incentive amounts
        ROUND(AVG(incentive_snapp), 0) as avg_incentive_snapp,
        ROUND(AVG(incentive_tapsi), 0) as avg_incentive_tapsi,
        ROUND(SUM(incentive_snapp), 0) as total_incentive_snapp,
        ROUND(SUM(incentive_tapsi), 0) as total_incentive_tapsi
    FROM survey_short
    """
    
    return pd.read_sql(query, conn)

def main():
    """Execute all updated analyses"""
    print("="*80)
    print("UPDATED SURVEY DATA ANALYSIS - INCLUDING NEW SCREENSHOT ANALYSES")
    print("="*80)
    
    # Connect to database
    conn = sqlite3.connect(DB_FILE)
    
    # Store all results
    results = {}
    
    try:
        # Executive Summary
        results['executive_summary'] = get_executive_summary(conn)
        
        # NEW ANALYSES matching screenshots
        results['snapp_incentive_amounts'] = get_incentive_amount_distribution(conn, 'snapp')
        results['tapsi_incentive_amounts'] = get_incentive_amount_distribution(conn, 'tapsi')
        results['payment_received_3days'] = get_payment_received_analysis(conn)
        results['inactive_periods'] = get_inactive_period_analysis(conn)
        results['incentive_types'] = get_incentive_type_distribution(conn)
        results['time_limitations'] = get_time_limitation_analysis(conn)
        results['satisfaction_review'] = get_satisfaction_review(conn)
        results['comprehensive_incentives'] = get_comprehensive_incentive_analysis(conn)
        
    except Exception as e:
        print(f"Error during analysis: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        conn.close()
    
    # Save results
    print("\n" + "="*80)
    print("SAVING RESULTS")
    print("="*80)
    
    output_file = OUTPUT_DIR / 'updated_survey_analysis.xlsx'
    
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        for name, df in results.items():
            if df is not None and not df.empty:
                sheet_name = name[:31]  # Excel sheet name limit
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                print(f"   Saved: {name} ({len(df)} rows)")
    
    print(f"\n✓ Results saved to: {output_file}")
    print("="*80)
    
    return output_file

if __name__ == "__main__":
    main()
