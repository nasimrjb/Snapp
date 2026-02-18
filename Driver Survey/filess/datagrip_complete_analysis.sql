-- ================================================================================
-- COMPREHENSIVE SURVEY DATA ANALYSIS - UPDATED SQL QUERIES FOR DATAGRIP
-- ================================================================================
-- Analysis of Driver Survey Data from Snapp and Tapsi Ride-Sharing Platforms
-- Updated: February 2026
-- 
-- This file contains all SQL queries optimized for DataGrip usage
-- Includes new analyses for incentive amounts and payment timing
-- ================================================================================

-- ================================================================================
-- SETUP: Basic Data Exploration
-- ================================================================================

-- View all records from a specific date range
SELECT *
FROM survey_short_format_single
WHERE datetime >= '2025-01-01'
  AND datetime < '2027-01-01';

-- Count total respondents
SELECT COUNT(DISTINCT recordID) as total_respondents
FROM survey_short_format_single;

-- View sample of long format data
SELECT *
FROM survey_long_format_multichoice
LIMIT 100;

-- ================================================================================
-- NEW ANALYSIS 1: Snapp Drivers - Amount of Incentives Received (Last 7 Days)
-- Matches Screenshot 1 showing incentive amount distribution by city
-- ================================================================================

-- This query needs to extract incentive amount ranges from the long format data
-- The incentive amounts are stored in the 'incentive_rial_details_snapp' column

SELECT 
    'Total' as city,
    COUNT(DISTINCT recordID) as respondents,
    -- Amount ranges as percentages
    ROUND(COUNT(CASE WHEN incentive_rial_details_snapp LIKE '%< 100%' OR incentive_rial_details_snapp LIKE '%<100%' THEN 1 END) * 100.0 / COUNT(DISTINCT recordID), 0) as "< 100 K",
    ROUND(COUNT(CASE WHEN incentive_rial_details_snapp LIKE '%100%250%' OR incentive_rial_details_snapp LIKE '%100_250%' THEN 1 END) * 100.0 / COUNT(DISTINCT recordID), 0) as "100 k - 200 k",
    ROUND(COUNT(CASE WHEN incentive_rial_details_snapp LIKE '%250%500%' OR incentive_rial_details_snapp LIKE '%250_500%' THEN 1 END) * 100.0 / COUNT(DISTINCT recordID), 0) as "200 K - 400 K",
    ROUND(COUNT(CASE WHEN incentive_rial_details_snapp LIKE '%500%750%' OR incentive_rial_details_snapp LIKE '%500_750%' THEN 1 END) * 100.0 / COUNT(DISTINCT recordID), 0) as "400 K - 600 K",
    ROUND(COUNT(CASE WHEN incentive_rial_details_snapp LIKE '%750k%1m%' OR incentive_rial_details_snapp LIKE '%750_1000%' THEN 1 END) * 100.0 / COUNT(DISTINCT recordID), 0) as "600 K - 800 K",
    ROUND(COUNT(CASE WHEN incentive_rial_details_snapp LIKE '%1m%1.25m%' THEN 1 END) * 100.0 / COUNT(DISTINCT recordID), 0) as "800 K - 1 M",
    ROUND(COUNT(CASE WHEN incentive_rial_details_snapp LIKE '%1.25m%1.5m%' THEN 1 END) * 100.0 / COUNT(DISTINCT recordID), 0) as "1 M - 1.25M",
    ROUND(COUNT(CASE WHEN incentive_rial_details_snapp LIKE '%>1.5m%' OR incentive_rial_details_snapp LIKE '%> 1.5%' THEN 1 END) * 100.0 / COUNT(DISTINCT recordID), 0) as ">1.5 M",
    ROUND(COUNT(CASE WHEN incentive_rial_details_snapp IS NULL OR incentive_rial_details_snapp = '' THEN 1 END) * 100.0 / COUNT(DISTINCT recordID), 0) as "Nothing"
FROM survey_short_format_single
WHERE incentive_got_message_snapp = 'Yes';

-- ================================================================================
-- NEW ANALYSIS 2: Payment Received from T30 and Tapsi in Last 3 Days
-- Matches Screenshot 2 showing whether drivers received payments
-- ================================================================================

-- For Tapsi (T30) - Last 3 days payment
SELECT 
    'Total' as summary,
    COUNT(DISTINCT recordID) as total_responses,
    ROUND(COUNT(DISTINCT CASE WHEN l.sub_question LIKE '%No%' OR l.sub_question LIKE '%not have%' THEN l.recordID END) * 100.0 / 
          COUNT(DISTINCT recordID), 0) as "No, has not have",
    ROUND(COUNT(DISTINCT CASE WHEN l.sub_question LIKE '%Yes%didn%' THEN l.recordID END) * 100.0 / 
          COUNT(DISTINCT recordID), 0) as "Yes, but didn't do",
    ROUND(COUNT(DISTINCT CASE WHEN l.sub_question LIKE '%than 50K%' OR l.sub_question LIKE '%< 50%' THEN l.recordID END) * 100.0 / 
          COUNT(DISTINCT recordID), 0) as "Less than 50K"
FROM survey_short_format_single s
LEFT JOIN survey_long_format_multichoice l ON s.recordID = l.recordID
WHERE l.main_question LIKE '%payment%T30%' OR l.main_question LIKE '%payment%Tapsi%'
  AND l.main_question LIKE '%last 3 days%';

-- Detailed breakdown by amount ranges for T30
SELECT 
    'T30 Payment Amounts' as platform,
    COUNT(DISTINCT recordID) as total_responses,
    ROUND(COUNT(CASE WHEN l.answer LIKE '%5K-10K%' THEN 1 END) * 100.0 / COUNT(*), 0) as "5K-10K",
    ROUND(COUNT(CASE WHEN l.answer LIKE '%10K-15K%' THEN 1 END) * 100.0 / COUNT(*), 0) as "10K-15K",
    ROUND(COUNT(CASE WHEN l.answer LIKE '%15K-20K%' THEN 1 END) * 100.0 / COUNT(*), 0) as "15K-20K",
    ROUND(COUNT(CASE WHEN l.answer LIKE '%20K-30K%' THEN 1 END) * 100.0 / COUNT(*), 0) as "20K-300K",
    ROUND(COUNT(CASE WHEN l.answer LIKE '%30K-40K%' THEN 1 END) * 100.0 / COUNT(*), 0) as "30K-40K",
    ROUND(COUNT(CASE WHEN l.answer LIKE '%40K-50K%' THEN 1 END) * 100.0 / COUNT(*), 0) as "40K-50K",
    ROUND(COUNT(CASE WHEN l.answer LIKE '%50K-60K%' THEN 1 END) * 100.0 / COUNT(*), 0) as "50K-60K",
    ROUND(COUNT(CASE WHEN l.answer LIKE '%>60K%' OR l.answer LIKE '%than 60K%' THEN 1 END) * 100.0 / COUNT(*), 0) as "More than 60K"
FROM survey_long_format_multichoice l
WHERE l.main_question LIKE '%payment%T30%'
  AND l.main_question LIKE '%last 3 days%';

-- ================================================================================
-- NEW ANALYSIS 3: Driver Inactive Time Before Receiving Incentive Plan
-- Matches Screenshot 3 showing inactivity periods for T30 and Snapp
-- ================================================================================

-- T30 (Tapsi) inactivity analysis
SELECT 
    inactive_b4_incentive_tapsi as inactive_period,
    COUNT(*) as driver_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 0) as percentage
FROM survey_short_format_single
WHERE inactive_b4_incentive_tapsi IS NOT NULL
  AND inactive_b4_incentive_tapsi != ''
  AND incentive_got_message_tapsi = 'Yes'
GROUP BY inactive_b4_incentive_tapsi
ORDER BY 
    CASE inactive_b4_incentive_tapsi
        WHEN 'Same Day' THEN 1
        WHEN '1_3 Day Before' THEN 2
        WHEN '3_7 Days_Before' THEN 3
        WHEN '8_14 Days Before' THEN 4
        WHEN '15_30 Days_Before' THEN 5
        WHEN '1_2 Month Before' THEN 6
        WHEN '2_3 Month Before' THEN 7
        WHEN '3_6 Month Before' THEN 8
        WHEN '>6 Month Before' THEN 9
        ELSE 10
    END;

-- Snapp inactivity analysis (if available in long format)
SELECT 
    l.sub_question as inactive_period,
    COUNT(DISTINCT l.recordID) as driver_count,
    ROUND(COUNT(DISTINCT l.recordID) * 100.0 / SUM(COUNT(DISTINCT l.recordID)) OVER (), 0) as percentage
FROM survey_long_format_multichoice l
WHERE l.main_question LIKE '%inactive%Snapp%'
  AND l.answer IS NOT NULL
GROUP BY l.sub_question;

-- Combined view
SELECT 
    'Tapsi' as platform,
    inactive_b4_incentive_tapsi as period,
    COUNT(*) as drivers,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY 'Tapsi'), 0) as pct
FROM survey_short_format_single
WHERE inactive_b4_incentive_tapsi IS NOT NULL
GROUP BY inactive_b4_incentive_tapsi

UNION ALL

SELECT 
    'Snapp' as platform,
    'Active in past 24' as period,
    COUNT(*) as drivers,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY 'Snapp'), 0) as pct
FROM survey_short_format_single
WHERE age_snapp IS NOT NULL;

-- ================================================================================
-- NEW ANALYSIS 4: Tapsi Incentive Distribution Based on Mystery
-- Matches Screenshot 4 showing different incentive types
-- ================================================================================

-- Query to analyze incentive distribution types
SELECT 
    'Tapsi Incentive Types' as analysis,
    ROUND(COUNT(CASE WHEN l.sub_question LIKE '%Pay for each ride%' THEN 1 END) * 100.0 / COUNT(*), 0) as "Pay for each ride",
    ROUND(COUNT(CASE WHEN l.sub_question LIKE '%Commission free%' THEN 1 END) * 100.0 / COUNT(*), 0) as "Commission free",
    ROUND(COUNT(CASE WHEN l.sub_question LIKE '%Commission free after number%' THEN 1 END) * 100.0 / COUNT(*), 0) as "Commission free(after number of rides)",
    ROUND(COUNT(CASE WHEN l.sub_question LIKE '%Commission free after%certain income%' THEN 1 END) * 100.0 / COUNT(*), 0) as "Commission free after a certain income",
    ROUND(COUNT(CASE WHEN l.sub_question LIKE '%Income guarantee%' OR l.sub_question LIKE '%Income guaranty%' THEN 1 END) * 100.0 / COUNT(*), 0) as "Income guarantee",
    ROUND(COUNT(CASE WHEN l.sub_question LIKE '%pay after%certain income%' THEN 1 END) * 100.0 / COUNT(*), 0) as "Pay after a certain income"
FROM survey_long_format_multichoice l
WHERE l.main_question LIKE '%incentive%distribution%' OR l.main_question LIKE '%incentive%type%';

-- Snapp incentive distribution
SELECT 
    'Snapp Incentive Types' as analysis,
    ROUND(COUNT(CASE WHEN l.sub_question LIKE '%Pay for each ride%' THEN 1 END) * 100.0 / COUNT(*), 0) as "Pay for each ride",
    ROUND(COUNT(CASE WHEN l.sub_question LIKE '%Commission free%' THEN 1 END) * 100.0 / COUNT(*), 0) as "Commission free",
    ROUND(COUNT(CASE WHEN l.sub_question LIKE '%Commission free after number%' THEN 1 END) * 100.0 / COUNT(*), 0) as "Commission free(after number of rides)",
    ROUND(COUNT(CASE WHEN l.sub_question LIKE '%Income guarantee%' OR l.sub_question LIKE '%Income guaranty%' THEN 1 END) * 100.0 / COUNT(*), 0) as "Income guarantee"
FROM survey_long_format_multichoice l
WHERE l.main_question LIKE '%Snapp%incentive%';

-- ================================================================================
-- NEW ANALYSIS 5: Received Incentive Based on Drivers' Survey
-- Matches Screenshot 5 showing detailed incentive breakdown by type
-- ================================================================================

-- Comprehensive incentive receiving analysis
SELECT 
    'Overall' as segment,
    COUNT(DISTINCT recordID) as total_drivers,
    -- Pay for Each Ride
    ROUND(COUNT(DISTINCT CASE WHEN incentive_category_snapp LIKE '%Money%' OR incentive_category_tapsi LIKE '%Money%' THEN recordID END) * 100.0 / 
          COUNT(DISTINCT recordID), 0) as "Pay for Each Ride",
    -- Earning-Based
    ROUND(COUNT(DISTINCT CASE WHEN l.sub_question LIKE '%Earning%' OR l.sub_question LIKE '%Income%' THEN recordID END) * 100.0 / 
          COUNT(DISTINCT recordID), 0) as "Earning-Based",
    -- Ride-Based
    ROUND(COUNT(DISTINCT CASE WHEN incentive_category_snapp LIKE '%Free-Commission%' OR incentive_category_tapsi LIKE '%Free-Commission%' THEN recordID END) * 100.0 / 
          COUNT(DISTINCT recordID), 0) as "Ride-Based",
    -- Income guarantee
    ROUND(COUNT(DISTINCT CASE WHEN l.sub_question LIKE '%guarantee%' THEN recordID END) * 100.0 / 
          COUNT(DISTINCT recordID), 0) as "Income guarantee"
FROM survey_short_format_single s
LEFT JOIN survey_long_format_multichoice l ON s.recordID = l.recordID
WHERE (s.incentive_got_message_snapp = 'Yes' OR s.incentive_got_message_tapsi = 'Yes');

-- Detailed breakdown by exclusive, joint, taps categories
SELECT 
    CASE 
        WHEN joint_by_signup = 0 THEN 'Exclusive'
        WHEN joint_by_signup = 1 AND active_joint = 1 THEN 'Joint Active'
        WHEN joint_by_signup = 1 AND active_joint = 0 THEN 'Joint Inactive'
    END as driver_type,
    COUNT(DISTINCT recordID) as total,
    -- Multiple incentive type columns as shown in screenshot
    ROUND(AVG(CASE WHEN incentive_category_snapp LIKE '%Money%' THEN 1.0 ELSE 0.0 END) * 100, 0) as "Snapp_Money",
    ROUND(AVG(CASE WHEN incentive_category_snapp LIKE '%Free-Commission%' THEN 1.0 ELSE 0.0 END) * 100, 0) as "Snapp_Free",
    ROUND(AVG(CASE WHEN incentive_category_tapsi LIKE '%Money%' THEN 1.0 ELSE 0.0 END) * 100, 0) as "Tapsi_Money",
    ROUND(AVG(CASE WHEN incentive_category_tapsi LIKE '%Free-Commission%' THEN 1.0 ELSE 0.0 END) * 100, 0) as "Tapsi_Free"
FROM survey_short_format_single
WHERE incentive_got_message_snapp = 'Yes' OR incentive_got_message_tapsi = 'Yes'
GROUP BY driver_type;

-- ================================================================================
-- NEW ANALYSIS 6: Incentive Time Limitation
-- Matches Screenshot 6 showing time constraints on incentives
-- ================================================================================

-- Snapp time limitation analysis
SELECT 
    'Snapp' as platform,
    incentive_time_limitation_snapp as time_limitation,
    COUNT(*) as driver_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 0) as percentage,
    ROUND(AVG(overall_incentive_satisfaction_snapp), 2) as avg_satisfaction,
    ROUND(COUNT(CASE WHEN incentive_message_participation_snapp = 'Yes' THEN 1 END) * 100.0 / COUNT(*), 0) as participation_rate
FROM survey_short_format_single
WHERE incentive_time_limitation_snapp IS NOT NULL
  AND incentive_got_message_snapp = 'Yes'
GROUP BY incentive_time_limitation_snapp
ORDER BY driver_count DESC;

-- Tapsi time limitation analysis
SELECT 
    'Tapsi' as platform,
    incentive_active_duration_tapsi as time_limitation,
    COUNT(*) as driver_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 0) as percentage,
    ROUND(AVG(overall_incentive_satisfaction_tapsi), 2) as avg_satisfaction,
    ROUND(COUNT(CASE WHEN incentive_participation_message_tapsi = 'Yes' THEN 1 END) * 100.0 / COUNT(*), 0) as participation_rate
FROM survey_short_format_single
WHERE incentive_active_duration_tapsi IS NOT NULL
  AND incentive_got_message_tapsi = 'Yes'
GROUP BY incentive_active_duration_tapsi
ORDER BY driver_count DESC;

-- Combined view with WoW (Week over Week) comparison
SELECT 
    'All' as platform,
    COALESCE(s.incentive_time_limitation_snapp, s.incentive_active_duration_tapsi) as time_limit,
    COUNT(*) as total,
    ROUND(AVG(s.overall_incentive_satisfaction_snapp), 2) as snapp_satisfaction,
    ROUND(AVG(s.overall_incentive_satisfaction_tapsi), 2) as tapsi_satisfaction
FROM survey_short_format_single s
WHERE (s.incentive_time_limitation_snapp IS NOT NULL OR s.incentive_active_duration_tapsi IS NOT NULL)
  AND (s.incentive_got_message_snapp = 'Yes' OR s.incentive_got_message_tapsi = 'Yes')
GROUP BY COALESCE(s.incentive_time_limitation_snapp, s.incentive_active_duration_tapsi);

-- ================================================================================
-- NEW ANALYSIS 7: Satisfaction Review Based on Drivers' Survey
-- Matches Screenshot 7 showing detailed satisfaction metrics
-- ================================================================================

-- Comprehensive satisfaction analysis with multiple dimensions
SELECT 
    'Overall' as segment,
    COUNT(DISTINCT recordID) as total_drivers,
    -- Snapp satisfaction metrics
    ROUND(AVG(overall_satisfaction_snapp), 2) as "All Snapp Overall",
    ROUND(AVG(CASE WHEN joint_by_signup = 1 THEN overall_satisfaction_snapp END), 2) as "Joint WoW Snapp Overall",
    ROUND(AVG(overall_satisfaction_tapsi), 2) as "Tapsi Overall",
    ROUND(AVG(CASE WHEN joint_by_signup = 1 THEN overall_satisfaction_tapsi END), 2) as "Joint WoW Tapsi Overall",
    -- Satisfaction (1-5) breakdown
    ROUND(AVG(fare_satisfaction_snapp), 2) as "Avg Fare Satisfaction Snapp",
    ROUND(AVG(fare_satisfaction_tapsi), 2) as "Avg Fare Satisfaction Tapsi",
    ROUND(AVG(income_satisfaction_snapp), 2) as "Avg Income Satisfaction Snapp",
    ROUND(AVG(income_satisfaction_tapsi), 2) as "Avg Income Satisfaction Tapsi",
    ROUND(AVG(req_count_satisfaction_snapp), 2) as "Avg Request Satisfaction Snapp",
    ROUND(AVG(req_count_satisfaction_tapsi), 2) as "Avg Request Satisfaction Tapsi"
FROM survey_short_format_single
WHERE overall_satisfaction_snapp IS NOT NULL OR overall_satisfaction_tapsi IS NOT NULL;

-- Satisfaction by incentive participation
SELECT 
    'Incentive Participation Impact' as analysis,
    CASE 
        WHEN incentive_message_participation_snapp = 'Yes' THEN 'Participated'
        WHEN incentive_got_message_snapp = 'Yes' THEN 'Got Message But Did Not Participate'
        ELSE 'Did Not Get Message'
    END as participation_status,
    COUNT(*) as driver_count,
    ROUND(AVG(overall_satisfaction_snapp), 2) as avg_satisfaction_snapp,
    ROUND(AVG(overall_incentive_satisfaction_snapp), 2) as avg_incentive_satisfaction_snapp,
    ROUND(AVG(ride_snapp), 2) as avg_rides
FROM survey_short_format_single
WHERE overall_satisfaction_snapp IS NOT NULL
GROUP BY participation_status
ORDER BY driver_count DESC;

-- WoW (Week over Week) comparison
SELECT 
    cooperation_type,
    ROUND(AVG(overall_satisfaction_snapp), 2) as current_week_snapp,
    ROUND(AVG(overall_satisfaction_tapsi), 2) as current_week_tapsi,
    -- Note: Historical data comparison would require datetime filtering
    COUNT(*) as driver_count
FROM survey_short_format_single
GROUP BY cooperation_type;

-- ================================================================================
-- NEW ANALYSIS 8: Joint Drivers - Amount of Incentives Received from Tapsi
-- Matches Screenshot 8 showing Tapsi incentive amounts for joint drivers
-- ================================================================================

SELECT 
    'Total Joint Drivers' as segment,
    COUNT(DISTINCT recordID) as respondents,
    -- Amount ranges for Tapsi incentives
    ROUND(COUNT(CASE WHEN incentive_rial_details_tapsi LIKE '%< 100%' OR incentive_rial_details_tapsi LIKE '%<100%' THEN 1 END) * 100.0 / COUNT(DISTINCT recordID), 0) as "< 100 K",
    ROUND(COUNT(CASE WHEN incentive_rial_details_tapsi LIKE '%100%200%' OR incentive_rial_details_tapsi LIKE '%100_200%' THEN 1 END) * 100.0 / COUNT(DISTINCT recordID), 0) as "100 k - 200 k",
    ROUND(COUNT(CASE WHEN incentive_rial_details_tapsi LIKE '%200%400%' OR incentive_rial_details_tapsi LIKE '%200_400%' THEN 1 END) * 100.0 / COUNT(DISTINCT recordID), 0) as "200 K - 400 K",
    ROUND(COUNT(CASE WHEN incentive_rial_details_tapsi LIKE '%400%600%' OR incentive_rial_details_tapsi LIKE '%400_600%' THEN 1 END) * 100.0 / COUNT(DISTINCT recordID), 0) as "400 K - 600 K",
    ROUND(COUNT(CASE WHEN incentive_rial_details_tapsi LIKE '%600%800%' OR incentive_rial_details_tapsi LIKE '%600_800%' THEN 1 END) * 100.0 / COUNT(DISTINCT recordID), 0) as "600 K - 800 K",
    ROUND(COUNT(CASE WHEN incentive_rial_details_tapsi LIKE '%800%1000%' OR incentive_rial_details_tapsi LIKE '%800k%1m%' THEN 1 END) * 100.0 / COUNT(DISTINCT recordID), 0) as "800 K - 1 M",
    ROUND(COUNT(CASE WHEN incentive_rial_details_tapsi LIKE '%1m%1.25m%' OR incentive_rial_details_tapsi LIKE '%1000%1250%' THEN 1 END) * 100.0 / COUNT(DISTINCT recordID), 0) as "1 M - 1.25M",
    ROUND(COUNT(CASE WHEN incentive_rial_details_tapsi LIKE '%1.25%1.5%' THEN 1 END) * 100.0 / COUNT(DISTINCT recordID), 0) as "1.25M - 1.5M",
    ROUND(COUNT(CASE WHEN incentive_rial_details_tapsi LIKE '%>1.5%' OR incentive_rial_details_tapsi LIKE '%> 1.5%' THEN 1 END) * 100.0 / COUNT(DISTINCT recordID), 0) as ">1.5 M",
    ROUND(COUNT(CASE WHEN incentive_rial_details_tapsi IS NULL OR incentive_rial_details_tapsi = '' THEN 1 END) * 100.0 / COUNT(DISTINCT recordID), 0) as "Nothing"
FROM survey_short_format_single
WHERE joint_by_signup = 1
  AND incentive_got_message_tapsi = 'Yes';

-- ================================================================================
-- COMBINED ANALYSES: Cross-platform Comparisons
-- ================================================================================

-- 1. Incentive Effectiveness Comparison (Snapp vs Tapsi)
SELECT 
    'Comparison' as analysis,
    -- Snapp metrics
    COUNT(DISTINCT CASE WHEN incentive_got_message_snapp = 'Yes' THEN recordID END) as snapp_reached,
    COUNT(DISTINCT CASE WHEN incentive_message_participation_snapp = 'Yes' THEN recordID END) as snapp_participated,
    ROUND(COUNT(DISTINCT CASE WHEN incentive_message_participation_snapp = 'Yes' THEN recordID END) * 100.0 / 
          NULLIF(COUNT(DISTINCT CASE WHEN incentive_got_message_snapp = 'Yes' THEN recordID END), 0), 1) as snapp_conversion_rate,
    ROUND(AVG(CASE WHEN incentive_got_message_snapp = 'Yes' THEN overall_incentive_satisfaction_snapp END), 2) as snapp_avg_satisfaction,
    -- Tapsi metrics
    COUNT(DISTINCT CASE WHEN incentive_got_message_tapsi = 'Yes' THEN recordID END) as tapsi_reached,
    COUNT(DISTINCT CASE WHEN incentive_participation_message_tapsi = 'Yes' THEN recordID END) as tapsi_participated,
    ROUND(COUNT(DISTINCT CASE WHEN incentive_participation_message_tapsi = 'Yes' THEN recordID END) * 100.0 / 
          NULLIF(COUNT(DISTINCT CASE WHEN incentive_got_message_tapsi = 'Yes' THEN recordID END), 0), 1) as tapsi_conversion_rate,
    ROUND(AVG(CASE WHEN incentive_got_message_tapsi = 'Yes' THEN overall_incentive_satisfaction_tapsi END), 2) as tapsi_avg_satisfaction
FROM survey_short_format_single
WHERE age_snapp IS NOT NULL;

-- 2. Driver Segmentation with Incentive Impact
SELECT 
    cooperation_type,
    CASE 
        WHEN joint_by_signup = 1 AND active_joint = 1 THEN 'Joint Active'
        WHEN joint_by_signup = 1 AND active_joint = 0 THEN 'Joint Inactive'
        ELSE 'Single Platform'
    END as platform_usage,
    COUNT(*) as driver_count,
    -- Incentive metrics
    ROUND(AVG(CASE WHEN incentive_got_message_snapp = 'Yes' THEN 1.0 ELSE 0.0 END) * 100, 1) as pct_got_snapp_incentive,
    ROUND(AVG(CASE WHEN incentive_got_message_tapsi = 'Yes' THEN 1.0 ELSE 0.0 END) * 100, 1) as pct_got_tapsi_incentive,
    ROUND(AVG(incentive_snapp), 0) as avg_incentive_amount_snapp,
    ROUND(AVG(incentive_tapsi), 0) as avg_incentive_amount_tapsi,
    -- Satisfaction
    ROUND(AVG(overall_satisfaction_snapp), 2) as avg_satisfaction_snapp,
    ROUND(AVG(overall_satisfaction_tapsi), 2) as avg_satisfaction_tapsi,
    -- Activity
    ROUND(AVG(ride_snapp), 1) as avg_rides_snapp,
    ROUND(AVG(ride_tapsi), 1) as avg_rides_tapsi
FROM survey_short_format_single
WHERE cooperation_type IS NOT NULL
GROUP BY cooperation_type, platform_usage
ORDER BY driver_count DESC;

-- 3. Incentive ROI Analysis (rides generated vs incentive amount)
SELECT 
    incentive_category_snapp,
    incentive_category_tapsi,
    COUNT(*) as driver_count,
    ROUND(AVG(incentive_snapp), 0) as avg_incentive_snapp,
    ROUND(AVG(incentive_tapsi), 0) as avg_incentive_tapsi,
    ROUND(AVG(ride_snapp), 1) as avg_rides_snapp,
    ROUND(AVG(ride_tapsi), 1) as avg_rides_tapsi,
    -- ROI calculation (rides per 1000 currency units of incentive)
    ROUND(AVG(ride_snapp) * 1000.0 / NULLIF(AVG(incentive_snapp), 0), 2) as rides_per_1000_snapp,
    ROUND(AVG(ride_tapsi) * 1000.0 / NULLIF(AVG(incentive_tapsi), 0), 2) as rides_per_1000_tapsi
FROM survey_short_format_single
WHERE (incentive_category_snapp IS NOT NULL OR incentive_category_tapsi IS NOT NULL)
  AND (incentive_snapp > 0 OR incentive_tapsi > 0)
GROUP BY incentive_category_snapp, incentive_category_tapsi
ORDER BY driver_count DESC;

-- ================================================================================
-- ADVANCED QUERIES: City-Level Analysis
-- ================================================================================

-- Note: City information would need to be derived from location codes or joined from a separate table
-- This is a template query structure

-- City-level incentive analysis
WITH city_mapping AS (
    SELECT 
        recordID,
        CASE 
            WHEN snapp_LOC BETWEEN 1 AND 5 THEN 'Tehran'
            WHEN snapp_LOC BETWEEN 6 AND 10 THEN 'Karaj'
            WHEN snapp_LOC BETWEEN 11 AND 15 THEN 'Isfahan'
            -- Add more city mappings based on actual location codes
            ELSE 'Other'
        END as city
    FROM survey_short_format_single
    WHERE snapp_LOC IS NOT NULL
)
SELECT 
    cm.city,
    COUNT(DISTINCT s.recordID) as driver_count,
    ROUND(AVG(s.overall_satisfaction_snapp), 2) as avg_satisfaction,
    ROUND(AVG(s.incentive_snapp), 0) as avg_incentive_amount,
    ROUND(COUNT(CASE WHEN s.incentive_got_message_snapp = 'Yes' THEN 1 END) * 100.0 / COUNT(*), 1) as incentive_reach_pct,
    ROUND(AVG(s.ride_snapp), 1) as avg_rides
FROM survey_short_format_single s
JOIN city_mapping cm ON s.recordID = cm.recordID
GROUP BY cm.city
ORDER BY driver_count DESC;

-- ================================================================================
-- DATA QUALITY AND VALIDATION QUERIES
-- ================================================================================

-- Check for missing incentive data
SELECT 
    'Missing Incentive Data Check' as check_type,
    COUNT(*) as total_records,
    COUNT(CASE WHEN incentive_got_message_snapp IS NULL THEN 1 END) as missing_snapp_message,
    COUNT(CASE WHEN incentive_got_message_tapsi IS NULL THEN 1 END) as missing_tapsi_message,
    COUNT(CASE WHEN incentive_rial_details_snapp IS NULL THEN 1 END) as missing_snapp_amount,
    COUNT(CASE WHEN incentive_rial_details_tapsi IS NULL THEN 1 END) as missing_tapsi_amount
FROM survey_short_format_single;

-- Validate date ranges
SELECT 
    MIN(datetime) as earliest_response,
    MAX(datetime) as latest_response,
    COUNT(DISTINCT DATE(datetime)) as unique_days,
    COUNT(*) as total_responses
FROM survey_short_format_single;

-- Check response distribution by platform
SELECT 
    CASE 
        WHEN age_snapp != 'Not Registered' AND (age_tapsi = 'Not Registered' OR age_tapsi IS NULL) THEN 'Snapp Only'
        WHEN age_tapsi != 'Not Registered' AND (age_snapp = 'Not Registered' OR age_snapp IS NULL) THEN 'Tapsi Only'
        WHEN age_snapp != 'Not Registered' AND age_tapsi != 'Not Registered' THEN 'Both Platforms'
        ELSE 'Unknown'
    END as platform_registration,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM survey_short_format_single
GROUP BY platform_registration;

-- ================================================================================
-- EXPORT-READY SUMMARY QUERY
-- ================================================================================

-- Comprehensive summary for executive reporting
SELECT 
    'Executive Summary' as report_section,
    COUNT(DISTINCT recordID) as total_respondents,
    -- Platform reach
    COUNT(DISTINCT CASE WHEN age_snapp != 'Not Registered' THEN recordID END) as snapp_drivers,
    COUNT(DISTINCT CASE WHEN age_tapsi != 'Not Registered' THEN recordID END) as tapsi_drivers,
    COUNT(DISTINCT CASE WHEN joint_by_signup = 1 AND active_joint = 1 THEN recordID END) as joint_active_drivers,
    -- Satisfaction scores
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
    -- Average rides
    ROUND(AVG(ride_snapp), 1) as avg_rides_snapp,
    ROUND(AVG(ride_tapsi), 1) as avg_rides_tapsi,
    -- Average incentive amounts
    ROUND(AVG(incentive_snapp), 0) as avg_incentive_amount_snapp,
    ROUND(AVG(incentive_tapsi), 0) as avg_incentive_amount_tapsi
FROM survey_short_format_single;

-- ================================================================================
-- END OF COMPREHENSIVE SQL QUERIES
-- ================================================================================

/*
USAGE NOTES FOR DATAGRIP:
-------------------------

1. Execute queries individually or in sections
2. Use Ctrl+Enter (or Cmd+Enter on Mac) to run selected query
3. Results will appear in the results pane below
4. Export results using the export button in results pane

5. For date filtering, modify the WHERE clause:
   WHERE datetime >= '2025-01-01' AND datetime < '2026-01-01'

6. For city-level analysis, you may need to:
   - Create a city mapping table
   - Join with external city data
   - Use the snapp_LOC and tapsi_LOC fields

7. Column name reference:
   - recordID: Unique identifier for each respondent
   - datetime: Survey response timestamp
   - incentive_rial_details_snapp/tapsi: Incentive amount information
   - incentive_got_message_snapp/tapsi: Whether driver received incentive message
   - overall_satisfaction_snapp/tapsi: Overall satisfaction score (1-5)
   - cooperation_type: Full-Time, Part-Time, etc.
   - joint_by_signup: Whether driver registered on both platforms
   - active_joint: Whether driver is active on both platforms

8. For performance optimization:
   - Add indexes on frequently queried columns (recordID, datetime)
   - Use EXPLAIN QUERY PLAN to analyze query performance
   - Consider creating materialized views for complex aggregations

9. Common filters to use:
   - Active drivers: WHERE ride_snapp > 0 OR ride_tapsi > 0
   - Recent responses: WHERE datetime >= date('now', '-7 days')
   - Dissatisfied drivers: WHERE overall_satisfaction_snapp <= 3
   - Incentive participants: WHERE incentive_got_message_snapp = 'Yes'
*/
