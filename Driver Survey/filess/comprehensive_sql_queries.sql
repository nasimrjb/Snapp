-- ================================================================================
-- COMPREHENSIVE SURVEY DATA ANALYSIS - SQL QUERIES
-- ================================================================================
-- Analysis of Driver Survey Data from Snapp and Tapsi Ride-Sharing Platforms
-- Generated: February 2026
-- 
-- This document contains all SQL queries used to analyze the survey data
-- The data can be merged using the recordID column between the two main tables
-- ================================================================================

-- TABLE OF CONTENTS:
-- 1. Data Loading and Table Structure
-- 2. Dissatisfaction Analysis
-- 3. Commission-Free Ride Analysis
-- 4. Inactive Time Analysis
-- 5. Driver Persona Analysis
-- 6. Ride Share Distribution
-- 7. Incentive Analysis
-- 8. Satisfaction Metrics
-- 9. Demographics Analysis
-- 10. Registration and Referral Analysis
-- 11. Navigation App Usage
-- 12. Carpooling Analysis
-- 13. Summary Statistics

-- ================================================================================
-- 1. DATA LOADING AND TABLE STRUCTURE
-- ================================================================================

-- IMPORTANT: To merge the two sheets (survey_short and survey_long):
-- Both tables share the 'recordID' column as the primary key
-- Use JOIN operations on recordID to combine data from both tables

-- Example merge query:
-- SELECT 
--     s.*,
--     l.main_question,
--     l.sub_question,
--     l.answer
-- FROM survey_short s
-- LEFT JOIN survey_long l ON s.recordID = l.recordID
-- WHERE l.main_question = 'specific_question';

-- ================================================================================
-- 2. DISSATISFACTION ANALYSIS
-- Query drivers who gave low ratings (<=3) and their reasons
-- ================================================================================

-- 2.1 Overall dissatisfaction reasons for all drivers
SELECT 
    main_question as reason_category,
    sub_question as specific_reason,
    COUNT(DISTINCT l.recordID) as driver_count,
    ROUND(COUNT(DISTINCT l.recordID) * 100.0 / 
        (SELECT COUNT(DISTINCT recordID) FROM survey_short_format_single WHERE overall_satisfaction_snapp <= 3), 2) as percentage
FROM survey_long_format_multichoice l
JOIN survey_short_format_single s ON l.recordID = s.recordID
WHERE s.overall_satisfaction_snapp <= 3
  AND l.main_question LIKE '%diss%'
  AND l.answer IS NOT NULL
GROUP BY main_question, sub_question
ORDER BY driver_count DESC;

-- 2.2 Dissatisfaction by driver type (Joint vs Single platform)
SELECT 
    CASE 
        WHEN s.joint_by_signup = 1 AND s.active_joint = 1 THEN 'Joint Active Drivers'
        WHEN s.joint_by_signup = 1 AND s.active_joint = 0 THEN 'Joint Snapp Only'
        ELSE 'Snapp Only'
    END as driver_type,
    l.sub_question as dissatisfaction_reason,
    COUNT(DISTINCT l.recordID) as count,
    ROUND(COUNT(DISTINCT l.recordID) * 100.0 / SUM(COUNT(DISTINCT l.recordID)) OVER (PARTITION BY 
        CASE 
            WHEN s.joint_by_signup = 1 AND s.active_joint = 1 THEN 'Joint Active Drivers'
            WHEN s.joint_by_signup = 1 AND s.active_joint = 0 THEN 'Joint Snapp Only'
            ELSE 'Snapp Only'
        END), 2) as percentage_within_group
FROM survey_long_format_multichoice l
JOIN survey_short_format_single s ON l.recordID = s.recordID
WHERE s.overall_satisfaction_snapp <= 3
  AND l.main_question LIKE '%unsatis%snapp%'
  AND l.answer IS NOT NULL
GROUP BY driver_type, l.sub_question
ORDER BY driver_type, count DESC;

-- 2.3 Satisfaction scores by specific satisfaction dimensions
SELECT 
    cooperation_type,
    COUNT(*) as driver_count,
    ROUND(AVG(overall_satisfaction_snapp), 2) as avg_overall_satisfaction,
    ROUND(AVG(fare_satisfaction_snapp), 2) as avg_fare_satisfaction,
    ROUND(AVG(income_satisfaction_snapp), 2) as avg_income_satisfaction,
    ROUND(AVG(req_count_satisfaction_snapp), 2) as avg_request_count_satisfaction,
    -- Percentage with low satisfaction
    ROUND(COUNT(CASE WHEN overall_satisfaction_snapp <= 3 THEN 1 END) * 100.0 / COUNT(*), 2) as pct_low_satisfaction
FROM survey_short
WHERE cooperation_type IS NOT NULL
GROUP BY cooperation_type
ORDER BY driver_count DESC;

-- ================================================================================
-- 3. COMMISSION-FREE RIDE ANALYSIS
-- Analyze commission-free rides and their impact
-- ================================================================================

-- 3.1 Overall commission-free ride statistics
SELECT 
    COUNT(DISTINCT recordID) as total_drivers,
    -- Snapp metrics
    ROUND(AVG(ride_snapp), 2) as avg_total_rides_snapp,
    ROUND(AVG(commfree_disc_ride_snapp), 2) as avg_commfree_rides_snapp,
    ROUND(AVG(commfree_disc_ride_snapp) * 100.0 / NULLIF(AVG(ride_snapp), 0), 2) as pct_rides_commfree_snapp,
    ROUND(SUM(commfree_snapp), 0) as total_commfree_amount_snapp,
    ROUND(AVG(commfree_snapp), 0) as avg_commfree_amount_per_driver_snapp,
    -- Tapsi metrics
    ROUND(AVG(ride_tapsi), 2) as avg_total_rides_tapsi,
    ROUND(AVG(commfree_disc_ride_tapsi), 2) as avg_commfree_rides_tapsi,
    ROUND(AVG(commfree_disc_ride_tapsi) * 100.0 / NULLIF(AVG(ride_tapsi), 0), 2) as pct_rides_commfree_tapsi,
    ROUND(SUM(commfree_tapsi), 0) as total_commfree_amount_tapsi,
    ROUND(AVG(commfree_tapsi), 0) as avg_commfree_amount_per_driver_tapsi,
    -- Penetration rates
    ROUND(COUNT(CASE WHEN commfree_disc_ride_snapp > 0 THEN 1 END) * 100.0 / COUNT(*), 2) as pct_drivers_got_commfree_snapp,
    ROUND(COUNT(CASE WHEN commfree_disc_ride_tapsi > 0 THEN 1 END) * 100.0 / COUNT(*), 2) as pct_drivers_got_commfree_tapsi
FROM survey_short
WHERE ride_snapp IS NOT NULL OR ride_tapsi IS NOT NULL;

-- 3.2 Commission-free rides by driver type
SELECT 
    cooperation_type,
    CASE 
        WHEN joint_by_signup = 1 AND active_joint = 1 THEN 'Joint Active'
        WHEN joint_by_signup = 1 AND active_joint = 0 THEN 'Joint Inactive'
        ELSE 'Single Platform'
    END as joint_status,
    COUNT(*) as driver_count,
    ROUND(AVG(commfree_disc_ride_snapp), 2) as avg_commfree_rides_snapp,
    ROUND(AVG(commfree_disc_ride_tapsi), 2) as avg_commfree_rides_tapsi,
    ROUND(AVG(commfree_snapp), 0) as avg_commfree_amount_snapp,
    ROUND(AVG(commfree_tapsi), 0) as avg_commfree_amount_tapsi,
    ROUND(AVG(diff_commfree_snapp), 2) as avg_diff_commfree_snapp,
    ROUND(AVG(diff_commfree_tapsi), 2) as avg_diff_commfree_tapsi
FROM survey_short
GROUP BY cooperation_type, joint_status
ORDER BY driver_count DESC;

-- ================================================================================
-- 4. INACTIVE TIME ANALYSIS (Tapsi)
-- Analyze time drivers were inactive before receiving incentives
-- ================================================================================

-- 4.1 Distribution of inactive time periods before incentive
SELECT 
    inactive_b4_incentive_tapsi as inactive_period,
    COUNT(*) as driver_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage,
    ROUND(AVG(overall_incentive_satisfaction_tapsi), 2) as avg_incentive_satisfaction,
    ROUND(AVG(ride_tapsi), 2) as avg_rides_after_incentive
FROM survey_short
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

-- ================================================================================
-- 5. DRIVER PERSONA ANALYSIS
-- Categorize and analyze drivers by work patterns
-- ================================================================================

-- 5.1 Driver distribution by cooperation type
SELECT 
    cooperation_type,
    COUNT(*) as driver_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage,
    ROUND(AVG(ride_snapp), 2) as avg_rides_snapp,
    ROUND(AVG(ride_tapsi), 2) as avg_rides_tapsi,
    ROUND(AVG(overall_satisfaction_snapp), 2) as avg_satisfaction_snapp
FROM survey_short
WHERE cooperation_type IS NOT NULL
GROUP BY cooperation_type
ORDER BY driver_count DESC;

-- 5.2 Part-time vs Full-time driver analysis
SELECT 
    cooperation_type,
    active_time,
    COUNT(*) as driver_count,
    ROUND(AVG(ride_snapp), 2) as avg_rides_snapp,
    ROUND(AVG(overall_satisfaction_snapp), 2) as avg_satisfaction,
    ROUND(AVG(CASE WHEN joint_by_signup = 1 THEN 1.0 ELSE 0.0 END) * 100, 2) as pct_joint_drivers
FROM survey_short
WHERE cooperation_type IS NOT NULL AND active_time IS NOT NULL
GROUP BY cooperation_type, active_time
ORDER BY driver_count DESC;

-- 5.3 Joint driver activation status
SELECT 
    CASE 
        WHEN joint_by_signup = 1 AND active_joint = 1 THEN 'Both Platforms Active'
        WHEN joint_by_signup = 1 AND active_joint = 0 THEN 'Registered Both, Snapp Only Active'
        ELSE 'Snapp Only'
    END as driver_category,
    COUNT(*) as driver_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM survey_short
GROUP BY driver_category;

-- ================================================================================
-- 6. RIDE SHARE DISTRIBUTION
-- Analyze how rides are distributed between platforms
-- ================================================================================

-- 6.1 Overall ride share between Snapp and Tapsi
SELECT 
    CASE 
        WHEN joint_by_signup = 1 AND active_joint = 1 THEN 'Joint Active'
        WHEN joint_by_signup = 1 AND active_joint = 0 THEN 'Joint Inactive'
        ELSE 'Single Platform'
    END as driver_category,
    COUNT(DISTINCT recordID) as driver_count,
    -- Total rides
    ROUND(SUM(ride_snapp), 0) as total_snapp_rides,
    ROUND(SUM(COALESCE(ride_tapsi, 0)), 0) as total_tapsi_rides,
    ROUND(SUM(ride_snapp + COALESCE(ride_tapsi, 0)), 0) as total_all_rides,
    -- Average rides per driver
    ROUND(AVG(ride_snapp), 2) as avg_snapp_rides_per_driver,
    ROUND(AVG(ride_tapsi), 2) as avg_tapsi_rides_per_driver,
    -- Market share percentages
    ROUND(SUM(ride_snapp) * 100.0 / SUM(ride_snapp + COALESCE(ride_tapsi, 0)), 2) as snapp_ride_share_pct,
    ROUND(SUM(COALESCE(ride_tapsi, 0)) * 100.0 / SUM(ride_snapp + COALESCE(ride_tapsi, 0)), 2) as tapsi_ride_share_pct
FROM survey_short
WHERE ride_snapp IS NOT NULL
GROUP BY driver_category
ORDER BY driver_count DESC;

-- 6.2 Ride distribution by cooperation type
SELECT 
    cooperation_type,
    COUNT(*) as driver_count,
    ROUND(AVG(ride_snapp), 2) as avg_snapp,
    ROUND(AVG(ride_tapsi), 2) as avg_tapsi,
    ROUND(AVG(ride_snapp) / NULLIF(AVG(ride_snapp + COALESCE(ride_tapsi, 0)), 0) * 100, 2) as snapp_share_pct,
    ROUND(AVG(ride_tapsi) / NULLIF(AVG(ride_snapp + COALESCE(ride_tapsi, 0)), 0) * 100, 2) as tapsi_share_pct
FROM survey_short
WHERE cooperation_type IS NOT NULL
GROUP BY cooperation_type
ORDER BY driver_count DESC;

-- ================================================================================
-- 7. INCENTIVE ANALYSIS
-- Comprehensive analysis of incentive programs
-- ================================================================================

-- 7.1 Incentive message reach and participation
SELECT 
    'Snapp' as platform,
    COUNT(DISTINCT recordID) as total_drivers,
    COUNT(DISTINCT CASE WHEN incentive_got_message_snapp = 'Yes' THEN recordID END) as drivers_got_message,
    COUNT(DISTINCT CASE WHEN incentive_message_participation_snapp = 'Yes' THEN recordID END) as drivers_participated,
    ROUND(COUNT(DISTINCT CASE WHEN incentive_got_message_snapp = 'Yes' THEN recordID END) * 100.0 / 
          COUNT(DISTINCT recordID), 2) as pct_got_message,
    ROUND(COUNT(DISTINCT CASE WHEN incentive_message_participation_snapp = 'Yes' THEN recordID END) * 100.0 / 
          NULLIF(COUNT(DISTINCT CASE WHEN incentive_got_message_snapp = 'Yes' THEN recordID END), 0), 2) as pct_participated_of_reached,
    ROUND(AVG(overall_incentive_satisfaction_snapp), 2) as avg_incentive_satisfaction
FROM survey_short
WHERE age_snapp IS NOT NULL

UNION ALL

SELECT 
    'Tapsi' as platform,
    COUNT(DISTINCT recordID) as total_drivers,
    COUNT(DISTINCT CASE WHEN incentive_got_message_tapsi = 'Yes' THEN recordID END) as drivers_got_message,
    COUNT(DISTINCT CASE WHEN incentive_participation_message_tapsi = 'Yes' THEN recordID END) as drivers_participated,
    ROUND(COUNT(DISTINCT CASE WHEN incentive_got_message_tapsi = 'Yes' THEN recordID END) * 100.0 / 
          COUNT(DISTINCT recordID), 2) as pct_got_message,
    ROUND(COUNT(DISTINCT CASE WHEN incentive_participation_message_tapsi = 'Yes' THEN recordID END) * 100.0 / 
          NULLIF(COUNT(DISTINCT CASE WHEN incentive_got_message_tapsi = 'Yes' THEN recordID END), 0), 2) as pct_participated_of_reached,
    ROUND(AVG(overall_incentive_satisfaction_tapsi), 2) as avg_incentive_satisfaction
FROM survey_short
WHERE age_tapsi IS NOT NULL AND age_tapsi != 'Not Registered';

-- 7.2 Incentive categories and effectiveness
SELECT 
    incentive_category_snapp,
    incentive_category_tapsi,
    COUNT(*) as driver_count,
    ROUND(AVG(incentive_snapp), 0) as avg_incentive_amount_snapp,
    ROUND(AVG(incentive_tapsi), 0) as avg_incentive_amount_tapsi,
    ROUND(AVG(overall_incentive_satisfaction_snapp), 2) as avg_satisfaction_snapp,
    ROUND(AVG(overall_incentive_satisfaction_tapsi), 2) as avg_satisfaction_tapsi,
    ROUND(AVG(ride_snapp), 2) as avg_rides_snapp,
    ROUND(AVG(ride_tapsi), 2) as avg_rides_tapsi
FROM survey_short
WHERE incentive_category_snapp IS NOT NULL OR incentive_category_tapsi IS NOT NULL
GROUP BY incentive_category_snapp, incentive_category_tapsi
ORDER BY driver_count DESC;

-- 7.3 Time limitation impact on participation
SELECT 
    'Snapp' as platform,
    incentive_time_limitation_snapp as time_limitation,
    COUNT(*) as driver_count,
    ROUND(COUNT(CASE WHEN incentive_message_participation_snapp = 'Yes' THEN 1 END) * 100.0 / COUNT(*), 2) as participation_rate,
    ROUND(AVG(overall_incentive_satisfaction_snapp), 2) as avg_satisfaction
FROM survey_short
WHERE incentive_time_limitation_snapp IS NOT NULL
  AND incentive_got_message_snapp = 'Yes'
GROUP BY incentive_time_limitation_snapp

UNION ALL

SELECT 
    'Tapsi' as platform,
    incentive_active_duration_tapsi as time_limitation,
    COUNT(*) as driver_count,
    ROUND(COUNT(CASE WHEN incentive_participation_message_tapsi = 'Yes' THEN 1 END) * 100.0 / COUNT(*), 2) as participation_rate,
    ROUND(AVG(overall_incentive_satisfaction_tapsi), 2) as avg_satisfaction
FROM survey_short
WHERE incentive_active_duration_tapsi IS NOT NULL
  AND incentive_got_message_tapsi = 'Yes'
GROUP BY incentive_active_duration_tapsi
ORDER BY platform, driver_count DESC;

-- ================================================================================
-- 8. SATISFACTION METRICS
-- Detailed satisfaction analysis across dimensions
-- ================================================================================

-- 8.1 Multi-dimensional satisfaction comparison
SELECT 
    CASE 
        WHEN joint_by_signup = 1 AND active_joint = 1 THEN 'Joint Active'
        WHEN joint_by_signup = 1 AND active_joint = 0 THEN 'Joint Inactive'
        ELSE 'Single Platform'
    END as driver_type,
    cooperation_type,
    COUNT(*) as driver_count,
    -- Snapp satisfaction dimensions
    ROUND(AVG(overall_satisfaction_snapp), 2) as overall_sat_snapp,
    ROUND(AVG(fare_satisfaction_snapp), 2) as fare_sat_snapp,
    ROUND(AVG(income_satisfaction_snapp), 2) as income_sat_snapp,
    ROUND(AVG(req_count_satisfaction_snapp), 2) as request_sat_snapp,
    -- Tapsi satisfaction dimensions
    ROUND(AVG(overall_satisfaction_tapsi), 2) as overall_sat_tapsi,
    ROUND(AVG(fare_satisfaction_tapsi), 2) as fare_sat_tapsi,
    ROUND(AVG(income_satisfaction_tapsi), 2) as income_sat_tapsi,
    ROUND(AVG(req_count_satisfaction_tapsi), 2) as request_sat_tapsi
FROM survey_short
WHERE cooperation_type IS NOT NULL
GROUP BY driver_type, cooperation_type
ORDER BY driver_count DESC;

-- 8.2 Satisfaction distribution (1-5 scale)
SELECT 
    overall_satisfaction_snapp as satisfaction_score,
    COUNT(*) as driver_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage,
    ROUND(AVG(ride_snapp), 2) as avg_rides
FROM survey_short
WHERE overall_satisfaction_snapp IS NOT NULL
GROUP BY overall_satisfaction_snapp
ORDER BY overall_satisfaction_snapp;

-- ================================================================================
-- 9. DEMOGRAPHICS ANALYSIS
-- Analyze driver demographics and their impact
-- ================================================================================

-- 9.1 Age group analysis
SELECT 
    age_group,
    COUNT(*) as driver_count,
    ROUND(AVG(ride_snapp), 2) as avg_rides_snapp,
    ROUND(AVG(overall_satisfaction_snapp), 2) as avg_satisfaction_snapp,
    ROUND(COUNT(CASE WHEN joint_by_signup = 1 THEN 1 END) * 100.0 / COUNT(*), 2) as pct_joint_drivers,
    ROUND(COUNT(CASE WHEN cooperation_type = 'Full-Time' THEN 1 END) * 100.0 / COUNT(*), 2) as pct_fulltime
FROM survey_short
WHERE age_group IS NOT NULL
GROUP BY age_group
ORDER BY driver_count DESC;

-- 9.2 Education level analysis
SELECT 
    education,
    COUNT(*) as driver_count,
    ROUND(AVG(ride_snapp), 2) as avg_rides,
    ROUND(AVG(overall_satisfaction_snapp), 2) as avg_satisfaction,
    ROUND(AVG(CASE WHEN cooperation_type = 'Full-Time' THEN 1.0 ELSE 0.0 END) * 100, 2) as pct_fulltime
FROM survey_short
WHERE education IS NOT NULL
GROUP BY education
ORDER BY driver_count DESC;

-- 9.3 Combined demographics
SELECT 
    age_group,
    gender,
    marital_status,
    COUNT(*) as driver_count,
    ROUND(AVG(overall_satisfaction_snapp), 2) as avg_satisfaction
FROM survey_short
WHERE age_group IS NOT NULL AND gender IS NOT NULL
GROUP BY age_group, gender, marital_status
HAVING COUNT(*) >= 10
ORDER BY driver_count DESC;

-- ================================================================================
-- 10. REGISTRATION AND REFERRAL ANALYSIS
-- Analyze how drivers joined and referral patterns
-- ================================================================================

-- 10.1 Registration channels
SELECT 
    'Snapp' as platform,
    register_type_snapp as registration_type,
    main_reg_reason_snapp as main_reason,
    COUNT(*) as driver_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY 'Snapp'), 2) as percentage,
    ROUND(AVG(overall_satisfaction_snapp), 2) as avg_satisfaction,
    ROUND(AVG(ride_snapp), 2) as avg_rides
FROM survey_short
WHERE register_type_snapp IS NOT NULL
GROUP BY register_type_snapp, main_reg_reason_snapp

UNION ALL

SELECT 
    'Tapsi' as platform,
    register_type_tapsi as registration_type,
    main_reg_reason_tapsi as main_reason,
    COUNT(*) as driver_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY 'Tapsi'), 2) as percentage,
    ROUND(AVG(overall_satisfaction_tapsi), 2) as avg_satisfaction,
    ROUND(AVG(ride_tapsi), 2) as avg_rides
FROM survey_short
WHERE register_type_tapsi IS NOT NULL AND age_tapsi != 'Not Registered'
GROUP BY register_type_tapsi, main_reg_reason_tapsi
ORDER BY platform, driver_count DESC;

-- 10.2 Referral effectiveness
SELECT 
    CASE 
        WHEN main_reg_reason_snapp LIKE '%Friend%' OR main_reg_reason_snapp LIKE '%Family%' THEN 'Referral'
        WHEN main_reg_reason_snapp LIKE '%ADS%' THEN 'Advertising'
        ELSE 'Other'
    END as acquisition_channel,
    COUNT(*) as driver_count,
    ROUND(AVG(recommend_snapp), 2) as avg_recommend_score,
    ROUND(AVG(overall_satisfaction_snapp), 2) as avg_satisfaction,
    ROUND(AVG(ride_snapp), 2) as avg_rides
FROM survey_short
WHERE main_reg_reason_snapp IS NOT NULL
GROUP BY acquisition_channel
ORDER BY driver_count DESC;

-- ================================================================================
-- 11. NAVIGATION APP USAGE
-- Analyze which navigation apps drivers use
-- ================================================================================

SELECT 
    l.sub_question as navigation_app,
    COUNT(DISTINCT l.recordID) as total_users,
    COUNT(DISTINCT CASE WHEN s.joint_by_signup = 1 THEN l.recordID END) as joint_users,
    COUNT(DISTINCT CASE WHEN s.joint_by_signup = 0 THEN l.recordID END) as snapp_only_users,
    ROUND(COUNT(DISTINCT l.recordID) * 100.0 / 
        (SELECT COUNT(DISTINCT recordID) FROM survey_short), 2) as pct_of_all_drivers
FROM survey_long l
JOIN survey_short s ON l.recordID = s.recordID
WHERE l.main_question LIKE '%avigation%'
  AND l.answer IS NOT NULL
GROUP BY l.sub_question
ORDER BY total_users DESC;

-- ================================================================================
-- 12. CARPOOLING ANALYSIS (Tapsi-specific feature)
-- ================================================================================

-- 12.1 Carpooling awareness and usage
SELECT 
    carpooling_familiar_tapsi as familiarity,
    carpooling_gotoffer_accepted_tapsi as offer_acceptance,
    COUNT(*) as driver_count,
    ROUND(AVG(overall_satisfaction_tapsi), 2) as avg_satisfaction_tapsi,
    ROUND(AVG(ride_tapsi), 2) as avg_rides_tapsi
FROM survey_short
WHERE carpooling_familiar_tapsi IS NOT NULL
  AND age_tapsi != 'Not Registered'
GROUP BY carpooling_familiar_tapsi, carpooling_gotoffer_accepted_tapsi
ORDER BY driver_count DESC;

-- ================================================================================
-- 13. SUMMARY STATISTICS
-- High-level overview of all key metrics
-- ================================================================================

SELECT 
    'Total Survey Respondents' as metric,
    COUNT(DISTINCT recordID) as value,
    '' as percentage
FROM survey_short

UNION ALL

SELECT 
    'Joint Drivers (Active on Both)',
    COUNT(DISTINCT recordID),
    ROUND(COUNT(DISTINCT recordID) * 100.0 / (SELECT COUNT(DISTINCT recordID) FROM survey_short), 2) || '%'
FROM survey_short
WHERE joint_by_signup = 1 AND active_joint = 1

UNION ALL

SELECT 
    'Average Satisfaction Score (Snapp)',
    ROUND(AVG(overall_satisfaction_snapp), 2),
    'Scale: 1-5'
FROM survey_short
WHERE overall_satisfaction_snapp IS NOT NULL

UNION ALL

SELECT 
    'Average Satisfaction Score (Tapsi)',
    ROUND(AVG(overall_satisfaction_tapsi), 2),
    'Scale: 1-5'
FROM survey_short
WHERE overall_satisfaction_tapsi IS NOT NULL

UNION ALL

SELECT 
    'Drivers with Low Satisfaction (≤3)',
    COUNT(DISTINCT recordID),
    ROUND(COUNT(DISTINCT recordID) * 100.0 / (SELECT COUNT(DISTINCT recordID) FROM survey_short WHERE overall_satisfaction_snapp IS NOT NULL), 2) || '%'
FROM survey_short
WHERE overall_satisfaction_snapp <= 3

UNION ALL

SELECT 
    'Average Rides per Driver (Snapp)',
    ROUND(AVG(ride_snapp), 2),
    ''
FROM survey_short
WHERE ride_snapp IS NOT NULL

UNION ALL

SELECT 
    'Average Rides per Driver (Tapsi)',
    ROUND(AVG(ride_tapsi), 2),
    ''
FROM survey_short
WHERE ride_tapsi IS NOT NULL

UNION ALL

SELECT 
    'Full-Time Drivers',
    COUNT(*),
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM survey_short WHERE cooperation_type IS NOT NULL), 2) || '%'
FROM survey_short
WHERE cooperation_type = 'Full-Time'

UNION ALL

SELECT 
    'Drivers Who Received Incentive Message (Snapp)',
    COUNT(DISTINCT CASE WHEN incentive_got_message_snapp = 'Yes' THEN recordID END),
    ROUND(COUNT(DISTINCT CASE WHEN incentive_got_message_snapp = 'Yes' THEN recordID END) * 100.0 / COUNT(DISTINCT recordID), 2) || '%'
FROM survey_short;

-- ================================================================================
-- END OF SQL QUERIES
-- ================================================================================

-- NOTES ON DATA MERGING:
-- ----------------------
-- The survey_short table contains one row per respondent with all single-choice answers
-- The survey_long table contains multiple rows per respondent for multi-choice questions
-- 
-- To merge data from both tables, always use the recordID column:
--   - Use INNER JOIN when you need only respondents who have data in both tables
--   - Use LEFT JOIN when you want all records from survey_short plus matching long data
--   - Use RIGHT JOIN when you want all multi-choice responses plus matching short data
--
-- Example merge for analysis:
-- SELECT 
--     s.recordID,
--     s.cooperation_type,
--     s.overall_satisfaction_snapp,
--     l.main_question,
--     l.sub_question,
--     l.answer
-- FROM survey_short s
-- LEFT JOIN survey_long l ON s.recordID = l.recordID
-- WHERE l.main_question = 'dissatisfaction_reasons';
--
-- The codebook table contains column definitions and allowed values
-- Use it to understand what each column represents and decode categorical values
