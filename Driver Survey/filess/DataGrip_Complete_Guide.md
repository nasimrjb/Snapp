# Survey Data Analysis - Complete DataGrip Guide

## Overview
This guide provides comprehensive SQL queries for analyzing Snapp and Tapsi driver survey data using DataGrip or any SQL IDE. The analysis covers **8 new screenshot-based analyses** plus all original analyses.

## 📊 Quick Start with DataGrip

### 1. Connect to Database
```sql
-- If using SQLite file:
-- File > New > Data Source > SQLite
-- Point to: /home/claude/survey_analysis.db

-- Test connection:
SELECT COUNT(*) as total_records FROM survey_short_format_single;
```

### 2. Basic Date-Based Query (Your Format)
```sql
SELECT *
FROM survey_short_format_single
WHERE datetime >= '2025-01-01'
  AND datetime <  '2027-01-01';
```

### 3. Table Structure
- **survey_short_format_single**: One row per respondent (5,656 rows, 73 columns)
- **survey_long_format_multichoice**: Multiple rows per respondent for multi-choice questions (26,406 rows, 76 columns)
- **codebook**: Column definitions (115 rows)

**Joining Tables**: Both tables share `recordID` as the primary key:
```sql
SELECT s.*, l.main_question, l.sub_question, l.answer
FROM survey_short_format_single s
LEFT JOIN survey_long_format_multichoice l ON s.recordID = l.recordID
WHERE l.main_question LIKE '%specific_topic%';
```

---

## 🆕 NEW ANALYSES (Matching Screenshots 1-8)

### Analysis 1: Snapp Drivers - Incentive Amounts Received (Last 7 Days)
**Screenshot Reference**: Distribution of incentive amounts by range

```sql
-- Overall incentive amount distribution
SELECT 
    COUNT(DISTINCT recordID) as total_respondents,
    -- Amount range percentages
    ROUND(COUNT(CASE WHEN incentive_rial_details_snapp LIKE '%< 100%' THEN 1 END) * 100.0 / COUNT(*), 1) as "< 100K",
    ROUND(COUNT(CASE WHEN incentive_rial_details_snapp LIKE '%100%200%' THEN 1 END) * 100.0 / COUNT(*), 1) as "100K-200K",
    ROUND(COUNT(CASE WHEN incentive_rial_details_snapp LIKE '%200%400%' THEN 1 END) * 100.0 / COUNT(*), 1) as "200K-400K",
    ROUND(COUNT(CASE WHEN incentive_rial_details_snapp LIKE '%400%600%' THEN 1 END) * 100.0 / COUNT(*), 1) as "400K-600K",
    ROUND(COUNT(CASE WHEN incentive_rial_details_snapp LIKE '%600%800%' THEN 1 END) * 100.0 / COUNT(*), 1) as "600K-800K",
    ROUND(COUNT(CASE WHEN incentive_rial_details_snapp LIKE '%800%1%' THEN 1 END) * 100.0 / COUNT(*), 1) as "800K-1M",
    ROUND(COUNT(CASE WHEN incentive_rial_details_snapp LIKE '%1m%1.25%' THEN 1 END) * 100.0 / COUNT(*), 1) as "1M-1.25M",
    ROUND(COUNT(CASE WHEN incentive_rial_details_snapp LIKE '%1.25%1.5%' THEN 1 END) * 100.0 / COUNT(*), 1) as "1.25M-1.5M",
    ROUND(COUNT(CASE WHEN incentive_rial_details_snapp LIKE '%>1.5%' THEN 1 END) * 100.0 / COUNT(*), 1) as ">1.5M"
FROM survey_short_format_single
WHERE incentive_got_message_snapp = 'Yes';
```

**Same analysis for Tapsi**:
```sql
-- Replace 'snapp' with 'tapsi' in column names
SELECT 
    COUNT(DISTINCT recordID) as total_respondents,
    ROUND(COUNT(CASE WHEN incentive_rial_details_tapsi LIKE '%< 100%' THEN 1 END) * 100.0 / COUNT(*), 1) as "< 100K",
    -- ... same pattern for other ranges
FROM survey_short_format_single
WHERE incentive_got_message_tapsi = 'Yes';
```

### Analysis 2: Payment Received from T30/Tapsi (Last 3 Days)
**Screenshot Reference**: Whether drivers received recent payments

```sql
-- Payment status analysis
SELECT 
    COUNT(DISTINCT l.recordID) as total_responses,
    ROUND(COUNT(CASE WHEN l.sub_question LIKE '%No%not have%' THEN 1 END) * 100.0 / COUNT(*), 1) as "No, has not have",
    ROUND(COUNT(CASE WHEN l.sub_question LIKE '%Yes%didn%' THEN 1 END) * 100.0 / COUNT(*), 1) as "Yes, but didnt do",
    ROUND(COUNT(CASE WHEN l.sub_question LIKE '%less than 50K%' THEN 1 END) * 100.0 / COUNT(*), 1) as "Less than 50K",
    ROUND(COUNT(CASE WHEN l.answer LIKE '%5K-10K%' THEN 1 END) * 100.0 / COUNT(*), 1) as "5K-10K",
    ROUND(COUNT(CASE WHEN l.answer LIKE '%10K-15K%' THEN 1 END) * 100.0 / COUNT(*), 1) as "10K-15K",
    ROUND(COUNT(CASE WHEN l.answer LIKE '%40K-50K%' THEN 1 END) * 100.0 / COUNT(*), 1) as "40K-50K",
    ROUND(COUNT(CASE WHEN l.answer LIKE '%>60K%' THEN 1 END) * 100.0 / COUNT(*), 1) as "More than 60K"
FROM survey_long_format_multichoice l
WHERE l.main_question LIKE '%payment%last 3 days%'
  AND l.answer IS NOT NULL;
```

### Analysis 3: Driver Inactive Time Before Incentive
**Screenshot Reference**: Inactivity periods before receiving incentive plans

```sql
-- T30/Tapsi inactivity distribution
SELECT 
    inactive_b4_incentive_tapsi as inactive_period,
    COUNT(*) as driver_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as percentage
FROM survey_short_format_single
WHERE inactive_b4_incentive_tapsi IS NOT NULL
  AND incentive_got_message_tapsi = 'Yes'
GROUP BY inactive_b4_incentive_tapsi
ORDER BY 
    CASE inactive_b4_incentive_tapsi
        WHEN 'Same Day' THEN 1
        WHEN '1_3 Day Before' THEN 2
        WHEN '3_7 Days_Before' THEN 3
        WHEN '15_30 Days_Before' THEN 5
        WHEN '2_3 Month Before' THEN 7
        WHEN '>6 Month Before' THEN 9
        ELSE 10
    END;

-- Snapp inactivity (if available from long format)
SELECT 
    l.sub_question as inactive_period,
    COUNT(DISTINCT l.recordID) as driver_count,
    ROUND(COUNT(DISTINCT l.recordID) * 100.0 / SUM(COUNT(DISTINCT l.recordID)) OVER (), 1) as percentage
FROM survey_long_format_multichoice l
WHERE l.main_question LIKE '%inactive%Snapp%'
GROUP BY l.sub_question;
```

### Analysis 4: Incentive Distribution by Type (Tapsi & Snapp)
**Screenshot Reference**: Different incentive structures (pay per ride, commission free, etc.)

```sql
-- Incentive type distribution
SELECT 
    'Tapsi' as platform,
    ROUND(COUNT(CASE WHEN l.sub_question LIKE '%Pay for each ride%' THEN 1 END) * 100.0 / COUNT(*), 1) as "Pay for each ride",
    ROUND(COUNT(CASE WHEN l.sub_question LIKE '%Commission free all%' THEN 1 END) * 100.0 / COUNT(*), 1) as "Commission free all",
    ROUND(COUNT(CASE WHEN l.sub_question LIKE '%Commission free after number%' THEN 1 END) * 100.0 / COUNT(*), 1) as "Comm free after N rides",
    ROUND(COUNT(CASE WHEN l.sub_question LIKE '%Commission free after%certain income%' THEN 1 END) * 100.0 / COUNT(*), 1) as "Comm free after income",
    ROUND(COUNT(CASE WHEN l.sub_question LIKE '%Income guarantee%' THEN 1 END) * 100.0 / COUNT(*), 1) as "Income guarantee",
    ROUND(COUNT(CASE WHEN l.sub_question LIKE '%pay after%certain income%' THEN 1 END) * 100.0 / COUNT(*), 1) as "Pay after income"
FROM survey_long_format_multichoice l
WHERE l.main_question LIKE '%incentive%type%' 
   OR l.main_question LIKE '%incentive%distribution%';

-- Same for Snapp
SELECT 
    'Snapp' as platform,
    incentive_category_snapp as category,
    COUNT(*) as driver_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as percentage
FROM survey_short_format_single
WHERE incentive_category_snapp IS NOT NULL
GROUP BY incentive_category_snapp;
```

### Analysis 5: Received Incentive Based on Survey (Detailed Breakdown)
**Screenshot Reference**: Complex incentive type matrix by driver segment

```sql
-- Comprehensive incentive breakdown
SELECT 
    CASE 
        WHEN joint_by_signup = 0 THEN 'Exclusive Snapp'
        WHEN joint_by_signup = 1 AND active_joint = 1 THEN 'Joint Active'
        WHEN joint_by_signup = 1 AND active_joint = 0 THEN 'Joint Inactive'
    END as driver_segment,
    cooperation_type,
    COUNT(*) as total_drivers,
    -- Snapp incentives
    ROUND(AVG(CASE WHEN incentive_category_snapp LIKE '%Money%' THEN 1.0 ELSE 0.0 END) * 100, 1) as "Snapp_Pay_Per_Ride",
    ROUND(AVG(CASE WHEN incentive_category_snapp LIKE '%Free-Commission%' THEN 1.0 ELSE 0.0 END) * 100, 1) as "Snapp_Free_Commission",
    -- Tapsi incentives
    ROUND(AVG(CASE WHEN incentive_category_tapsi LIKE '%Money%' THEN 1.0 ELSE 0.0 END) * 100, 1) as "Tapsi_Pay_Per_Ride",
    ROUND(AVG(CASE WHEN incentive_category_tapsi LIKE '%Free-Commission%' THEN 1.0 ELSE 0.0 END) * 100, 1) as "Tapsi_Free_Commission",
    -- Satisfaction
    ROUND(AVG(overall_incentive_satisfaction_snapp), 2) as avg_sat_snapp,
    ROUND(AVG(overall_incentive_satisfaction_tapsi), 2) as avg_sat_tapsi
FROM survey_short_format_single
WHERE incentive_got_message_snapp = 'Yes' OR incentive_got_message_tapsi = 'Yes'
GROUP BY driver_segment, cooperation_type
ORDER BY total_drivers DESC;
```

### Analysis 6: Incentive Time Limitation Impact
**Screenshot Reference**: Time constraints and their effect on participation

```sql
-- Snapp time limitation analysis
SELECT 
    'Snapp' as platform,
    incentive_time_limitation_snapp as time_limit,
    COUNT(*) as driver_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as pct_of_total,
    ROUND(AVG(overall_incentive_satisfaction_snapp), 2) as avg_satisfaction,
    ROUND(COUNT(CASE WHEN incentive_message_participation_snapp = 'Yes' THEN 1 END) * 100.0 / COUNT(*), 1) as participation_rate
FROM survey_short_format_single
WHERE incentive_time_limitation_snapp IS NOT NULL
  AND incentive_got_message_snapp = 'Yes'
GROUP BY incentive_time_limitation_snapp
ORDER BY driver_count DESC;

-- Tapsi time limitation analysis
SELECT 
    'Tapsi' as platform,
    incentive_active_duration_tapsi as time_limit,
    COUNT(*) as driver_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as pct_of_total,
    ROUND(AVG(overall_incentive_satisfaction_tapsi), 2) as avg_satisfaction,
    ROUND(COUNT(CASE WHEN incentive_participation_message_tapsi = 'Yes' THEN 1 END) * 100.0 / COUNT(*), 1) as participation_rate
FROM survey_short_format_single
WHERE incentive_active_duration_tapsi IS NOT NULL
  AND incentive_got_message_tapsi = 'Yes'
GROUP BY incentive_active_duration_tapsi
ORDER BY driver_count DESC;
```

### Analysis 7: Satisfaction Review (Multi-dimensional)
**Screenshot Reference**: Comprehensive satisfaction metrics comparison

```sql
-- Satisfaction breakdown by driver type and platform
SELECT 
    cooperation_type,
    CASE 
        WHEN joint_by_signup = 1 AND active_joint = 1 THEN 'Joint Active'
        WHEN joint_by_signup = 1 AND active_joint = 0 THEN 'Joint Inactive'
        ELSE 'Single Platform'
    END as platform_status,
    COUNT(*) as drivers,
    -- Overall satisfaction
    ROUND(AVG(overall_satisfaction_snapp), 2) as "Overall_Snapp",
    ROUND(AVG(overall_satisfaction_tapsi), 2) as "Overall_Tapsi",
    -- Component satisfaction - Snapp
    ROUND(AVG(fare_satisfaction_snapp), 2) as "Fare_Snapp",
    ROUND(AVG(income_satisfaction_snapp), 2) as "Income_Snapp",
    ROUND(AVG(req_count_satisfaction_snapp), 2) as "Request_Snapp",
    -- Component satisfaction - Tapsi
    ROUND(AVG(fare_satisfaction_tapsi), 2) as "Fare_Tapsi",
    ROUND(AVG(income_satisfaction_tapsi), 2) as "Income_Tapsi",
    ROUND(AVG(req_count_satisfaction_tapsi), 2) as "Request_Tapsi",
    -- Incentive satisfaction
    ROUND(AVG(overall_incentive_satisfaction_snapp), 2) as "Incentive_Sat_Snapp",
    ROUND(AVG(overall_incentive_satisfaction_tapsi), 2) as "Incentive_Sat_Tapsi"
FROM survey_short_format_single
WHERE cooperation_type IS NOT NULL
GROUP BY cooperation_type, platform_status
ORDER BY drivers DESC;

-- WoW (Week over Week) comparison would require datetime grouping:
SELECT 
    DATE(datetime) as survey_date,
    ROUND(AVG(overall_satisfaction_snapp), 2) as avg_satisfaction_snapp,
    ROUND(AVG(overall_satisfaction_tapsi), 2) as avg_satisfaction_tapsi,
    COUNT(*) as responses
FROM survey_short_format_single
WHERE datetime >= '2025-12-01'
GROUP BY DATE(datetime)
ORDER BY survey_date;
```

### Analysis 8: Joint Drivers - Tapsi Incentive Amounts
**Screenshot Reference**: Incentive amount distribution specifically for joint drivers

```sql
-- Tapsi incentive amounts for joint drivers
SELECT 
    'Joint Drivers' as segment,
    COUNT(*) as total_respondents,
    ROUND(COUNT(CASE WHEN incentive_rial_details_tapsi LIKE '%< 100%' THEN 1 END) * 100.0 / COUNT(*), 1) as "< 100K",
    ROUND(COUNT(CASE WHEN incentive_rial_details_tapsi LIKE '%100%200%' THEN 1 END) * 100.0 / COUNT(*), 1) as "100K-200K",
    ROUND(COUNT(CASE WHEN incentive_rial_details_tapsi LIKE '%200%400%' THEN 1 END) * 100.0 / COUNT(*), 1) as "200K-400K",
    ROUND(COUNT(CASE WHEN incentive_rial_details_tapsi LIKE '%400%600%' THEN 1 END) * 100.0 / COUNT(*), 1) as "400K-600K",
    ROUND(COUNT(CASE WHEN incentive_rial_details_tapsi LIKE '%600%800%' THEN 1 END) * 100.0 / COUNT(*), 1) as "600K-800K",
    ROUND(COUNT(CASE WHEN incentive_rial_details_tapsi LIKE '%800%1%' THEN 1 END) * 100.0 / COUNT(*), 1) as "800K-1M",
    ROUND(COUNT(CASE WHEN incentive_rial_details_tapsi LIKE '%>1.5%' THEN 1 END) * 100.0 / COUNT(*), 1) as ">1.5M"
FROM survey_short_format_single
WHERE joint_by_signup = 1
  AND incentive_got_message_tapsi = 'Yes';
```

---

## 📈 ORIGINAL ANALYSES (Still Relevant)

### Dissatisfaction Analysis
```sql
-- Reasons for low satisfaction (<=3)
SELECT 
    l.sub_question as dissatisfaction_reason,
    COUNT(DISTINCT l.recordID) as driver_count,
    ROUND(COUNT(DISTINCT l.recordID) * 100.0 / 
        (SELECT COUNT(*) FROM survey_short_format_single WHERE overall_satisfaction_snapp <= 3), 2) as percentage
FROM survey_long_format_multichoice l
JOIN survey_short_format_single s ON l.recordID = s.recordID
WHERE s.overall_satisfaction_snapp <= 3
  AND l.main_question LIKE '%dissatis%'
GROUP BY l.sub_question
ORDER BY driver_count DESC;
```

### Commission-Free Ride Analysis
```sql
-- Overall commission-free metrics
SELECT 
    COUNT(*) as total_drivers,
    ROUND(AVG(ride_snapp), 2) as avg_rides_snapp,
    ROUND(AVG(commfree_disc_ride_snapp), 2) as avg_commfree_rides_snapp,
    ROUND(AVG(commfree_snapp), 0) as avg_commfree_amount_snapp,
    ROUND(SUM(commfree_snapp), 0) as total_commfree_distributed_snapp,
    ROUND(COUNT(CASE WHEN commfree_disc_ride_snapp > 0 THEN 1 END) * 100.0 / COUNT(*), 2) as pct_got_commfree
FROM survey_short_format_single
WHERE ride_snapp IS NOT NULL;
```

### Ride Share Distribution
```sql
-- Platform share analysis
SELECT 
    CASE 
        WHEN joint_by_signup = 1 AND active_joint = 1 THEN 'Joint Active'
        ELSE 'Other'
    END as driver_type,
    COUNT(*) as drivers,
    ROUND(SUM(ride_snapp), 0) as total_snapp_rides,
    ROUND(SUM(ride_tapsi), 0) as total_tapsi_rides,
    ROUND(SUM(ride_snapp) * 100.0 / (SUM(ride_snapp) + SUM(COALESCE(ride_tapsi, 0))), 2) as snapp_share_pct,
    ROUND(SUM(ride_tapsi) * 100.0 / (SUM(ride_snapp) + SUM(COALESCE(ride_tapsi, 0))), 2) as tapsi_share_pct
FROM survey_short_format_single
WHERE ride_snapp > 0
GROUP BY driver_type;
```

---

## 🔧 DataGrip Pro Tips

### 1. Creating Indexes for Better Performance
```sql
-- Create indexes on frequently queried columns
CREATE INDEX idx_recordID ON survey_short_format_single(recordID);
CREATE INDEX idx_datetime ON survey_short_format_single(datetime);
CREATE INDEX idx_cooperation ON survey_short_format_single(cooperation_type);
CREATE INDEX idx_joint_status ON survey_short_format_single(joint_by_signup, active_joint);
```

### 2. Using Views for Complex Queries
```sql
-- Create a view for joint drivers
CREATE VIEW joint_drivers AS
SELECT *
FROM survey_short_format_single
WHERE joint_by_signup = 1 AND active_joint = 1;

-- Now query the view easily
SELECT COUNT(*) FROM joint_drivers;
```

### 3. Parameterized Queries in DataGrip
```sql
-- Use parameters with @ symbol
SELECT *
FROM survey_short_format_single
WHERE datetime >= @start_date
  AND datetime < @end_date
  AND cooperation_type = @coop_type;

-- DataGrip will prompt you for values
```

### 4. Exporting Results
- Run your query in DataGrip
- Right-click on results → Export Data
- Choose format: Excel, CSV, JSON, SQL, etc.
- Set file location

### 5. Query History
- Ctrl+H (Cmd+H on Mac) to view query history
- Reuse previous queries easily
- Search through query history

---

## 🎯 Common Use Cases

### Use Case 1: Find High-Value Drivers
```sql
SELECT 
    recordID,
    cooperation_type,
    ride_snapp + COALESCE(ride_tapsi, 0) as total_rides,
    overall_satisfaction_snapp,
    incentive_snapp + COALESCE(incentive_tapsi, 0) as total_incentive
FROM survey_short_format_single
WHERE (ride_snapp + COALESCE(ride_tapsi, 0)) > 50
ORDER BY total_rides DESC
LIMIT 100;
```

### Use Case 2: Incentive ROI Analysis
```sql
SELECT 
    incentive_category_snapp,
    COUNT(*) as drivers,
    ROUND(AVG(incentive_snapp), 0) as avg_incentive,
    ROUND(AVG(ride_snapp), 2) as avg_rides,
    ROUND(AVG(ride_snapp) * 1000.0 / NULLIF(AVG(incentive_snapp), 0), 2) as rides_per_1000_currency
FROM survey_short_format_single
WHERE incentive_snapp > 0
GROUP BY incentive_category_snapp
ORDER BY rides_per_1000_currency DESC;
```

### Use Case 3: Time-Series Analysis
```sql
SELECT 
    DATE(datetime) as date,
    COUNT(*) as responses,
    ROUND(AVG(overall_satisfaction_snapp), 2) as avg_satisfaction,
    ROUND(AVG(ride_snapp), 2) as avg_rides
FROM survey_short_format_single
WHERE datetime >= '2025-12-01'
GROUP BY DATE(datetime)
ORDER BY date;
```

---

## 📝 Key Column Reference

### Satisfaction Columns (1-5 scale)
- `overall_satisfaction_snapp` / `overall_satisfaction_tapsi`
- `fare_satisfaction_snapp` / `fare_satisfaction_tapsi`
- `income_satisfaction_snapp` / `income_satisfaction_tapsi`
- `req_count_satisfaction_snapp` / `req_count_satisfaction_tapsi`
- `overall_incentive_satisfaction_snapp` / `overall_incentive_satisfaction_tapsi`

### Incentive Columns
- `incentive_got_message_snapp` / `incentive_got_message_tapsi` (Yes/No)
- `incentive_message_participation_snapp` / `incentive_participation_message_tapsi` (Yes/No)
- `incentive_rial_details_snapp` / `incentive_rial_details_tapsi` (Text with amount ranges)
- `incentive_snapp` / `incentive_tapsi` (Numeric amount)
- `incentive_category_snapp` / `incentive_category_tapsi` (Money, Free-Commission, etc.)
- `incentive_time_limitation_snapp` / `incentive_active_duration_tapsi` (Time period)

### Driver Status Columns
- `joint_by_signup` (0=Snapp only, 1=Registered both)
- `active_joint` (0=Inactive on Tapsi, 1=Active on both)
- `cooperation_type` (Full-Time, Part-Time)
- `age_snapp` / `age_tapsi` (Registration age category)

### Activity Columns
- `ride_snapp` / `ride_tapsi` (Number of rides)
- `commfree_disc_ride_snapp` / `commfree_disc_ride_tapsi` (Commission-free rides)
- `commfree_snapp` / `commfree_tapsi` (Commission-free amount)

---

## 🚀 Advanced Queries

### Cross-Platform Comparison
```sql
SELECT 
    'Snapp' as platform,
    COUNT(DISTINCT CASE WHEN incentive_got_message_snapp = 'Yes' THEN recordID END) as reached,
    COUNT(DISTINCT CASE WHEN incentive_message_participation_snapp = 'Yes' THEN recordID END) as participated,
    ROUND(AVG(overall_incentive_satisfaction_snapp), 2) as avg_satisfaction
FROM survey_short_format_single

UNION ALL

SELECT 
    'Tapsi' as platform,
    COUNT(DISTINCT CASE WHEN incentive_got_message_tapsi = 'Yes' THEN recordID END) as reached,
    COUNT(DISTINCT CASE WHEN incentive_participation_message_tapsi = 'Yes' THEN recordID END) as participated,
    ROUND(AVG(overall_incentive_satisfaction_tapsi), 2) as avg_satisfaction
FROM survey_short_format_single;
```

### Cohort Analysis
```sql
-- Group drivers by signup period
SELECT 
    CASE 
        WHEN age_snapp = 'less_than_3_months' THEN 'New (0-3mo)'
        WHEN age_snapp IN ('3_to_6_months', '6_months_to_1_year') THEN 'Recent (3-12mo)'
        WHEN age_snapp IN ('1_to_3_years', '3_to_5_years') THEN 'Established (1-5yr)'
        ELSE 'Veteran (5yr+)'
    END as cohort,
    COUNT(*) as drivers,
    ROUND(AVG(ride_snapp), 2) as avg_rides,
    ROUND(AVG(overall_satisfaction_snapp), 2) as avg_satisfaction,
    ROUND(COUNT(CASE WHEN incentive_got_message_snapp = 'Yes' THEN 1 END) * 100.0 / COUNT(*), 1) as incentive_reach_pct
FROM survey_short_format_single
WHERE age_snapp IS NOT NULL
GROUP BY cohort
ORDER BY drivers DESC;
```

---

## ✅ Data Quality Checks

```sql
-- Check for completeness
SELECT 
    COUNT(*) as total,
    COUNT(CASE WHEN overall_satisfaction_snapp IS NULL THEN 1 END) as missing_satisfaction,
    COUNT(CASE WHEN ride_snapp IS NULL THEN 1 END) as missing_rides,
    COUNT(CASE WHEN datetime IS NULL THEN 1 END) as missing_datetime
FROM survey_short_format_single;

-- Validate ranges
SELECT 
    MIN(overall_satisfaction_snapp) as min_satisfaction,
    MAX(overall_satisfaction_snapp) as max_satisfaction,
    MIN(ride_snapp) as min_rides,
    MAX(ride_snapp) as max_rides
FROM survey_short_format_single;
```

---

## 📧 Support

For questions or issues:
1. Check this README
2. Review the comprehensive_sql_queries.sql file
3. Examine the codebook.xlsx for column definitions
4. Test queries on small data samples first

**Database Location**: `/home/claude/survey_analysis.db`
**All Query Files**: `/mnt/user-data/outputs/`

---

*Last Updated: February 17, 2026*
