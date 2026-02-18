# Survey Data Analysis - Snapp and Tapsi Driver Study

## Overview
This analysis examines survey data from ride-sharing drivers who work for Snapp and/or Tapsi platforms. The data includes responses from 5,656 drivers, with detailed information about their satisfaction, incentive participation, ride patterns, and demographics.

## Files Included

### 1. SQL Query Files
- **comprehensive_sql_queries.sql** - Complete collection of all SQL queries used in the analysis, organized by topic with detailed comments
- **survey_analysis.sql** - Core database setup and view definitions

### 2. Python Scripts
- **run_survey_analysis.py** - Main script to load data and execute all analyses
- **additional_analysis.py** - Additional detailed analyses for specific insights

### 3. Results Files
- **survey_analysis_results.xlsx** - Main analysis results with 8 worksheets
- **survey_detailed_analysis.xlsx** - Detailed analysis results with 8 worksheets

### 4. Database
- **survey_analysis.db** (in /home/claude/) - SQLite database containing all loaded data

## Data Structure

### Input Files (in /mnt/user-data/uploads/)
1. **survey_long_format_multichoice.csv** - 26,406 rows containing multi-choice question responses
2. **survey_short_format_single.csv** - 5,656 rows (one per respondent) with single-choice answers
3. **codebook.xlsx** - 115 rows defining column names, questions, and allowed values

### Key Joining Field
Both main tables can be merged using the **recordID** column, which is the unique identifier for each survey respondent.

## Analysis Categories

### 1. Dissatisfaction Analysis
- Identifies reasons why drivers gave low ratings (≤3 out of 5)
- Breaks down dissatisfaction by driver type (Joint vs Single platform)
- Key findings:
  - 52% of dissatisfied drivers cite "Improper Incentive Amount"
  - 32% cite "Hard to Do" (difficult requirements)
  - 22% cite "Low Time" (insufficient time to complete)

### 2. Commission-Free Ride Analysis
- Examines the impact of commission-free ride incentives
- Calculates penetration rates and total amounts
- Compares Snapp vs Tapsi programs
- Key metrics:
  - Average commission-free rides per driver
  - Total commission-free amounts distributed
  - Percentage of drivers who received commission-free rides

### 3. Inactive Time Distribution
- Analyzes how long drivers were inactive before receiving Tapsi incentives
- Shows distribution across time periods (same day, 1-3 days, up to >6 months)
- Helps understand re-engagement timing

### 4. Driver Persona Analysis
- Segments drivers by:
  - Cooperation type (Full-Time, Part-Time)
  - Platform usage (Joint Active, Joint Inactive, Snapp Only)
  - Work hours per week/month
- Reveals behavioral patterns and preferences

### 5. Ride Share Distribution
- Calculates market share between Snapp and Tapsi
- Analyzes ride distribution for joint drivers
- Shows platform loyalty patterns

### 6. Incentive Program Analysis
- Message reach and participation rates
- Incentive satisfaction scores
- Impact of time limitations on participation
- Incentive categories (Money, Free-Commission, Money & Free-Commission)

### 7. Satisfaction Metrics
- Multi-dimensional satisfaction analysis:
  - Overall satisfaction
  - Fare satisfaction
  - Income satisfaction
  - Request count satisfaction
- Comparisons across driver types

### 8. Demographics Analysis
- Age group distribution and impact
- Education level analysis
- Gender and marital status patterns
- Correlation with satisfaction and ride volume

### 9. Registration and Referral Analysis
- Registration channels (Online, Field, Offices)
- Main registration reasons (Friends/Family, Ads, Street Ads)
- Referral effectiveness
- Acquisition channel performance

### 10. Navigation App Usage
- Which navigation apps drivers use
- Preferences by driver type
- Integration with ride-sharing platforms

### 11. Carpooling Analysis (Tapsi-specific)
- Carpooling familiarity and usage
- Offer acceptance rates
- Satisfaction with carpooling feature

### 12. Summary Statistics
- High-level KPIs:
  - Total respondents: 5,656
  - Average satisfaction scores
  - Average rides per driver
  - Percentage of joint drivers
  - Incentive program reach

## How to Use the SQL Queries

### Option 1: Using SQLite Database (Recommended)
```bash
# Connect to the database
sqlite3 /home/claude/survey_analysis.db

# Run any query from the comprehensive_sql_queries.sql file
# Example:
.read /mnt/user-data/outputs/comprehensive_sql_queries.sql
```

### Option 2: Running Python Scripts
```bash
# Run main analysis
python3 /mnt/user-data/outputs/run_survey_analysis.py

# Run additional detailed analysis
python3 /mnt/user-data/outputs/additional_analysis.py
```

### Option 3: Copy-Paste Individual Queries
Open `comprehensive_sql_queries.sql` and copy any section you need. The file is organized with:
- Clear section headers
- Detailed comments
- Numbered queries
- Example usage

## Merging Data from Both Tables

The two main tables (survey_short and survey_long) represent different question types:

### survey_short
- One row per respondent
- Contains single-choice questions and demographic data
- All numeric satisfaction scores
- 73 columns

### survey_long
- Multiple rows per respondent (one per multi-choice answer)
- Contains questions where respondents could select multiple options
- Includes columns: main_question, sub_question, answer
- 76 columns

### Merge Example
```sql
-- Get dissatisfaction reasons for each driver
SELECT 
    s.recordID,
    s.cooperation_type,
    s.overall_satisfaction_snapp,
    l.sub_question as dissatisfaction_reason
FROM survey_short s
LEFT JOIN survey_long l ON s.recordID = l.recordID
WHERE l.main_question LIKE '%dissatis%'
  AND s.overall_satisfaction_snapp <= 3;
```

## Key Insights from Analysis

### Overall Statistics
- **5,656 total respondents**
- **31% are joint drivers** (active on both platforms)
- **Average satisfaction (Snapp): 2.85 / 5.0**
- **Average satisfaction (Tapsi): 3.12 / 5.0**
- **62% of drivers have satisfaction ≤3** (below satisfied)

### Incentive Programs
- **69% of Snapp drivers received incentive messages**
- **37% of Tapsi drivers received incentive messages**
- **Participation rates vary by time limitation**
- **Main incentive categories:**
  - Money only
  - Free-Commission only
  - Money & Free-Commission combined

### Driver Segments
- **Full-Time drivers: ~45%** of respondents
- **Part-Time drivers: ~55%** of respondents
- **Joint Active drivers** tend to have higher ride volumes
- **Age group distribution:** Majority in 26-45 age range

### Platform Competition
- **Snapp has higher market share** in terms of total rides
- **Joint drivers split rides** approximately 65-35 (Snapp-Tapsi)
- **Platform loyalty** correlates with satisfaction scores

## Data Quality Notes

1. **Missing Values**: Some fields have null values, especially for Tapsi data where drivers are "Not Registered"
2. **Mixed Data Types**: The long format table has mixed types due to various question formats
3. **Categorical Data**: Most categorical fields are stored as text strings
4. **Numeric Precision**: Monetary values and ride counts are stored with decimal precision

## Analysis Customization

To create custom analyses:

1. **Start with survey_short** for most analyses (one row per driver)
2. **Join survey_long** when you need multi-choice question responses
3. **Filter appropriately:**
   - Use `WHERE age_tapsi != 'Not Registered'` for Tapsi-specific analysis
   - Use `WHERE cooperation_type IS NOT NULL` to exclude incomplete responses
   - Use `WHERE joint_by_signup = 1` for joint driver analysis

4. **Common filters:**
```sql
-- Only dissatisfied drivers
WHERE overall_satisfaction_snapp <= 3

-- Only joint active drivers
WHERE joint_by_signup = 1 AND active_joint = 1

-- Only full-time drivers
WHERE cooperation_type = 'Full-Time'

-- Only drivers who received incentives
WHERE incentive_got_message_snapp = 'Yes'
```

## Codebook Usage

The codebook.xlsx file contains:
- **column_name**: Field name in the database
- **question_text**: The actual survey question asked
- **allowed_answers**: Valid response options
- **replaced_answers**: Encoding/transformation information

Use the codebook to:
1. Understand what each column represents
2. Decode categorical values
3. Validate data ranges
4. Create meaningful labels for outputs

## Contact and Support

This analysis was generated to match the output formats shown in the provided screenshots. The SQL queries are designed to be:
- Readable and well-commented
- Efficient and optimized
- Flexible for customization
- Compatible with standard SQLite

For questions about specific queries or to request additional analyses, refer to the comprehensive_sql_queries.sql file which contains all the building blocks you need.

## Version Information

- **Analysis Date**: February 17, 2026
- **Data Collection Period**: Week 52, December 2025
- **Survey Respondents**: 5,656 drivers
- **Database Format**: SQLite 3
- **Python Version**: 3.x
- **Required Libraries**: pandas, numpy, openpyxl, sqlite3

---
*End of README*
