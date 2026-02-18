# Survey Data Cleaning Options Guide

## Overview
This guide explains all available cleaning options for transforming your raw survey data into analysis-ready format.

---

## 🧹 CLEANING OPTION 1: Column Name Standardization

**What it does:**
- Converts all column names to lowercase
- Removes special characters
- Replaces spaces with underscores
- Creates consistent naming convention

**Why use it:**
- Makes data easier to work with programmatically
- Prevents errors from inconsistent naming
- Ensures compatibility with analysis tools

**Example:**
```
Before: "Incentive Type - Pay After Ride (Snapp)"
After:  "incentive_type_pay_after_ride_snapp"
```

---

## 🧹 CLEANING OPTION 2: Missing Value Handling

**What it does:**
Offers 5 different strategies for dealing with missing data:

### Strategy A: 'flag' (Recommended)
- Creates indicator columns showing where data is missing
- Preserves original data
- Allows you to analyze missing patterns

### Strategy B: 'drop_rows'
- Removes survey responses with >50% missing values
- Good for quality control
- Reduces dataset size

### Strategy C: 'drop_cols'
- Removes questions with >70% non-response
- Useful for questions most people skipped
- Simplifies analysis

### Strategy D: 'impute_mode'
- Fills missing values with most common answer
- Statistical imputation approach
- Can bias results if overused

### Strategy E: 'impute_none'
- Fills missing with "Not Answered"
- Treats non-response as valid category
- Good for analysis of response patterns

**When to use each:**
- **'flag'**: When you want to preserve all data and study missingness
- **'drop_rows'**: When you need complete responses only
- **'drop_cols'**: When certain questions performed poorly
- **'impute_mode'**: For statistical modeling requiring complete data
- **'impute_none'**: For categorical analysis of all responses

---

## 🧹 CLEANING OPTION 3: Duplicate Removal

**What it does:**
- Identifies and removes duplicate survey responses
- Keeps only the first occurrence
- Can target specific columns for duplicate detection

**Why use it:**
- Prevents double-counting responses
- Removes test submissions
- Ensures data integrity

**Options:**
- Remove exact duplicates (all columns identical)
- Remove duplicates based on specific columns (e.g., respondent ID)

---

## 🧹 CLEANING OPTION 4: Text Response Standardization

**What it does:**
- Trims leading/trailing whitespace
- Removes extra spaces between words
- Standardizes text formatting

**Why use it:**
- "Yes" and " Yes " become the same value
- Prevents issues in grouping and filtering
- Improves data quality

**Example:**
```
Before: "  Pay After Ride   "
After:  "Pay After Ride"
```

---

## 🧹 CLEANING OPTION 5: Data Type Conversion

**What it does:**
- Converts columns to appropriate data types
- Converts numeric strings to numbers
- Converts date strings to datetime objects
- Optimizes categorical data

**Why use it:**
- Enables proper mathematical operations
- Allows time-based analysis
- Reduces memory usage
- Prevents calculation errors

**What gets converted:**
- Age, trip counts → integers
- Satisfaction scores → floats
- Response timestamps → datetime
- Text responses → optimized categories

---

## 🧹 CLEANING OPTION 6: Derived Variable Creation

**What it does:**
Creates new useful variables from existing data:

### A. Completeness Score
- Percentage of questions answered by each respondent
- Useful for quality filtering

### B. Time-Based Features
- Response date
- Hour of day (morning/afternoon/evening)
- Day of week
- Month

### C. Response Quality Indicators
- Identifies straight-lining (same answer to all questions)
- Flags suspicious response patterns

**Why use it:**
- Enables deeper analysis
- Quality control
- Segmentation opportunities

**Example New Variables:**
```
completeness_score: 87.5%  (answered 87.5% of questions)
response_hour: 14  (responded at 2 PM)
response_day_of_week: "Monday"
response_month: "January"
```

---

## 🧹 CLEANING OPTION 7: Multiple Choice Unpivoting ⭐

**What it does:**
Transforms data from WIDE format to LONG format

### WIDE Format (Current):
```
| recordID | incentive_type_rial | incentive_type_commfree | incentive_type_nothing |
|----------|---------------------|-------------------------|------------------------|
| 001      | 1                   | 1                       | 0                      |
| 002      | 0                   | 1                       | 0                      |
```

### LONG Format (After Unpivoting):
```
| recordID | question       | choice              |
|----------|----------------|---------------------|
| 001      | incentive_type | Rial                |
| 001      | incentive_type | Commission-free     |
| 002      | incentive_type | Commission-free     |
```

**Why use it:**
- Easier to analyze multiple choice questions
- Better for visualization
- Simplifies statistical analysis
- Standard format for most analysis tools

**Best for:**
- Counting choice frequencies
- Cross-tabulation analysis
- Visualization in tools like Tableau/Power BI
- Statistical modeling

---

## 🧹 CLEANING OPTION 8: Data Validation

**What it does:**
Performs quality checks on your data:

### A. Outlier Detection
- Identifies extreme values in numeric columns
- Uses statistical methods (IQR method)
- Flags potentially invalid responses

### B. Categorical Validation
- Checks for unexpected answer values
- Identifies high-cardinality issues
- Detects data entry errors

### C. Consistency Checks
- Verifies logical consistency
- Checks for impossible combinations

**Why use it:**
- Catches data quality issues early
- Identifies problematic responses
- Ensures analysis reliability

**Example Output:**
```
⚠ age_snapp: 15 potential outliers detected
⚠ trip_count_snapp: 8 potential outliers detected
ℹ open_text_response: High cardinality (2,341 unique values)
```

---

## 🧹 CLEANING OPTION 9: Summary Statistics Generation

**What it does:**
Creates comprehensive overview of your dataset:

- Total number of responses
- Total number of questions
- Breakdown by question type
- Missing value summary
- Average completeness rate

**Why use it:**
- Quick data overview
- Documentation for reports
- Quality assessment
- Baseline metrics

**Example Output:**
```
Total Responses: 5,656
Total Questions: 136
Numeric Questions: 28
Text Questions: 108
Missing Values: 12,847
Average Completeness: 82.35%
```

---

## 📊 RECOMMENDED CLEANING WORKFLOW

### For Basic Analysis:
1. ✅ Clean column names (Option 1)
2. ✅ Handle missing values with 'flag' (Option 2)
3. ✅ Remove duplicates (Option 3)
4. ✅ Standardize text (Option 4)
5. ✅ Create summary statistics (Option 9)

### For Statistical Analysis:
1. ✅ All basic options above
2. ✅ Convert data types (Option 5)
3. ✅ Handle missing values with 'impute_mode' (Option 2)
4. ✅ Validate data (Option 8)
5. ✅ Create derived variables (Option 6)

### For Multiple Choice Analysis:
1. ✅ All basic options above
2. ✅ **Unpivot multiple choice questions (Option 7)** ⭐
3. ✅ Export both wide and long formats

### For Comprehensive Reporting:
1. ✅ All options (1-9)
2. ✅ Export in both Excel and CSV formats
3. ✅ Include both wide and long formats

---

## 🎯 QUICK DECISION GUIDE

**Choose based on your analysis goal:**

| Your Goal | Key Options to Use |
|-----------|-------------------|
| Basic frequencies and crosstabs | 1, 3, 4, 7 |
| Statistical modeling | 1-6, 8 |
| Data visualization | 1, 4, 6, 7 |
| Quality assessment | 2, 8, 9 |
| Time-series analysis | 5, 6 |
| Multiple choice analysis | **7** (Essential) |
| Complete cleaning | 1-9 (All) |

---

## 💡 PRO TIPS

1. **Always start with Option 1**: Clean column names first to avoid issues later

2. **For missing values**: Use 'flag' strategy first to understand patterns, then decide on other strategies

3. **Multiple choice questions**: Option 7 (unpivoting) is ESSENTIAL for proper analysis of checkbox questions

4. **Export both formats**: Keep wide format for demographics, long format for multiple choice

5. **Document your choices**: The script creates logs of all cleaning operations

6. **Validate before finalizing**: Always run Option 8 before final export

7. **Iterate**: You can run the cleaner multiple times with different options to find the best approach

---

## 📁 OUTPUT FILES

The script creates:

1. **survey_cleaned.xlsx** with multiple sheets:
   - `Cleaned_Wide`: Your cleaned survey in original wide format
   - `Unpivoted_Long`: Multiple choice questions in long format

2. **survey_cleaned_wide.csv**: Wide format in CSV
3. **survey_cleaned_long.csv**: Long format in CSV

---

## 🔧 CUSTOMIZATION

You can customize the script by:

1. **Changing thresholds**: Modify missing value thresholds (currently 50% for rows, 70% for columns)

2. **Adding custom validation**: Add your own business rules in the validate_data() function

3. **Creating custom derived variables**: Extend the create_derived_variables() function

4. **Changing output format**: Modify the export function to include additional formats

---

## ❓ FAQ

**Q: Should I use wide or long format?**
A: Use both! Wide format is better for demographics and single-choice questions. Long format is essential for multiple-choice questions.

**Q: What if I want to keep some missing values but impute others?**
A: Run the cleaner twice - once with 'flag', save it, then run again with 'impute' on specific columns.

**Q: Can I undo cleaning operations?**
A: The script always preserves your original file. Work on copies!

**Q: How do I know which cleaning options to use?**
A: Start with the "Recommended Cleaning Workflow" for your use case, then customize based on results.

---

## 📞 NEXT STEPS

After cleaning:
1. Review the summary statistics
2. Check the validation report
3. Examine both wide and long format outputs
4. Begin your analysis with confidence!
