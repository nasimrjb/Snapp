# 🚗 Driver Survey Data Cleaning Package

## 📦 What You Have

This package contains everything you need to transform your raw survey data into analysis-ready format.

### Files Included:
1. **survey_data_cleaner.py** - Main cleaning script with 9 cleaning options
2. **quick_start_guide.py** - Ready-to-run examples for different scenarios
3. **CLEANING_OPTIONS_GUIDE.md** - Detailed explanation of all cleaning options

---

## 🚀 Quick Start (3 Steps)

### Step 1: Update File Paths
Open `quick_start_guide.py` and update these three lines with your actual file paths:
```python
survey_path = r"D:\OneDrive\Work\Driver Survey\Outputs\survey_raw_database.xlsx"
mapping_path = r"D:\OneDrive\Work\Driver Survey\DataSources\multiple_choice.xlsx"
output_path = r"D:\OneDrive\Work\Driver Survey\Outputs\survey_cleaned.xlsx"
```

### Step 2: Choose Your Scenario
In `quick_start_guide.py`, uncomment ONE of these lines (around line 225):

```python
# For basic cleaning:
# cleaner = basic_cleaning()

# For multiple choice analysis (RECOMMENDED):
cleaner = multiple_choice_analysis()  # ← This one is already active

# For statistical analysis:
# cleaner = statistical_analysis_prep()

# For everything:
# cleaner = comprehensive_cleaning()
```

### Step 3: Run It!
```bash
python quick_start_guide.py
```

That's it! Your cleaned data will be in your Outputs folder.

---

## 🎯 Which Scenario Should I Choose?

### Choose **Multiple Choice Analysis** if:
- ✅ You want to analyze checkbox questions (incentives, decline reasons, etc.)
- ✅ You need to create visualizations
- ✅ You're counting how many people selected each option
- ✅ **This is the MOST IMPORTANT scenario for your driver survey data**

### Choose **Basic Cleaning** if:
- ✅ You just want to explore the data
- ✅ You need quick, minimal processing
- ✅ You're checking data quality first

### Choose **Statistical Analysis** if:
- ✅ You're running regression or other statistical models
- ✅ You need complete data (no missing values)
- ✅ You need numeric calculations

### Choose **Comprehensive Cleaning** if:
- ✅ You want everything cleaned and organized
- ✅ You're preparing final reports
- ✅ You need both wide and long formats
- ✅ You want maximum data quality

---

## 📊 Understanding Your Output

After running, you'll get files like:

### `survey_cleaned_unpivoted.xlsx`
This Excel file has **2 sheets**:

#### Sheet 1: "Cleaned_Wide"
- Your original survey structure (one row per driver)
- Good for: Demographics, single-choice questions
- Example:
```
| recordID | age_snapp | overall_satisfaction_snapp | incentive_type_rial_snapp |
|----------|-----------|----------------------------|---------------------------|
| 001      | 32        | 4                          | 1                         |
```

#### Sheet 2: "Unpivoted_Long"
- Multiple choice questions unpivoted (one row per choice selected)
- **ESSENTIAL for analyzing checkbox questions**
- Example:
```
| recordID | question            | choice                    |
|----------|---------------------|---------------------------|
| 001      | incentive_type      | Rial                      |
| 001      | incentive_type      | Commission-free           |
| 002      | incentive_type      | Income Guarantee          |
```

### CSV Files (if you chose format='both'):
- `survey_cleaned_unpivoted_wide.csv` - Same as Sheet 1
- `survey_cleaned_unpivoted_long.csv` - Same as Sheet 2

---

## 🧹 The 9 Cleaning Options Explained

### Essential Options (Always Use):
1. **Clean Column Names** - Standardizes naming (lowercase, no spaces)
2. **Remove Duplicates** - Removes repeat submissions
3. **Standardize Text** - Fixes spacing and formatting
4. **Unpivot Multiple Choice** ⭐ - CRITICAL for checkbox questions

### Quality Enhancement Options:
5. **Handle Missing Values** - 5 different strategies available
6. **Convert Data Types** - Makes numbers actually numeric
7. **Create Derived Variables** - Adds useful calculated fields
8. **Validate Data** - Checks for outliers and issues
9. **Summary Statistics** - Generates overview report

*See CLEANING_OPTIONS_GUIDE.md for detailed explanations*

---

## 💡 Common Use Cases

### Use Case 1: "What incentives do drivers prefer?"
```python
# Use the multiple_choice_analysis scenario
cleaner = multiple_choice_analysis()

# Then look at the "Unpivoted_Long" sheet
# Filter question = "incentive_type_snapp"
# Count the values in "choice" column
```

### Use Case 2: "Compare Snapp vs Tapsi satisfaction"
```python
# Use basic_cleaning scenario
cleaner = basic_cleaning()

# Then look at "Cleaned_Wide" sheet
# Compare overall_satisfaction_snapp vs overall_satisfaction_tapsi
```

### Use Case 3: "What predicts driver satisfaction?"
```python
# Use statistical_analysis_prep scenario
cleaner = statistical_analysis_prep()

# You'll get complete data ready for regression analysis
```

### Use Case 4: "Build a dashboard in Tableau"
```python
# Use the helper function
export_for_tableau()

# Import the long format CSV into Tableau
```

---

## 🔧 Customization

### Change Missing Value Strategy:
In any scenario function, change this line:
```python
.handle_missing_values(strategy='flag')  # Change to: 'drop_rows', 'drop_cols', 'impute_mode', 'impute_none'
```

### Add More Cleaning Steps:
```python
cleaner.load_data() \
       .clean_column_names() \
       .remove_duplicates() \
       .convert_data_types() \        # Add this
       .create_derived_variables() \  # Add this
       .unpivot_multiple_choice() \
       .export_cleaned_data(output_path)
```

### Analyze Specific Question Only:
```python
# Use the helper function
analyze_specific_question('incentive_type_snapp')
```

---

## 📝 Understanding Your Data Structure

### What is "Multiple Choice" in your survey?
Questions where drivers could select MORE THAN ONE option:
- Incentive types received (could receive Rial AND Commission-free)
- Reasons for declining rides (could be Distance AND Fare)
- Unsatisfaction reasons (could have multiple complaints)

### Why "Unpivot"?
**Before (Wide - Hard to analyze):**
```
| ID  | rial | commfree | guarantee |
|-----|------|----------|-----------|
| 001 |  1   |    1     |     0     |
| 002 |  0   |    1     |     1     |
```
Hard to answer: "Which incentive is most popular?"

**After (Long - Easy to analyze):**
```
| ID  | incentive_choice  |
|-----|-------------------|
| 001 | Rial              |
| 001 | Commission-free   |
| 002 | Commission-free   |
| 002 | Income Guarantee  |
```
Easy to answer: Just count!

---

## 🐛 Troubleshooting

### "File not found" error:
- Check your file paths have correct slashes (\ for Windows)
- Use raw strings: `r"D:\OneDrive\..."`
- Make sure files actually exist at those locations

### "No module named 'openpyxl'":
```bash
pip install openpyxl pandas numpy
```

### Output file is empty:
- Check console output for errors
- Make sure mapping file matches your survey columns
- Try basic_cleaning() first to test

### Need help with a specific question:
- Open CLEANING_OPTIONS_GUIDE.md
- See the FAQ section at the end
- Check the examples in quick_start_guide.py

---

## 📈 Next Steps After Cleaning

1. **Open the Excel output** - Check both sheets
2. **Verify the data** - Make sure it looks correct
3. **For multiple choice analysis**:
   - Use the "Unpivoted_Long" sheet
   - Create pivot tables or import into visualization tools
4. **For demographics**:
   - Use the "Cleaned_Wide" sheet
   - Standard analysis applies

---

## 🎓 Learning Resources

- **Start with**: quick_start_guide.py (run it!)
- **Then read**: CLEANING_OPTIONS_GUIDE.md (understand options)
- **Finally customize**: survey_data_cleaner.py (advanced)

---

## ✅ Checklist Before Analysis

- [ ] Ran the cleaning script successfully
- [ ] Checked output files exist
- [ ] Reviewed both wide and long formats
- [ ] Understand which format to use for each analysis
- [ ] Validated data looks correct (spot check a few rows)
- [ ] Know which questions are multiple choice
- [ ] Ready to analyze!

---

## 📞 Quick Reference

**Your files:**
- Raw data: `survey_raw_database.xlsx`
- Mapping: `multiple_choice.xlsx`
- Cleaned output: `survey_cleaned_*.xlsx`

**Key concept:**
- Wide format = Original survey layout (one row per person)
- Long format = Unpivoted (one row per choice selected)

**Most important for your data:**
- Use `multiple_choice_analysis()` scenario
- Analyze checkbox questions using the "Unpivoted_Long" sheet

---

Good luck with your driver survey analysis! 🚗📊
