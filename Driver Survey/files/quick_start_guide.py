"""
QUICK START GUIDE - Survey Data Cleaning
========================================

This script shows you how to use the SurveyDataCleaner with different scenarios
"""

from survey_data_cleaner import SurveyDataCleaner

# =============================================================================
# YOUR FILE PATHS - UPDATE THESE
# =============================================================================
survey_path = r"D:\OneDrive\Work\Driver Survey\Outputs\survey_raw_database.xlsx"
mapping_path = r"D:\OneDrive\Work\Driver Survey\DataSources\multiple_choice.xlsx"
output_path = r"D:\OneDrive\Work\Driver Survey\Outputs\survey_cleaned.xlsx"


# =============================================================================
# SCENARIO 1: BASIC CLEANING (Minimum Required)
# =============================================================================
def basic_cleaning():
    """
    Use this for quick, basic data preparation
    Recommended for: Initial exploration and basic analysis
    """
    print("\n" + "="*70)
    print("SCENARIO 1: BASIC CLEANING")
    print("="*70)

    cleaner = SurveyDataCleaner(survey_path, mapping_path)

    cleaner.load_data() \
           .clean_column_names() \
           .handle_missing_values(strategy='flag') \
           .remove_duplicates() \
           .standardize_text_responses() \
           .create_summary_statistics() \
           .export_cleaned_data(output_path.replace('.xlsx', '_basic.xlsx'), format='excel')

    print("\n✅ Basic cleaning complete!")
    return cleaner


# =============================================================================
# SCENARIO 2: MULTIPLE CHOICE ANALYSIS (Most Important for Your Data)
# =============================================================================
def multiple_choice_analysis():
    """
    Use this when analyzing checkbox/multiple-select questions
    Recommended for: Analyzing driver incentive preferences, decline reasons, etc.
    """
    print("\n" + "="*70)
    print("SCENARIO 2: MULTIPLE CHOICE ANALYSIS")
    print("="*70)

    cleaner = SurveyDataCleaner(survey_path, mapping_path)

    cleaner.load_data() \
           .clean_column_names() \
           .remove_duplicates() \
           .standardize_text_responses() \
           .unpivot_multiple_choice() \
           .export_cleaned_data(output_path.replace('.xlsx', '_unpivoted.xlsx'), format='both')

    print("\n✅ Multiple choice unpivoting complete!")
    print("📊 You now have two formats:")
    print("   - Wide format: For demographics and single-choice questions")
    print("   - Long format: For analyzing multiple choice patterns")

    return cleaner


# =============================================================================
# SCENARIO 3: STATISTICAL ANALYSIS READY
# =============================================================================
def statistical_analysis_prep():
    """
    Use this when preparing data for statistical modeling
    Recommended for: Regression, clustering, predictive models
    """
    print("\n" + "="*70)
    print("SCENARIO 3: STATISTICAL ANALYSIS PREPARATION")
    print("="*70)

    cleaner = SurveyDataCleaner(survey_path, mapping_path)

    cleaner.load_data() \
           .clean_column_names() \
           .remove_duplicates() \
           .handle_missing_values(strategy='impute_mode') \
           .standardize_text_responses() \
           .convert_data_types() \
           .create_derived_variables() \
           .validate_data() \
           .export_cleaned_data(output_path.replace('.xlsx', '_statistical.xlsx'), format='both')

    print("\n✅ Statistical analysis preparation complete!")
    return cleaner


# =============================================================================
# SCENARIO 4: COMPREHENSIVE CLEANING (Everything)
# =============================================================================
def comprehensive_cleaning():
    """
    Use this for thorough, publication-ready data preparation
    Recommended for: Final analysis, reporting, dashboards
    """
    print("\n" + "="*70)
    print("SCENARIO 4: COMPREHENSIVE CLEANING")
    print("="*70)

    cleaner = SurveyDataCleaner(survey_path, mapping_path)

    cleaner.load_data() \
           .clean_column_names() \
           .handle_missing_values(strategy='flag') \
           .remove_duplicates() \
           .standardize_text_responses() \
           .convert_data_types() \
           .create_derived_variables() \
           .unpivot_multiple_choice() \
           .validate_data() \
           .create_summary_statistics() \
           .export_cleaned_data(output_path.replace('.xlsx', '_comprehensive.xlsx'), format='both')

    print("\n✅ Comprehensive cleaning complete!")
    print("📊 Check your outputs folder for cleaned files!")

    return cleaner


# =============================================================================
# SCENARIO 5: CUSTOM CLEANING PIPELINE
# =============================================================================
def custom_cleaning():
    """
    Build your own cleaning pipeline
    Mix and match options based on your needs
    """
    print("\n" + "="*70)
    print("SCENARIO 5: CUSTOM CLEANING")
    print("="*70)

    cleaner = SurveyDataCleaner(survey_path, mapping_path)

    # Load data
    cleaner.load_data()

    # Choose your own options:
    cleaner.clean_column_names()                          # Option 1
    # Option 2 - custom strategy
    cleaner.handle_missing_values(strategy='impute_none')
    # cleaner.remove_duplicates()                         # Option 3 - commented out if not needed
    cleaner.standardize_text_responses()                  # Option 4
    cleaner.convert_data_types()                          # Option 5
    # cleaner.create_derived_variables()                  # Option 6 - commented out if not needed
    # Option 7 - essential for multiple choice
    cleaner.unpivot_multiple_choice()
    cleaner.validate_data()                               # Option 8

    # Export
    cleaner.export_cleaned_data(output_path.replace(
        '.xlsx', '_custom.xlsx'), format='excel')

    print("\n✅ Custom cleaning complete!")
    return cleaner


# =============================================================================
# SCENARIO 6: QUALITY CONTROL ONLY
# =============================================================================
def quality_control_check():
    """
    Run validation without making changes
    Use this to assess data quality before deciding on cleaning strategy
    """
    print("\n" + "="*70)
    print("SCENARIO 6: QUALITY CONTROL CHECK")
    print("="*70)

    cleaner = SurveyDataCleaner(survey_path, mapping_path)

    cleaner.load_data() \
           .validate_data() \
           .create_summary_statistics()

    # Show missing value summary
    print("\n📊 MISSING VALUES BY COLUMN:")
    missing = cleaner.survey_raw.isnull().sum()
    missing = missing[missing > 0].sort_values(ascending=False)
    print(missing.head(10))

    print("\n✅ Quality control check complete!")
    print("💡 Review the output above to decide on cleaning strategy")

    return cleaner


# =============================================================================
# MAIN EXECUTION
# =============================================================================
if __name__ == "__main__":

    print("""
    ╔═══════════════════════════════════════════════════════════════════════╗
    ║                  SURVEY DATA CLEANING - QUICK START                   ║
    ║                                                                       ║
    ║  Choose a scenario by uncommenting the function call below:          ║
    ╚═══════════════════════════════════════════════════════════════════════╝
    """)

    # UNCOMMENT ONE OF THE FOLLOWING:

    # 1. Basic cleaning (fastest, minimal changes)
    # cleaner = basic_cleaning()

    # 2. Multiple choice analysis (RECOMMENDED for your data)
    cleaner = multiple_choice_analysis()

    # 3. Statistical analysis preparation
    # cleaner = statistical_analysis_prep()

    # 4. Comprehensive cleaning (everything)
    # cleaner = comprehensive_cleaning()

    # 5. Custom pipeline (build your own)
    # cleaner = custom_cleaning()

    # 6. Quality control check only (no changes to data)
    # cleaner = quality_control_check()

    print("""
    ╔═══════════════════════════════════════════════════════════════════════╗
    ║                         NEXT STEPS                                    ║
    ║                                                                       ║
    ║  1. Check your Outputs folder for cleaned files                      ║
    ║  2. Open the Excel file - it has multiple sheets                     ║
    ║  3. Review the wide format (original structure)                      ║
    ║  4. Review the long format (for multiple choice analysis)            ║
    ║  5. Start your analysis!                                             ║
    ╚═══════════════════════════════════════════════════════════════════════╝
    """)


# =============================================================================
# HELPER FUNCTIONS FOR SPECIFIC TASKS
# =============================================================================

def analyze_specific_question(question_name):
    """
    Quick analysis of a specific multiple choice question

    Example usage:
    analyze_specific_question('incentive_type_snapp')
    """
    cleaner = SurveyDataCleaner(survey_path, mapping_path)
    cleaner.load_data().unpivot_multiple_choice()

    # Filter for specific question
    question_data = cleaner.survey_unpivoted[
        cleaner.survey_unpivoted['question'] == question_name
    ]

    # Count choices
    print(f"\n📊 Analysis of: {question_name}")
    print("="*70)
    choice_counts = question_data['choice'].value_counts()
    print(choice_counts)

    return choice_counts


def export_for_tableau():
    """
    Prepare data specifically for Tableau/Power BI visualization
    Creates optimized long format
    """
    cleaner = SurveyDataCleaner(survey_path, mapping_path)

    cleaner.load_data() \
           .clean_column_names() \
           .standardize_text_responses() \
           .convert_data_types() \
           .unpivot_multiple_choice() \
           .export_cleaned_data(
               output_path.replace('.xlsx', '_tableau.xlsx'),
               format='both'
    )

    print("✅ Tableau-ready data exported!")


def export_for_spss():
    """
    Prepare data for SPSS analysis
    Keeps wide format, handles missing values appropriately
    """
    cleaner = SurveyDataCleaner(survey_path, mapping_path)

    cleaner.load_data() \
           .clean_column_names() \
           .handle_missing_values(strategy='impute_none') \
           .remove_duplicates() \
           .convert_data_types() \
           .export_cleaned_data(
               output_path.replace('.xlsx', '_spss.xlsx'),
               format='excel'
    )

    print("✅ SPSS-ready data exported!")
