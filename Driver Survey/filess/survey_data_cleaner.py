"""
Survey Data Cleaning and Transformation Script
==============================================
This script transforms raw survey data into analysis-ready format by:
1. Unpivoting multiple choice columns
2. Cleaning and standardizing responses
3. Handling missing data
4. Creating derived variables
"""

import pandas as pd
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')


class SurveyDataCleaner:
    """
    A comprehensive survey data cleaning class with multiple cleaning options
    """
    
    def __init__(self, survey_path, mapping_path):
        """
        Initialize the cleaner with file paths
        
        Parameters:
        -----------
        survey_path : str
            Path to the raw survey database Excel file
        mapping_path : str
            Path to the multiple choice mapping Excel file
        """
        self.survey_path = survey_path
        self.mapping_path = mapping_path
        self.survey_raw = None
        self.mapping = None
        self.survey_cleaned = None
        self.survey_unpivoted = None
        
    def load_data(self):
        """Load the survey data and mapping file"""
        print("📂 Loading data...")
        self.survey_raw = pd.read_excel(self.survey_path)
        self.mapping = pd.read_excel(self.mapping_path)
        print(f"   ✓ Survey data loaded: {self.survey_raw.shape[0]} rows, {self.survey_raw.shape[1]} columns")
        print(f"   ✓ Mapping loaded: {self.mapping.shape[0]} multiple choice options")
        return self
    
    def clean_column_names(self):
        """
        CLEANING OPTION 1: Standardize column names
        - Remove special characters
        - Convert to lowercase
        - Replace spaces with underscores
        """
        print("\n🧹 OPTION 1: Cleaning column names...")
        original_cols = self.survey_raw.columns.tolist()
        
        # Clean column names
        self.survey_raw.columns = (
            self.survey_raw.columns
            .str.strip()
            .str.lower()
            .str.replace(' ', '_')
            .str.replace('[^a-z0-9_]', '', regex=True)
        )
        
        changed = sum([1 for o, n in zip(original_cols, self.survey_raw.columns) if o != n])
        print(f"   ✓ Standardized {changed} column names")
        return self
    
    def handle_missing_values(self, strategy='flag'):
        """
        CLEANING OPTION 2: Handle missing values
        
        Parameters:
        -----------
        strategy : str
            'flag' - Create indicator columns for missing values
            'drop_rows' - Remove rows with too many missing values
            'drop_cols' - Remove columns with too many missing values
            'impute_mode' - Fill with most frequent value
            'impute_none' - Fill with 'Not Answered'
        """
        print(f"\n🧹 OPTION 2: Handling missing values (strategy: {strategy})...")
        
        missing_summary = self.survey_raw.isnull().sum()
        missing_pct = (missing_summary / len(self.survey_raw) * 100).round(2)
        
        print(f"   Current missing values: {self.survey_raw.isnull().sum().sum()} total")
        
        if strategy == 'flag':
            # Create indicator columns for columns with missing values
            for col in self.survey_raw.columns:
                if self.survey_raw[col].isnull().any():
                    self.survey_raw[f'{col}_missing'] = self.survey_raw[col].isnull().astype(int)
            print(f"   ✓ Created missing value flags for columns with NAs")
            
        elif strategy == 'drop_rows':
            # Drop rows with more than 50% missing values
            threshold = len(self.survey_raw.columns) * 0.5
            before = len(self.survey_raw)
            self.survey_raw = self.survey_raw.dropna(thresh=threshold)
            print(f"   ✓ Dropped {before - len(self.survey_raw)} rows with >50% missing values")
            
        elif strategy == 'drop_cols':
            # Drop columns with more than 70% missing values
            threshold = len(self.survey_raw) * 0.7
            before = len(self.survey_raw.columns)
            self.survey_raw = self.survey_raw.dropna(axis=1, thresh=threshold)
            print(f"   ✓ Dropped {before - len(self.survey_raw.columns)} columns with >70% missing values")
            
        elif strategy == 'impute_mode':
            # Fill with most frequent value for categorical columns
            for col in self.survey_raw.select_dtypes(include=['object']).columns:
                if self.survey_raw[col].isnull().any():
                    mode_val = self.survey_raw[col].mode()[0] if not self.survey_raw[col].mode().empty else 'Unknown'
                    self.survey_raw[col].fillna(mode_val, inplace=True)
            print(f"   ✓ Imputed missing values with mode for categorical columns")
            
        elif strategy == 'impute_none':
            # Fill with 'Not Answered' for object columns
            for col in self.survey_raw.select_dtypes(include=['object']).columns:
                self.survey_raw[col].fillna('Not Answered', inplace=True)
            print(f"   ✓ Filled missing values with 'Not Answered'")
        
        return self
    
    def remove_duplicates(self, subset=None):
        """
        CLEANING OPTION 3: Remove duplicate responses
        
        Parameters:
        -----------
        subset : list, optional
            Columns to consider for identifying duplicates (default: all columns)
        """
        print(f"\n🧹 OPTION 3: Removing duplicates...")
        before = len(self.survey_raw)
        
        if subset:
            self.survey_raw = self.survey_raw.drop_duplicates(subset=subset, keep='first')
        else:
            self.survey_raw = self.survey_raw.drop_duplicates(keep='first')
        
        removed = before - len(self.survey_raw)
        print(f"   ✓ Removed {removed} duplicate rows")
        return self
    
    def standardize_text_responses(self):
        """
        CLEANING OPTION 4: Standardize text responses
        - Trim whitespace
        - Convert to title case for consistency
        - Remove extra spaces
        """
        print(f"\n🧹 OPTION 4: Standardizing text responses...")
        
        for col in self.survey_raw.select_dtypes(include=['object']).columns:
            self.survey_raw[col] = (
                self.survey_raw[col]
                .astype(str)
                .str.strip()
                .str.replace(r'\s+', ' ', regex=True)
                .replace('nan', np.nan)
            )
        
        print(f"   ✓ Standardized text in {len(self.survey_raw.select_dtypes(include=['object']).columns)} columns")
        return self
    
    def convert_data_types(self):
        """
        CLEANING OPTION 5: Convert data types appropriately
        - Numeric columns to float/int
        - Date columns to datetime
        - Categorical columns optimization
        """
        print(f"\n🧹 OPTION 5: Converting data types...")
        
        # Convert datetime column if exists
        if 'datetime' in self.survey_raw.columns:
            self.survey_raw['datetime'] = pd.to_datetime(self.survey_raw['datetime'], errors='coerce')
            print(f"   ✓ Converted datetime column")
        
        # Identify and convert numeric columns that are stored as strings
        for col in self.survey_raw.columns:
            if self.survey_raw[col].dtype == 'object':
                # Try to convert to numeric
                numeric_converted = pd.to_numeric(self.survey_raw[col], errors='coerce')
                if numeric_converted.notna().sum() / len(self.survey_raw) > 0.8:  # If 80%+ are numeric
                    self.survey_raw[col] = numeric_converted
                    print(f"   ✓ Converted {col} to numeric")
        
        return self
    
    def create_derived_variables(self):
        """
        CLEANING OPTION 6: Create useful derived variables
        - Response completeness score
        - Time-based features from datetime
        - Response quality indicators
        """
        print(f"\n🧹 OPTION 6: Creating derived variables...")
        
        # Completeness score (% of non-missing values per response)
        self.survey_raw['completeness_score'] = (
            self.survey_raw.notna().sum(axis=1) / len(self.survey_raw.columns) * 100
        ).round(2)
        
        # Extract time features if datetime exists
        if 'datetime' in self.survey_raw.columns and pd.api.types.is_datetime64_any_dtype(self.survey_raw['datetime']):
            self.survey_raw['response_date'] = self.survey_raw['datetime'].dt.date
            self.survey_raw['response_hour'] = self.survey_raw['datetime'].dt.hour
            self.survey_raw['response_day_of_week'] = self.survey_raw['datetime'].dt.day_name()
            self.survey_raw['response_month'] = self.survey_raw['datetime'].dt.month_name()
            print(f"   ✓ Created time-based features")
        
        print(f"   ✓ Created completeness score")
        return self
    
    def unpivot_multiple_choice(self):
        """
        CLEANING OPTION 7: Unpivot multiple choice questions
        This transforms wide format (one column per choice) to long format
        (one row per selected choice)
        """
        print(f"\n🧹 OPTION 7: Unpivoting multiple choice questions...")
        
        # Group mapping by main question
        question_groups = self.mapping.groupby('Main Question')
        
        # Identify columns that don't need unpivoting (ID, demographics, etc.)
        mc_columns = self.mapping['Column Headers'].tolist()
        non_mc_columns = [col for col in self.survey_raw.columns if col not in mc_columns]
        
        unpivoted_dfs = []
        
        for question_name, group in question_groups:
            print(f"   Processing: {question_name}")
            
            # Get columns for this question
            question_cols = group['Column Headers'].tolist()
            choice_labels = dict(zip(group['Column Headers'], group['Multiple Choices']))
            
            # Check which columns exist in survey data
            existing_cols = [col for col in question_cols if col in self.survey_raw.columns]
            
            if not existing_cols:
                print(f"   ⚠ Skipping {question_name} - no matching columns found")
                continue
            
            # Create unpivoted version for this question
            for idx, row in self.survey_raw.iterrows():
                selected_choices = []
                
                for col in existing_cols:
                    # If the value is not null/empty/0/'0', it means this choice was selected
                    val = row[col]
                    if pd.notna(val) and val != 0 and val != '0' and val != '' and val != 'nan':
                        choice_label = choice_labels.get(col, col)
                        selected_choices.append({
                            'recordID': row.get('recordID', row.get('recordid', idx)),
                            'question': question_name,
                            'choice': choice_label,
                            'value': val
                        })
                
                if selected_choices:
                    unpivoted_dfs.extend(selected_choices)
        
        # Create unpivoted dataframe
        if unpivoted_dfs:
            self.survey_unpivoted = pd.DataFrame(unpivoted_dfs)
            
            # Merge with non-multiple-choice columns
            base_data = self.survey_raw[non_mc_columns].copy()
            if 'recordID' not in base_data.columns and 'recordid' in base_data.columns:
                base_data = base_data.rename(columns={'recordid': 'recordID'})
            
            self.survey_unpivoted = self.survey_unpivoted.merge(
                base_data, 
                on='recordID', 
                how='left'
            )
            
            print(f"   ✓ Created unpivoted dataset: {len(self.survey_unpivoted)} response-choice pairs")
        else:
            print(f"   ⚠ No data was unpivoted")
        
        return self
    
    def validate_data(self):
        """
        CLEANING OPTION 8: Data validation and quality checks
        - Check for outliers in numeric columns
        - Validate categorical responses
        - Check data consistency
        """
        print(f"\n🧹 OPTION 8: Validating data quality...")
        
        validation_report = []
        
        # Check numeric ranges
        numeric_cols = self.survey_raw.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            q1 = self.survey_raw[col].quantile(0.25)
            q3 = self.survey_raw[col].quantile(0.75)
            iqr = q3 - q1
            lower_bound = q1 - 3 * iqr
            upper_bound = q3 + 3 * iqr
            
            outliers = ((self.survey_raw[col] < lower_bound) | (self.survey_raw[col] > upper_bound)).sum()
            if outliers > 0:
                validation_report.append(f"   ⚠ {col}: {outliers} potential outliers detected")
        
        # Check for unexpected values in categorical columns
        cat_cols = self.survey_raw.select_dtypes(include=['object']).columns
        for col in cat_cols:
            unique_count = self.survey_raw[col].nunique()
            if unique_count > 100:
                validation_report.append(f"   ℹ {col}: High cardinality ({unique_count} unique values)")
        
        if validation_report:
            for report in validation_report[:10]:  # Show first 10
                print(report)
        else:
            print("   ✓ No major data quality issues detected")
        
        return self
    
    def create_summary_statistics(self):
        """
        CLEANING OPTION 9: Generate summary statistics
        """
        print(f"\n📊 Generating summary statistics...")
        
        summary = {
            'Total Responses': len(self.survey_raw),
            'Total Questions': len(self.survey_raw.columns),
            'Numeric Questions': len(self.survey_raw.select_dtypes(include=[np.number]).columns),
            'Text Questions': len(self.survey_raw.select_dtypes(include=['object']).columns),
            'Missing Values': self.survey_raw.isnull().sum().sum(),
            'Average Completeness': f"{self.survey_raw.notna().sum(axis=1).mean() / len(self.survey_raw.columns) * 100:.2f}%"
        }
        
        for key, value in summary.items():
            print(f"   {key}: {value}")
        
        return summary
    
    def export_cleaned_data(self, output_path, format='excel'):
        """
        Export cleaned data to file
        
        Parameters:
        -----------
        output_path : str
            Path for output file
        format : str
            'excel', 'csv', or 'both'
        """
        print(f"\n💾 Exporting cleaned data...")
        
        if format in ['excel', 'both']:
            with pd.ExcelWriter(output_path.replace('.csv', '.xlsx'), engine='openpyxl') as writer:
                self.survey_raw.to_excel(writer, sheet_name='Cleaned_Wide', index=False)
                if self.survey_unpivoted is not None:
                    self.survey_unpivoted.to_excel(writer, sheet_name='Unpivoted_Long', index=False)
            print(f"   ✓ Saved to Excel: {output_path.replace('.csv', '.xlsx')}")
        
        if format in ['csv', 'both']:
            self.survey_raw.to_csv(output_path.replace('.xlsx', '_wide.csv'), index=False)
            if self.survey_unpivoted is not None:
                self.survey_unpivoted.to_csv(output_path.replace('.xlsx', '_long.csv'), index=False)
            print(f"   ✓ Saved to CSV: {output_path.replace('.xlsx', '_wide.csv')}")
        
        return self


def main():
    """
    Main execution function with example usage
    """
    
    # File paths
    survey_path = r"D:\OneDrive\Work\Driver Survey\Outputs\survey_raw_database.xlsx"
    mapping_path = r"D:\OneDrive\Work\Driver Survey\DataSources\multiple_choice.xlsx"
    output_path = r"D:\OneDrive\Work\Driver Survey\Outputs\survey_cleaned.xlsx"
    
    print("="*70)
    print("SURVEY DATA CLEANING AND TRANSFORMATION")
    print("="*70)
    
    # Initialize cleaner
    cleaner = SurveyDataCleaner(survey_path, mapping_path)
    
    # Execute cleaning pipeline
    cleaner.load_data()
    
    # Apply all cleaning options
    cleaner.clean_column_names()
    cleaner.handle_missing_values(strategy='flag')  # Options: 'flag', 'drop_rows', 'drop_cols', 'impute_mode', 'impute_none'
    cleaner.remove_duplicates()
    cleaner.standardize_text_responses()
    cleaner.convert_data_types()
    cleaner.create_derived_variables()
    cleaner.unpivot_multiple_choice()
    cleaner.validate_data()
    
    # Summary
    summary = cleaner.create_summary_statistics()
    
    # Export
    cleaner.export_cleaned_data(output_path, format='both')
    
    print("\n" + "="*70)
    print("✅ CLEANING COMPLETE!")
    print("="*70)
    
    return cleaner


if __name__ == "__main__":
    cleaner = main()
