
import pandas as pd
import numpy as np
from datetime import time

CSV_PATH = r"D:\Work\Automation Project\DataSources\carpooling_export_11_10_to_01_21.csv"
EXCEL_PATH = r"D:\Work\Automation Project\DataSources\AllAvailableRoutes.xlsx"
REAL_DATA_PATH = r"D:\Work\Automation Project\DataSources\real_data_11_10_to_01_23.csv"


def load_data(csv_path, excel_path, real_data_path):
    return (
        pd.read_csv(csv_path, encoding="utf-8-sig"),
        pd.read_excel(excel_path),
        pd.read_csv(real_data_path, encoding="utf-8-sig")
    )


def main():
    df, routes_df, real_data_df = load_data(
        CSV_PATH, EXCEL_PATH, REAL_DATA_PATH)

    print("\n" + "="*60)
    print("=== REAL DATA INSPECTION ===")
    print("="*60)
    print(f"\nReal data shape: {real_data_df.shape}")
    print(f"\nReal data columns:\n{real_data_df.columns.tolist()}")
    print(f"\nFirst few rows:")
    print(real_data_df.head(10))
    print(f"\nData types:")
    print(real_data_df.dtypes)
    print("="*60 + "\n")

    df = (
        df.pipe(prepare_base_df)
          .pipe(add_time_features)
          .pipe(merge_routes, routes_df)
    )

    real_data_df = prepare_real_data(real_data_df)

    # Build tables with real data integration
    table_from = build_table_with_real_data(
        df,
        real_data_df,
        dims=['week_number', 'city', 'from_coded'],
        first_two_dims=['week_number', 'city']
    )

    table_time = build_table_with_real_data(
        df,
        real_data_df,
        dims=['week_number', 'city', 'time_bucket'],
        first_two_dims=['week_number', 'city']
    )

    table_distance = build_table_with_real_data(
        df,
        real_data_df,
        dims=['week_number', 'city', 'distance_bucket'],
        first_two_dims=['week_number', 'city']
    )

    # Save outputs
    table_from.to_csv(OUTPUT_FROM, index=False, encoding="utf-8-sig")
    table_time.to_csv(OUTPUT_TIME, index=False, encoding="utf-8-sig")
    table_distance.to_csv(OUTPUT_DISTANCE, index=False, encoding="utf-8-sig")

    print("✅ Aggregation complete with real data integration.")
    print(f"   - {OUTPUT_FROM}")
    print(f"   - {OUTPUT_TIME}")
    print(f"   - {OUTPUT_DISTANCE}")


if __name__ == "__main__":
    main()
