import pandas as pd
import numpy as np
from datetime import timedelta

# --- Helper Function for HH:MM:SS Formatting ---
def format_seconds_to_hms(seconds):
    """Converts a total number of seconds (float or int) to HH:MM:SS string."""
    if pd.isna(seconds) or seconds is None or seconds == 0:
        return np.nan
    
    # Convert to total integer seconds, rounding to handle potential float inaccuracies
    total_seconds = int(round(seconds))
    
    # Use timedelta for reliable conversion
    td = timedelta(seconds=total_seconds)
    
    # str(timedelta) returns H:MM:SS or D days, H:MM:SS. 
    # Since durations are < 1 day, it should be H:MM:SS.
    return str(td)

# --- Data Loading and Setup ---
df = pd.read_csv(r"D:\Work\Automation Project\DataSources\carpooling_export.csv")
df['travel_date'] = pd.to_datetime(df['travel_date'])
df['week_number'] = df['travel_date'].dt.isocalendar().week


# req_to_pair_s (Snapp)
df['req_to_pair_s_sec'] = np.where(
    df['snapp_paired'] == "Yes",
    df['snapp_pair_time'],
    np.nan
)
df['req_to_pair_s_hms'] = df['req_to_pair_s_sec'].apply(format_seconds_to_hms)


# req_to_pair_t (Tapsi)
df['req_to_pair_t_sec'] = np.where(
    df['tapsi_paired'] == "Yes",
    df['tapsi_pair_time'],
    np.nan
)
df['req_to_pair_t_hms'] = df['req_to_pair_t_sec'].apply(format_seconds_to_hms)


# pair_to_acc_s (Snapp)
df['pair_to_acc_s_sec'] = np.where(
    df['snapp_accepted'] == "Yes",
    df['snapp_acc_time'],
    np.nan
)
df['pair_to_acc_s_hms'] = df['pair_to_acc_s_sec'].apply(format_seconds_to_hms)


# pair_to_acc_tapsi (Tapsi)
df['pair_to_acc_tapsi_sec'] = np.where(
    df['tapsi_accepted'] == "Yes",
    df['tapsi_acc_time'],
    np.nan
)
df['pair_to_acc_tapsi_hms'] = df['pair_to_acc_tapsi_sec'].apply(format_seconds_to_hms)


# --- Step 2: Calculate cumulative duration columns (Sum seconds, then format) ---

# req_to_acc_s (Snapp Total)
df['req_to_acc_s_sec'] = np.where(
    df['snapp_accepted'] == "Yes",
    df['req_to_pair_s_sec'] + df['pair_to_acc_s_sec'],
    np.nan
)
df['req_to_acc_s_hms'] = df['req_to_acc_s_sec'].apply(format_seconds_to_hms)


# req_to_acc_t (Tapsi Total)
df['req_to_acc_t_sec'] = np.where(
    df['tapsi_accepted'] == "Yes",
    df['req_to_pair_t_sec'] + df['pair_to_acc_tapsi_sec'],
    np.nan
)
df['req_to_acc_t_hms'] = df['req_to_acc_t_sec'].apply(format_seconds_to_hms)



# Export to CSV
df.to_csv('my_dataframe_output.csv', index=False, encoding='utf-8-sig')

print("DataFrame exported successfully to 'my_dataframe_output.csv'")
