import pandas as pd
import numpy as np

# Engineering tip: Define constants at the top for easy tuning
DEFAULT_WINDOW_SIZE = 20
DEFAULT_THRESHOLD = 3.0

def detect_voltage_anomalies(df, window=DEFAULT_WINDOW_SIZE, threshold=DEFAULT_THRESHOLD):
    """
    Identifies voltage spikes/sags using a rolling Z-score.
    """
    # 1. Compute rolling statistics
    # 'center=True' looks at both sides of the window for better context
    rolling_mean = df['voltage'].rolling(window=window, center=True).mean()
    rolling_std = df['voltage'].rolling(window=window, center=True).std()

    # 2. Compute Z-score
    # Formula: (Value - Average) / Standard Deviation
    # We add a tiny value (1e-6) to std to avoid division by zero errors
    z_scores = (df['voltage'] - rolling_mean) / (rolling_std + 1e-6)

    # 3. Create a mask for outliers
    # abs() handles both spikes (positive) and sags (negative)
    is_anomaly = z_scores.abs() > threshold

    # 4. Extract anomaly rows
    # We use .copy() to avoid SettingWithCopyWarnings
    anomalies = df[is_anomaly].copy()
    
    # 5. Populate required fields
    anomalies['z_score'] = z_scores[is_anomaly]
    anomalies['reason'] = f"zscore > {threshold}"

    # 6. Return only the relevant summary columns
    # We check if timestamp or sample_index exists based on our previous step
    time_col = 'timestamp' if 'timestamp' in anomalies.columns else 'sample_index'
    
    return anomalies[[time_col, 'voltage', 'z_score', 'reason']]
