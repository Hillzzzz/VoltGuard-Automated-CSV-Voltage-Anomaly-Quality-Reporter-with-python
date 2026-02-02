import pandas as pd
import numpy as np

def clean_column_names(df):
    """Step 1: Standardize naming to a strict schema."""
    df.columns = [col.lower().strip() for col in df.columns]
    
    # Mapping logic
    mapping = {
        'v': 'voltage', 'volt': 'voltage', 'reading': 'voltage',
        'time': 'timestamp', 'date': 'timestamp'
    }
    # Check for partial matches or direct matches
    new_cols = {col: mapping[col] for col in df.columns if col in mapping}
    return df.rename(columns=new_cols)

def sanitize_voltage_values(df, min_v=10.0, max_v=15.0):
    """Step 2: Numeric conversion and physical boundary enforcement."""
    initial_count = len(df)
    
    # Clean string artifacts (like 'V' or 'v') before numeric conversion
    if df['voltage'].dtype == 'object':
        df['voltage'] = df['voltage'].str.replace(r'[Vv]', '', regex=True)
    
    # Coerce errors to NaN - this is where we "find" the garbage strings
    df['voltage'] = pd.to_numeric(df['voltage'], errors='coerce')
    
    # Create a mask for valid physics
    # Note: We keep NaNs for now to count them separately
    valid_physics = (df['voltage'] >= min_v) & (df['voltage'] <= max_v)
    
    # Count specific failures for the report
    type_failures = df['voltage'].isna().sum()
    physics_violations = (~valid_physics & df['voltage'].notna()).sum()
    
    # Apply the filter
    df = df[valid_physics].copy()
    
    return df, {"type_errors": type_failures, "physics_errors": physics_violations}

def finalize_time_index(df):
    """Step 3: Temporal alignment and duplicate removal."""
    if 'timestamp' not in df.columns:
        df['timestamp'] = pd.date_range(start='2026-01-01', periods=len(df), freq='5T')
    
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df = df.dropna(subset=['timestamp'])
    
    # Sort to ensure chronological order before checking duplicates
    df = df.sort_values('timestamp')
    dupes = df.duplicated(subset=['timestamp']).sum()
    df = df.drop_duplicates(subset=['timestamp'], keep='first')
    
    return df, {"duplicates": dupes}

def run_cleaning_pipeline(df, min_v=10.0, max_v=15.0):
    """The 'Bottled Judgment' Entry Point."""
    original_count = len(df)
    
    # Station 1: Schema
    df = clean_column_names(df)
    
    # Station 2: Voltage Logic
    df, v_stats = sanitize_voltage_values(df, min_v, max_v)
    
    # Station 3: Time Logic
    df, t_stats = finalize_time_index(df)
    
    # Final Reporting
    retention = (len(df) / original_count) * 100
    print(f"--- Pipeline Audit ---")
    print(f"Rows Retained: {len(df)} ({retention:.1f}%)")
    print(f"Failures: {v_stats['type_errors']} strings, {v_stats['physics_errors']} physics, {t_stats['duplicates']} dupes")
    
    return df
