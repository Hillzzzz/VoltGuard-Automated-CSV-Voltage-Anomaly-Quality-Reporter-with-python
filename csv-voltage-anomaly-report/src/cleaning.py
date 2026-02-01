import pandas as pd

def clean_column_names(df):
    """
    Step 1: Lowercase and map columns to 'voltage' and 'timestamp'.
    """
    # Lowercase everything for easier matching
    df.columns = [col.lower() for col in df.columns]
    
    voltage_candidates = ["voltage", "v", "volt", "reading"]
    time_candidates = ["timestamp", "time", "date"]
    
    # Map Voltage Column
    chosen_voltage = next((col for col in df.columns if col in voltage_candidates), None)
    if not chosen_voltage:
        raise ValueError(f"Could not find a voltage column. Looked for: {voltage_candidates}")

    # Map Timestamp Column
    chosen_time = next((col for col in df.columns if col in time_candidates), None)
    
    rename_dict = {chosen_voltage: "voltage"}
    if chosen_time:
        rename_dict[chosen_time] = "timestamp"
    
    return df.rename(columns=rename_dict)

def sanitize_voltage_values(df, min_v=0, max_v=1000):
    """
    Step 2: Convert voltage strings to clean floats and remove outliers.
    """
    # Remove units and whitespace, then convert to numeric
    df['voltage'] = (
        df['voltage']
        .astype(str)
        .str.replace(r'[Vv]', '', regex=True)
        .str.strip()
    )
    
    # initial count for reporting
    initial_len = len(df)
    df['voltage'] = pd.to_numeric(df['voltage'], errors='coerce')
    
    # Drop rows where conversion failed (NaN)
    df = df.dropna(subset=['voltage'])
    
    # Filter by physical bounds
    df = df[(df['voltage'] >= min_v) & (df['voltage'] <= max_v)]
    
    removed = initial_len - len(df)
    if removed > 0:
        print(f"Sanitization: Removed {removed} invalid or out-of-range voltage rows.")
        
    return df

def finalize_time_index(df):
    """
    Step 3: Handle sorting, duplicates, and missing time data.
    """
    if 'timestamp' in df.columns:
        # Convert and drop invalid dates
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        df = df.dropna(subset=['timestamp'])

        # Sort and drop duplicates
        df = df.sort_values(by='timestamp')
        
        dupes = df.duplicated(subset=['timestamp']).sum()
        if dupes > 0:
            print(f"Time Index: Dropped {dupes} duplicate timestamps.")
            df = df.drop_duplicates(subset=['timestamp'], keep='first')
    else:
        # Fallback to a simple index
        print("Time Index: No timestamp found. Using sequential index.")
        df['sample_index'] = range(len(df))
        # Move sample_index to the first column
        cols = ['sample_index'] + [c for c in df.columns if c != 'sample_index']
        df = df[cols]

    return df.reset_index(drop=True)

def run_cleaning_pipeline(df):
    """
    Helper function to run the entire cleaning process in order.
    """
    df = clean_column_names(df)
    df = sanitize_voltage_values(df)
    df = finalize_time_index(df)
    return df
