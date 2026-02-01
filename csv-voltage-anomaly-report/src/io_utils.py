import pandas as pd
import os

def load_csv_data(file_path):
    """
    Loads a CSV file into a DataFrame and prints its dimensions.
    """
    # Check if the file exists to avoid a crash
    if not os.path.exists(file_path):
        print(f"Error: The file at {file_path} was not found.")
        return None

    try:
        # Load the CSV
        df = pd.read_csv(file_path)
        
        # Get dimensions
        rows, cols = df.shape
        
        print(f"--- Data Summary ---")
        print(f"File: {os.path.basename(file_path)}")
        print(f"Rows: {rows}")
        print(f"Columns: {cols}")
        print(f"--------------------")
        
        return df
        
    except Exception as e:
        print(f"An error occurred while loading the CSV: {e}")
        return None
