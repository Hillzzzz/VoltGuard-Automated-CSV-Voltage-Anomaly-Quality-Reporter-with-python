import sys
import os

# Ensure the script can find its sister modules
sys.path.append(os.path.dirname(__file__))

from io_utils import load_csv_data
from cleaning import run_cleaning_pipeline
from detect import detect_voltage_anomalies
from report import generate_reports

def main():
    # 1. Define Paths
    # We assume you run this from the project root
    input_file = "data/sample_messy.csv"
    output_folder = "outputs"

    print(f"Starting Voltage Anomaly Report for: {input_file}")
    print("-" * 50)

    try:
        # 2. Extract: Load the raw data
        raw_df = load_csv_data(input_file)
        if raw_df is None:
            return

        # 3. Transform: Clean and Standardize
        # This handles column names, units, and time-sorting
        clean_df = run_cleaning_pipeline(raw_df)

        # 4. Analyze: Detect Spikes and Sags
        # Uses the rolling Z-score method
        anomalies = detect_voltage_anomalies(clean_df)

        # 5. Load/Output: Generate Files and Plots
        # Saves to the /outputs directory
        generate_reports(clean_df, anomalies, output_dir=output_folder)

        print("-" * 50)
        print("Process Complete! Check the 'outputs' folder for results.")

    except Exception as e:
        print(f"CRITICAL ERROR: {e}")

if __name__ == "__main__":
    main()
