import sys
import os
import json
from pathlib import Path # Modern way to handle folders/files

from io_utils import load_csv_data
from cleaning import run_cleaning_pipeline
# NEW IMPORTS
from detect import detect_spikes, DetectConfig 

def main():
    input_file = "data/sample_messy.csv"
    output_folder = Path("outputs") # Using Path object for easier "/" syntax
    output_folder.mkdir(exist_ok=True) # Ensure folder exists

    try:
        # --- LOADING & CLEANING (Steps 2 & 3) ---
        raw_df = load_csv_data(input_file)
        clean_df = run_cleaning_pipeline(raw_df)

        # --- 4. NEW ANALYZE LOGIC ---
        print("Detecting anomalies using Z-Score and Delta thresholds...")
        
        # We initialize our 'Settings' object
        config = DetectConfig(
            window=20,
            zscore_threshold=3.0,
            delta_threshold=20.0
        )

        # We run the detection
        spikes, detect_report = detect_spikes(clean_df, config)

        # --- 5. SAVING RESULTS ---
        spikes_path = output_folder / "spikes.csv"
        report_path = output_folder / "detection_report.json"

        # Save the detailed table of anomalies
        spikes.to_csv(spikes_path, index=False)

        # Save the high-level summary as JSON
        report_path.write_text(json.dumps(detect_report.to_dict(), indent=2))

        print("-" * 50)
        print(f"⚡ Spikes saved to: {spikes_path}")
        print(f"📊 Detection report saved to: {report_path}")
        print(json.dumps(detect_report.to_dict(), indent=2))

    except Exception as e:
        print(f"CRITICAL ERROR: {e}")

if __name__ == "__main__":
    main()
