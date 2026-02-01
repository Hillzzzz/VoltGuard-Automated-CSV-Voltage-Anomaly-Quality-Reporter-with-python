import pandas as pd
import matplotlib.pyplot as plt
import os

def generate_reports(df, anomalies, output_dir="outputs"):
    """
    Saves CSVs, generates plots, and writes a summary text file.
    """
    # 1. Ensure output directory exists
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 2. Save CSVs
    df.to_csv(f"{output_dir}/cleaned.csv", index=False)
    anomalies.to_csv(f"{output_dir}/spikes.csv", index=False)

    # 3. Determine the X-axis (Time or Index)
    x_axis = 'timestamp' if 'timestamp' in df.columns else 'sample_index'

    # 4. Plot 1: Standard Line Plot
    plt.figure(figsize=(12, 6))
    plt.plot(df[x_axis], df['voltage'], label='Voltage', color='blue', alpha=0.7)
    plt.title('Voltage Over Time')
    plt.xlabel('Time/Sample')
    plt.ylabel('Voltage (V)')
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.savefig(f"{output_dir}/voltage_plot.png")

    # 5. Plot 2: Voltage with Spikes Highlighted
    plt.scatter(anomalies[x_axis], anomalies['voltage'], color='red', label='Anomalies', zorder=5)
    plt.legend()
    plt.title('Voltage Anomalies Detected')
    plt.savefig(f"{output_dir}/voltage_spikes_plot.png")
    plt.close() # Close plot to free up memory

    # 6. Generate summary.txt
    summary_path = f"{output_dir}/summary.txt"
    with open(summary_path, 'w') as f:
        f.write("=== VOLTAGE ANOMALY REPORT ===\n")
        f.write(f"Total rows after cleaning: {len(df)}\n")
        f.write(f"Total anomalies detected:  {len(anomalies)}\n\n")
        
        f.write("--- Voltage Statistics ---\n")
        f.write(f"Min Voltage:  {df['voltage'].min():.2f}V\n")
        f.write(f"Max Voltage:  {df['voltage'].max():.2f}V\n")
        f.write(f"Mean Voltage: {df['voltage'].mean():.2f}V\n\n")
        
        f.write("--- Top 5 Extreme Spikes (by Z-Score) ---\n")
        top_spikes = anomalies.sort_values(by='z_score', ascending=False).head(5)
        f.write(top_spikes.to_string(index=False))

    print(f"Reports generated successfully in '{output_dir}/'")
