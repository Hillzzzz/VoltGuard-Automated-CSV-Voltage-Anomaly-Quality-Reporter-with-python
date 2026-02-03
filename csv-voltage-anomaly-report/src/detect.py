# src/detect.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple, Dict, Any

import pandas as pd
import numpy as np


# -----------------------------
# Configuration + Result Types
# -----------------------------

@dataclass
class DetectConfig:
    # Rolling window for local statistics
    window: int = 20

    # Z-score threshold
    zscore_threshold: float = 3.0

    # Step-change threshold (absolute volts)
    delta_threshold: float = 20.0

    # Minimum samples before detection starts
    min_periods: int = 10


@dataclass
class DetectReport:
    total_samples: int
    zscore_spikes: int
    delta_spikes: int
    combined_spikes: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_samples": self.total_samples,
            "zscore_spikes": self.zscore_spikes,
            "delta_spikes": self.delta_spikes,
            "combined_spikes": self.combined_spikes,
        }


# -----------------------------
# Core Detection Logic
# -----------------------------

def detect_spikes(
    df: pd.DataFrame,
    config: DetectConfig = DetectConfig(),
) -> Tuple[pd.DataFrame, DetectReport]:
    """
    Detect voltage spikes using rolling z-score and step-change logic.

    Required columns in df:
      - voltage (float)
      - timestamp (datetime) OR sample_index (int)

    Returns:
      spikes_df: rows flagged as abnormal with reasons
      report: summary counts
    """

    if "voltage" not in df.columns:
        raise ValueError("Input DataFrame must contain a 'voltage' column.")

    work = df.copy()

    v = work["voltage"]

    # Rolling statistics
    rolling_mean = v.rolling(
        window=config.window,
        min_periods=config.min_periods,
        center=False,
    ).mean()

    rolling_std = v.rolling(
        window=config.window,
        min_periods=config.min_periods,
        center=False,
    ).std()

    zscore = (v - rolling_mean) / rolling_std

    work["zscore"] = zscore
    work["zscore_spike"] = zscore.abs() > config.zscore_threshold

    # Step-change detection
    delta = v.diff().abs()
    work["delta"] = delta
    work["delta_spike"] = delta > config.delta_threshold

    # Combine logic
    work["is_spike"] = work["zscore_spike"] | work["delta_spike"]

    # Reason labeling (important for explainability)
    def _reason(row):
        reasons = []
        if row["zscore_spike"]:
            reasons.append("zscore")
        if row["delta_spike"]:
            reasons.append("delta")
        return "+".join(reasons)

    work["reason"] = work.apply(_reason, axis=1)

    spikes = work.loc[work["is_spike"]].copy()

    # Select useful columns only
    keep_cols = []
    if "timestamp" in spikes.columns:
        keep_cols.append("timestamp")
    if "sample_index" in spikes.columns:
        keep_cols.append("sample_index")

    keep_cols += ["voltage", "zscore", "delta", "reason"]

    spikes = spikes[keep_cols].reset_index(drop=True)

    report = DetectReport(
        total_samples=int(len(work)),
        zscore_spikes=int(work["zscore_spike"].sum()),
        delta_spikes=int(work["delta_spike"].sum()),
        combined_spikes=int(len(spikes)),
    )

    return spikes, report
