"""
Generic Dataset Health Classification
=======================================
Determines the overall health status of a dataset based on row-level scores.

This is a direct adaptation of Quality_Detection/health.py.
Same logic, same thresholds — just repackaged for the generic pipeline.

Health Levels:
  GOOD:     Average score >= 85 AND bad rows <= 5%
  DEGRADED: Average score >= 70 AND bad rows <= 20%
  FAIL:     Everything else
"""

import pandas as pd


def classify_generic_health(df: pd.DataFrame):
    """
    Computes overall dataset health based on the row-level quality scores.

    Expects the DataFrame to already have a 'Row_Quality_Score' column
    (added by generic_scoring.py).

    Args:
        df: DataFrame with 'Row_Quality_Score' column

    Returns:
        A dict with:
        - dataset_health: "GOOD", "DEGRADED", or "FAIL"
        - average_row_score: mean of all row scores
        - bad_row_percentage: % of rows with score < 70
        - rows_analyzed: total number of rows
        - good_count: rows classified as GOOD
        - warning_count: rows classified as WARNING
        - bad_count: rows classified as BAD
    """

    # Calculate the average quality score across all rows
    avg_score = df["Row_Quality_Score"].mean()

    # Calculate the percentage of "bad" rows (score < 70)
    # This threshold matches Quality_Detection/health.py
    bad_row_pct = (df["Row_Quality_Score"] < 70).mean() * 100

    # Classify overall health using the same logic as health.py:
    #   GOOD: high average AND very few bad rows
    #   DEGRADED: decent average AND moderate bad rows
    #   FAIL: poor average OR too many bad rows
    if avg_score >= 85 and bad_row_pct <= 5:
        status = "GOOD"
    elif avg_score >= 70 and bad_row_pct <= 20:
        status = "DEGRADED"
    else:
        status = "FAIL"

    # Count rows in each usability category
    # (these come from generic_scoring.py's classify() function)
    good_count = int((df["Row_Usability_Status"] == "GOOD").sum())
    warning_count = int((df["Row_Usability_Status"] == "WARNING").sum())
    bad_count = int((df["Row_Usability_Status"] == "BAD").sum())

    return {
        "dataset_health": status,
        "average_row_score": round(avg_score, 2),
        "bad_row_percentage": round(bad_row_pct, 2),
        "rows_analyzed": len(df),
        "good_count": good_count,
        "warning_count": warning_count,
        "bad_count": bad_count
    }
