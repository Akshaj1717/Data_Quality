"""
Generic Row-Level Quality Scoring
===================================
This module assigns a quality score (0–100) to each ROW in the dataset.

Modeled after Quality_Detection/row_scoring.py, but instead of checking
specific employee columns (Employee_ID, Email, etc.), it applies
universal scoring rules that work on any dataset:

Scoring Penalties:
  - Missing value in any column:    -5 points per missing field
  - Row is a full duplicate:        -30 points
  - Value is an outlier in its column: -10 points per outlier field

After scoring, each row is classified:
  - GOOD:    score >= 85  (row is usable as-is)
  - WARNING: score >= 70  (row has some issues, might need review)
  - BAD:     score <  70  (row has significant quality problems)

These thresholds match Quality_Detection/row_scoring.py for consistency.
"""

import pandas as pd
import numpy as np


def calculate_generic_row_scores(df: pd.DataFrame):
    """
    Adds 'Row_Quality_Score' and 'Row_Usability_Status' columns to the DataFrame.

    This function iterates through every row and applies penalty-based scoring.
    Each row starts at 100 and loses points for each quality issue found.

    Args:
        df: The input DataFrame (any dataset)

    Returns:
        The same DataFrame with two new columns added:
        - Row_Quality_Score: integer 0–100
        - Row_Usability_Status: "GOOD", "WARNING", or "BAD"
    """

    # -------------------------------------------------------
    # PRE-COMPUTATION: Figure out which columns are numeric
    # and pre-calculate outlier bounds for each.
    # We do this BEFORE the row loop so we don't recalculate
    # IQR statistics on every single row (performance).
    # -------------------------------------------------------
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

    # Store IQR bounds for each numeric column
    # Key: column name, Value: (lower_bound, upper_bound)
    outlier_bounds = {}
    for col in numeric_cols:
        values = df[col].dropna()
        if len(values) >= 4:
            q1 = values.quantile(0.25)
            q3 = values.quantile(0.75)
            iqr = q3 - q1
            outlier_bounds[col] = (q1 - 1.5 * iqr, q3 + 1.5 * iqr)

    # -------------------------------------------------------
    # PRE-COMPUTATION: Identify which rows are exact duplicates.
    # keep=False marks ALL copies (including the first occurrence)
    # so every duplicate row gets penalized, not just the second one.
    # -------------------------------------------------------
    duplicate_mask = df.duplicated(keep=False)

    # -------------------------------------------------------
    # ROW-BY-ROW SCORING
    # -------------------------------------------------------
    scores = []

    for idx, row in df.iterrows():
        # Every row starts with a perfect score
        score = 100

        # PENALTY 1: Missing values
        # Deduct 5 points for each column that has a missing value.
        # We cap the total penalty at 50 points so a row with many
        # columns doesn't immediately drop to 0.
        missing_count = int(row.isna().sum())
        missing_penalty = min(missing_count * 5, 50)
        score -= missing_penalty

        # PENALTY 2: Duplicate row
        # If this row is an exact copy of another row, deduct 30 points.
        # This matches the employee scorer's duplicate penalty.
        if duplicate_mask.iloc[idx]:
            score -= 30

        # PENALTY 3: Outlier values in numeric columns
        # If a numeric value falls outside the IQR bounds, deduct 10 points
        # per outlier column. Cap at 30 points total for outlier penalties.
        outlier_penalty = 0
        for col in numeric_cols:
            if col in outlier_bounds and not pd.isna(row[col]):
                lower, upper = outlier_bounds[col]
                if row[col] < lower or row[col] > upper:
                    outlier_penalty += 10
        score -= min(outlier_penalty, 30)

        # Ensure score never goes below 0
        scores.append(max(score, 0))

    # Add the scores as a new column
    df["Row_Quality_Score"] = scores

    # -------------------------------------------------------
    # USABILITY CLASSIFICATION
    # -------------------------------------------------------
    # These thresholds match Quality_Detection/row_scoring.py:
    #   >= 85 → GOOD
    #   >= 70 → WARNING
    #   <  70 → BAD
    def classify(score):
        if score >= 85:
            return "GOOD"
        elif score >= 70:
            return "WARNING"
        return "BAD"

    df["Row_Usability_Status"] = df["Row_Quality_Score"].apply(classify)

    return df
