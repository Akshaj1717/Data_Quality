"""
Generic Data Quality Checks
============================
This module performs universal quality checks that work on ANY CSV dataset.
It does NOT assume any specific column names or data domain.

Checks performed:
1. Completeness   — What % of values are missing in each column?
2. Duplicates     — Are there fully duplicated rows?
3. Type Consistency — Are values in each column consistently the same type?
4. Outlier Detection — Are there extreme values in numeric columns? (IQR method)

Design Notes:
- Modeled after Quality_Detection/schema_checks.py and anomaly_checks.py
- Returns structured dictionaries so results can be serialized to JSON via the API
- All functions take a pandas DataFrame as input (already loaded)
"""

import pandas as pd
import numpy as np


# =============================================================
# 1. COMPLETENESS CHECK
# =============================================================
# Mirrors Quality_Detection.Quality_Detection.completeness_checks()
# but works on ALL columns (not just employee-specific ones).

def check_completeness(df: pd.DataFrame):
    """
    Calculates the percentage of missing (null/NaN) values for every column.

    Returns:
        A list of dicts, one per column, with:
        - column: the column name
        - missing_count: how many rows have missing values
        - missing_pct: percentage of rows missing (0.0 to 100.0)
        - severity: HIGH if >20%, MEDIUM if >5%, LOW otherwise
    """
    results = []

    for col in df.columns:
        # Count how many values are null/NaN in this column
        missing_count = int(df[col].isna().sum())

        # Calculate percentage of missing values
        missing_pct = round((missing_count / len(df)) * 100, 2)

        # Assign severity based on how much data is missing
        # These thresholds match common data quality standards:
        #   >20% missing = HIGH severity (significant data gap)
        #   >5% missing  = MEDIUM severity (notable but not critical)
        #   <=5% missing = LOW severity (acceptable)
        if missing_pct > 20:
            severity = "HIGH"
        elif missing_pct > 5:
            severity = "MEDIUM"
        else:
            severity = "LOW"

        results.append({
            "column": col,
            "missing_count": missing_count,
            "missing_pct": missing_pct,
            "severity": severity
        })

    return results


# =============================================================
# 2. DUPLICATE ROW DETECTION
# =============================================================
# Mirrors Quality_Detection/schema_checks.py duplicate ID check
# but checks for FULL row duplicates (since we don't know which
# column is the primary key in an arbitrary dataset).

def check_duplicates(df: pd.DataFrame):
    """
    Detects fully duplicated rows in the dataset.

    Returns:
        A dict with:
        - total_rows: total number of rows in the dataset
        - duplicate_count: number of rows that are exact duplicates of another row
        - duplicate_pct: percentage of duplicate rows
        - severity: CRITICAL if >10%, HIGH if >5%, MEDIUM if >0%, LOW if 0%
    """
    # duplicated() marks rows that are exact copies of a previous row
    # keep=False would mark ALL copies; default (keep='first') marks
    # only the second, third, etc. occurrence — we use default here
    # so each duplicate is counted once.
    duplicate_count = int(df.duplicated().sum())
    duplicate_pct = round((duplicate_count / len(df)) * 100, 2)

    # Assign severity based on % of duplicates
    if duplicate_pct > 10:
        severity = "CRITICAL"
    elif duplicate_pct > 5:
        severity = "HIGH"
    elif duplicate_pct > 0:
        severity = "MEDIUM"
    else:
        severity = "LOW"

    return {
        "total_rows": len(df),
        "duplicate_count": duplicate_count,
        "duplicate_pct": duplicate_pct,
        "severity": severity
    }


# =============================================================
# 3. TYPE CONSISTENCY CHECK
# =============================================================
# This is a NEW check not present in Quality_Detection.
# For each column, we infer what the "dominant" data type is
# and flag any values that don't match. This catches things like
# a numeric column that has some text entries mixed in.

def check_type_consistency(df: pd.DataFrame):
    """
    For each column, determines the dominant data type and checks
    if all values are consistent with that type.

    Returns:
        A list of dicts, one per column, with:
        - column: the column name
        - inferred_type: the dominant type ("numeric", "datetime", "text")
        - inconsistent_count: how many non-null values don't match the dominant type
        - inconsistent_pct: percentage of inconsistent values
    """
    results = []

    for col in df.columns:
        # Drop nulls — we only check type consistency on actual values
        non_null = df[col].dropna()

        if len(non_null) == 0:
            # Entire column is empty — nothing to check
            results.append({
                "column": col,
                "inferred_type": "empty",
                "inconsistent_count": 0,
                "inconsistent_pct": 0.0
            })
            continue

        # Try to convert the column to numeric
        # errors='coerce' turns non-numeric values into NaN
        numeric_converted = pd.to_numeric(non_null, errors="coerce")
        numeric_valid_count = numeric_converted.notna().sum()
        numeric_pct = numeric_valid_count / len(non_null)

        # Try to convert the column to datetime
        # errors='coerce' turns non-date values into NaT
        # Note: infer_datetime_format was removed — pandas 2.x infers format automatically
        datetime_converted = pd.to_datetime(non_null, errors="coerce")
        datetime_valid_count = datetime_converted.notna().sum()
        datetime_pct = datetime_valid_count / len(non_null)

        # Decision logic for type inference:
        # If >80% of values are numeric → it's a numeric column
        # Else if >80% of values parse as dates → it's a datetime column
        # Otherwise → it's a text column
        # The 80% threshold allows for some dirty data while still
        # identifying the "intended" type.
        if numeric_pct >= 0.80:
            inferred_type = "numeric"
            # Inconsistent values are the ones that AREN'T numeric
            inconsistent_count = int(len(non_null) - numeric_valid_count)
        elif datetime_pct >= 0.80:
            inferred_type = "datetime"
            # Inconsistent values are the ones that AREN'T dates
            inconsistent_count = int(len(non_null) - datetime_valid_count)
        else:
            inferred_type = "text"
            # For text columns, all values are "consistent" by default
            inconsistent_count = 0

        inconsistent_pct = round((inconsistent_count / len(non_null)) * 100, 2)

        results.append({
            "column": col,
            "inferred_type": inferred_type,
            "inconsistent_count": inconsistent_count,
            "inconsistent_pct": inconsistent_pct
        })

    return results


# =============================================================
# 4. OUTLIER DETECTION (IQR Method)
# =============================================================
# Mirrors Quality_Detection/anomaly_checks.py salary outlier check
# but applies to ALL numeric columns automatically.
# Uses the Interquartile Range (IQR) method:
#   - Q1 = 25th percentile, Q3 = 75th percentile
#   - IQR = Q3 - Q1
#   - Outlier if value < Q1 - 1.5*IQR or value > Q3 + 1.5*IQR

def check_outliers(df: pd.DataFrame):
    """
    Detects statistical outliers in all numeric columns using the IQR method.

    Returns:
        A list of dicts, one per numeric column, with:
        - column: the column name
        - outlier_count: how many values fall outside the IQR bounds
        - outlier_pct: percentage of outlier values
        - lower_bound: the lower fence (Q1 - 1.5*IQR)
        - upper_bound: the upper fence (Q3 + 1.5*IQR)
        - severity: HIGH if >10%, MEDIUM if >5%, LOW otherwise
    """
    results = []

    # Select only numeric columns for outlier detection
    # This automatically skips text, date, and categorical columns
    numeric_cols = df.select_dtypes(include=[np.number]).columns

    for col in numeric_cols:
        # Drop nulls before computing statistics
        values = df[col].dropna()

        if len(values) < 4:
            # Need at least 4 values for meaningful IQR calculation
            continue

        # Calculate IQR boundaries
        q1 = values.quantile(0.25)
        q3 = values.quantile(0.75)
        iqr = q3 - q1

        # Define outlier fences
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr

        # Count values outside the fences
        outliers = (values < lower_bound) | (values > upper_bound)
        outlier_count = int(outliers.sum())
        outlier_pct = round((outlier_count / len(values)) * 100, 2)

        # Assign severity based on outlier percentage
        if outlier_pct > 10:
            severity = "HIGH"
        elif outlier_pct > 5:
            severity = "MEDIUM"
        else:
            severity = "LOW"

        results.append({
            "column": col,
            "outlier_count": outlier_count,
            "outlier_pct": outlier_pct,
            "lower_bound": round(float(lower_bound), 2),
            "upper_bound": round(float(upper_bound), 2),
            "severity": severity
        })

    return results


# =============================================================
# 5. COLUMN SUMMARY (Helper for dashboard display)
# =============================================================
# This provides a quick overview of each column — useful for
# the dashboard to show the user what their dataset contains.

def generate_column_summary(df: pd.DataFrame):
    """
    Generates a summary of each column in the dataset.

    Returns:
        A list of dicts, one per column, with:
        - column: the column name
        - dtype: the pandas dtype (int64, float64, object, etc.)
        - non_null_count: how many non-null values exist
        - unique_count: how many unique values exist
        - sample_values: up to 3 example values from the column
    """
    results = []

    for col in df.columns:
        # Get up to 3 unique non-null sample values for preview
        samples = df[col].dropna().unique()[:3].tolist()

        # Convert numpy types to native Python types for JSON serialization
        # (numpy int64, float64, etc. can't be serialized to JSON directly)
        safe_samples = []
        for val in samples:
            if isinstance(val, (np.integer,)):
                safe_samples.append(int(val))
            elif isinstance(val, (np.floating,)):
                safe_samples.append(float(val))
            else:
                safe_samples.append(str(val))

        results.append({
            "column": col,
            "dtype": str(df[col].dtype),
            "non_null_count": int(df[col].notna().sum()),
            "unique_count": int(df[col].nunique()),
            "sample_values": safe_samples
        })

    return results
