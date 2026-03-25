"""
Data Quality MCP Server
========================
This is the TRUE MCP server for the Data Quality Engine.

Instead of hardcoded pipelines, this exposes individual data quality
checks as MCP Tools that any MCP client (like Claude Desktop) can
call autonomously.

How to run:
    fastmcp run mcp_main.py

How to connect from Claude Desktop:
    fastmcp install mcp_main.py
"""

from fastmcp import FastMCP
import pandas as pd
import numpy as np

# =============================================================
# CREATE THE MCP SERVER
# =============================================================
# This is the central server object. All tools are registered on it.
# The name "DataQuality" is what MCP clients will see when they connect.

mcp = FastMCP("DataQuality")


# =============================================================
# TOOL 1: GET DATASET SUMMARY
# =============================================================
# This is the first tool an LLM would call after receiving a CSV.
# It gives the LLM context about what it's working with:
# column names, data types, row count, and sample values.

@mcp.tool()
def get_dataset_summary(csv_path: str) -> dict:
    """
    Returns a summary of the dataset: column names, data types,
    row count, and a few sample values per column.

    Use this tool FIRST when analyzing a new dataset to understand
    what columns and data types are present.
    """
    df = pd.read_csv(csv_path)

    # Build a summary for each column
    columns = []
    for col in df.columns:
        # Grab up to 3 sample values (non-null) for context
        samples = df[col].dropna().unique()[:3].tolist()

        # Convert numpy types to native Python for clean output
        safe_samples = []
        for val in samples:
            if isinstance(val, (np.integer,)):
                safe_samples.append(int(val))
            elif isinstance(val, (np.floating,)):
                safe_samples.append(float(val))
            else:
                safe_samples.append(str(val))

        columns.append({
            "name": col,
            "dtype": str(df[col].dtype),
            "non_null_count": int(df[col].notna().sum()),
            "unique_count": int(df[col].nunique()),
            "sample_values": safe_samples
        })

    return {
        "total_rows": len(df),
        "total_columns": len(df.columns),
        "columns": columns
    }


# =============================================================
# TOOL 2: CHECK MISSING VALUES
# =============================================================
# Returns the completeness profile of the dataset.
# The LLM can use this to identify which columns have gaps.

@mcp.tool()
def check_missing_values(csv_path: str) -> dict:
    """
    Checks every column for missing (null/NaN) values.
    Returns the count and percentage of missing values per column,
    along with a severity rating (LOW / MEDIUM / HIGH).

    Use this tool to assess data COMPLETENESS.
    """
    df = pd.read_csv(csv_path)

    results = []
    for col in df.columns:
        missing_count = int(df[col].isna().sum())
        missing_pct = round((missing_count / len(df)) * 100, 2)

        # Severity thresholds:
        #   > 20% missing = HIGH (significant data gap)
        #   > 5% missing  = MEDIUM (notable)
        #   <= 5% missing = LOW (acceptable)
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

    # Quick overall summary
    total_cells = len(df) * len(df.columns)
    total_missing = int(df.isna().sum().sum())
    overall_completeness = round((1 - total_missing / total_cells) * 100, 2)

    return {
        "overall_completeness_pct": overall_completeness,
        "total_cells": total_cells,
        "total_missing": total_missing,
        "columns": results
    }


# =============================================================
# TOOL 3: FIND DUPLICATES
# =============================================================
# Checks for exact duplicate rows in the dataset.
# This maps to the UNIQUENESS pillar of data quality.

@mcp.tool()
def find_duplicates(csv_path: str) -> dict:
    """
    Detects fully duplicated rows in the dataset.
    Returns the count, percentage, and severity of duplication.

    Use this tool to assess data UNIQUENESS.
    """
    df = pd.read_csv(csv_path)

    # Count rows that are exact copies of another row
    duplicate_count = int(df.duplicated().sum())
    duplicate_pct = round((duplicate_count / len(df)) * 100, 2)

    # Severity based on duplication rate
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
# TOOL 4: DETECT NUMERIC OUTLIERS
# =============================================================
# Uses IQR (Interquartile Range) to find statistical outliers
# in a specific numeric column. The LLM decides WHICH column
# to check — this is what makes it flexible for any dataset.

@mcp.tool()
def detect_numeric_outliers(csv_path: str, column: str) -> dict:
    """
    Detects statistical outliers in a specific numeric column
    using the IQR (Interquartile Range) method.

    Args:
        csv_path: Path to the CSV file.
        column: The name of the numeric column to check.

    Use this tool to assess data VALIDITY for numeric fields.
    Call get_dataset_summary first to see which columns are numeric.
    """
    df = pd.read_csv(csv_path)

    # Validate the column exists
    if column not in df.columns:
        return {"error": f"Column '{column}' not found in dataset."}

    # Convert to numeric, coercing errors to NaN
    values = pd.to_numeric(df[column], errors="coerce").dropna()

    if len(values) < 4:
        return {"error": f"Column '{column}' has too few numeric values for outlier detection."}

    # IQR calculation
    q1 = values.quantile(0.25)
    q3 = values.quantile(0.75)
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr

    # Find outliers
    outlier_mask = (values < lower_bound) | (values > upper_bound)
    outlier_count = int(outlier_mask.sum())
    outlier_pct = round((outlier_count / len(values)) * 100, 2)

    # Severity
    if outlier_pct > 10:
        severity = "HIGH"
    elif outlier_pct > 5:
        severity = "MEDIUM"
    else:
        severity = "LOW"

    return {
        "column": column,
        "values_checked": len(values),
        "outlier_count": outlier_count,
        "outlier_pct": outlier_pct,
        "lower_bound": round(float(lower_bound), 2),
        "upper_bound": round(float(upper_bound), 2),
        "severity": severity
    }


# =============================================================
# TOOL 5: CHECK TYPE CONSISTENCY
# =============================================================
# For each column, infers the dominant data type and reports
# how many values don't match. Catches things like a numeric
# column with stray text entries mixed in.

@mcp.tool()
def check_type_consistency(csv_path: str) -> dict:
    """
    For each column, infers whether it is numeric, datetime, or text,
    then reports how many values are inconsistent with that type.

    Use this tool to assess data CONSISTENCY.
    """
    df = pd.read_csv(csv_path)

    results = []
    for col in df.columns:
        non_null = df[col].dropna()

        if len(non_null) == 0:
            results.append({
                "column": col,
                "inferred_type": "empty",
                "inconsistent_count": 0,
                "inconsistent_pct": 0.0
            })
            continue

        # Try numeric conversion
        numeric_converted = pd.to_numeric(non_null, errors="coerce")
        numeric_valid = numeric_converted.notna().sum()
        numeric_pct = numeric_valid / len(non_null)

        # Try datetime conversion
        datetime_converted = pd.to_datetime(non_null, errors="coerce")
        datetime_valid = datetime_converted.notna().sum()
        datetime_pct = datetime_valid / len(non_null)

        # Infer type: 80%+ of values matching = dominant type
        if numeric_pct >= 0.80:
            inferred_type = "numeric"
            inconsistent_count = int(len(non_null) - numeric_valid)
        elif datetime_pct >= 0.80:
            inferred_type = "datetime"
            inconsistent_count = int(len(non_null) - datetime_valid)
        else:
            inferred_type = "text"
            inconsistent_count = 0

        inconsistent_pct = round((inconsistent_count / len(non_null)) * 100, 2)

        results.append({
            "column": col,
            "inferred_type": inferred_type,
            "inconsistent_count": inconsistent_count,
            "inconsistent_pct": inconsistent_pct
        })

    return {"columns": results}


# =============================================================
# ENTRY POINT
# =============================================================
# When you run `fastmcp run mcp_main.py`, this starts the server.

if __name__ == "__main__":
    mcp.run()
