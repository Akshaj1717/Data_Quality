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
# TOOL 6: VALIDATE COLUMN RANGE
# =============================================================
# Checks if numeric values in a column are within a specific min and max.
# This mimics the "Age 18-70" or "Salary > 0" logic from the employee app.

@mcp.tool()
def validate_column_range(csv_path: str, column: str, min_val: float = None, max_val: float = None) -> dict:
    """
    Checks if numeric values in a column are within a specific [min, max] range.
    Returns the count and list of violations (out-of-bounds rows).

    Use this tool to enforce business context (e.g., "Age must be 18-70").
    """
    df = pd.read_csv(csv_path)

    if column not in df.columns:
        return {"error": f"Column '{column}' not found."}

    # Convert to numeric, handle NaN
    vals = pd.to_numeric(df[column], errors="coerce")
    non_null_mask = vals.notna()

    violations = []

    # Check for Min
    if min_val is not None:
        too_low_mask = non_null_mask & (vals < min_val)
        if too_low_mask.any():
            violations.append({
                "type": "TOO_LOW",
                "threshold": min_val,
                "count": int(too_low_mask.sum())
            })

    # Check for Max
    if max_val is not None:
        too_high_mask = non_null_mask & (vals > max_val)
        if too_high_mask.any():
            violations.append({
                "type": "TOO_HIGH",
                "threshold": max_val,
                "count": int(too_high_mask.sum())
            })

    # Severity
    total_violations = sum(v["count"] for v in violations)
    severity = "HIGH" if total_violations > (len(df) * 0.1) else "MEDIUM" if total_violations > 0 else "LOW"

    return {
        "column": column,
        "total_rows_checked": int(non_null_mask.sum()),
        "violation_count": total_violations,
        "violations": violations,
        "severity": severity
    }


# =============================================================
# TOOL 7: CHECK SCHEMA VALIDITY
# =============================================================
# Compares the CSV's columns against a list provided by the LLM.
# Ensures the dataset is "valid" for the intended business use.

@mcp.tool()
def check_schema_validity(csv_path: str, expected_columns: list[str]) -> dict:
    """
    Checks if the dataset contains all the required (expected) columns.
    Returns which columns are missing and which extra columns are present.

    Use this tool to verify the dataset's structure against a business schema.
    """
    df = pd.read_csv(csv_path, nrows=0) # Read only headers for speed
    actual_columns = df.columns.tolist()

    missing = [col for col in expected_columns if col not in actual_columns]
    extra = [col for col in actual_columns if col not in expected_columns]

    status = "VALID" if not missing else "INVALID"

    return {
        "status": status,
        "actual_columns": actual_columns,
        "missing_columns": missing,
        "extra_columns": extra,
        "is_schema_match": len(missing) == 0
    }


# =============================================================
# TOOL 8: CHECK REGEX PATTERN
# =============================================================
# Validates string values against a regex pattern (Email, Phone, SSN).
# The pattern is provided by the LLM client, keeping it flexible.

@mcp.tool()
def check_regex_pattern(csv_path: str, column: str, pattern: str, description: str = "custom pattern") -> dict:
    """
    Validates a string column against a regular expression (regex).
    Returns the count of rows that DO NOT match the pattern.

    Common patterns:
    - Email: ^[\\w\\.-]+@[\\w\\.-]+\\.[a-zA-Z]{2,}$
    - Phone: ^\\d{3}-\\d{3}-\\d{4}$
    """
    df = pd.read_csv(csv_path)

    if column not in df.columns:
        return {"error": f"Column '{column}' not found."}

    # Clean data (remove NaN) for checking
    series = df[column].astype(str).replace('nan', None).dropna()

    import re
    compiled_regex = re.compile(pattern)

    # Find non-matches
    invalid_mask = series.apply(lambda x: not bool(compiled_regex.match(str(x))))
    invalid_count = int(invalid_mask.sum())
    invalid_pct = round((invalid_count / len(df)) * 100, 2)

    return {
        "column": column,
        "description": description,
        "invalid_count": invalid_count,
        "invalid_pct": invalid_pct,
        "severity": "HIGH" if invalid_pct > 5 else "MEDIUM" if invalid_pct > 0 else "LOW"
    }


# =============================================================
# ENTRY POINT
# =============================================================
# When you run `fastmcp run mcp_main.py`, this starts the server.

if __name__ == "__main__":
    mcp.run()
