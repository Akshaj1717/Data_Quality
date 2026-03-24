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
# ENTRY POINT
# =============================================================
# When you run `fastmcp run mcp_main.py`, this starts the server.

if __name__ == "__main__":
    mcp.run()
