"""
Generic Quality Pipeline
==========================
Orchestrates all generic quality checks into a single analysis run.

This is the equivalent of Quality_Detection/full_pipeline.py but for
generic (any CSV) datasets. It calls each check module in sequence
and returns a unified report dictionary.

Pipeline Steps:
  1. Load the CSV file
  2. Generate column summary (metadata about each column)
  3. Run completeness checks (missing values)
  4. Run duplicate detection (full row duplicates)
  5. Run type consistency checks (mixed types in columns)
  6. Run outlier detection (IQR method on numeric columns)
  7. Calculate row-level quality scores
  8. Classify overall dataset health

The returned report dictionary is designed to be JSON-serializable
so it can be returned directly by the FastAPI endpoint.
"""

import pandas as pd

# Import all check functions from our generic checks module
from .generic_checks import (
    check_completeness,
    check_duplicates,
    check_type_consistency,
    check_outliers,
    generate_column_summary
)

# Import the row-level scoring function
from .generic_scoring import calculate_generic_row_scores

# Import the health classification function
from .generic_health import classify_generic_health


def load_generic_data(csv_path: str) -> pd.DataFrame:
    """
    Loads a CSV file into a pandas DataFrame.

    This is a simple wrapper around pd.read_csv, similar to
    Quality_Detection.Quality_Detection.load_data(), but without
    any employee-specific column transformations.

    Args:
        csv_path: Path to the CSV file on disk

    Returns:
        A pandas DataFrame containing the CSV data
    """
    df = pd.read_csv(csv_path)
    return df


def run_generic_pipeline(csv_path: str) -> dict:
    """
    Runs the full generic quality analysis pipeline on a CSV file.

    This is the main entry point for generic analysis — equivalent to
    Quality_Detection.full_pipeline.run_full_analysis() but dataset-agnostic.

    Args:
        csv_path: Path to the CSV file to analyze

    Returns:
        A dict containing the full analysis report with:
        - tool: identifier for this analysis type
        - status: overall pass/fail status
        - dataset_info: basic dataset metadata (rows, columns)
        - column_summary: per-column metadata
        - completeness: missing value analysis
        - duplicates: duplicate row analysis
        - type_consistency: type inference and consistency
        - outliers: statistical outlier detection
        - health: overall dataset health classification
        - row_scores_preview: first 20 rows with their scores
    """

    # ---------------------------------------------------------
    # STEP 1: Load the dataset
    # ---------------------------------------------------------
    df = load_generic_data(csv_path)

    # ---------------------------------------------------------
    # STEP 2: Generate column summary
    # This gives the dashboard metadata about each column
    # (data types, unique counts, sample values)
    # ---------------------------------------------------------
    column_summary = generate_column_summary(df)

    # ---------------------------------------------------------
    # STEP 3: Run completeness checks
    # Finds missing values in every column
    # ---------------------------------------------------------
    completeness = check_completeness(df)

    # ---------------------------------------------------------
    # STEP 4: Run duplicate detection
    # Counts exact duplicate rows
    # ---------------------------------------------------------
    duplicates = check_duplicates(df)

    # ---------------------------------------------------------
    # STEP 5: Run type consistency checks
    # Detects mixed data types within columns
    # ---------------------------------------------------------
    type_consistency = check_type_consistency(df)

    # ---------------------------------------------------------
    # STEP 6: Run outlier detection
    # Finds IQR-based outliers in numeric columns
    # ---------------------------------------------------------
    outliers = check_outliers(df)

    # ---------------------------------------------------------
    # STEP 7: Calculate row-level quality scores
    # Adds Row_Quality_Score and Row_Usability_Status columns
    # ---------------------------------------------------------
    df = calculate_generic_row_scores(df)

    # ---------------------------------------------------------
    # STEP 8: Classify overall dataset health
    # GOOD / DEGRADED / FAIL based on score distribution
    # ---------------------------------------------------------
    health = classify_generic_health(df)

    # ---------------------------------------------------------
    # DETERMINE OVERALL STATUS
    # If health is FAIL → status is FAIL
    # If health is DEGRADED → status is WARNING
    # Otherwise → status is PASS
    # ---------------------------------------------------------
    if health["dataset_health"] == "FAIL":
        overall_status = "FAIL"
    elif health["dataset_health"] == "DEGRADED":
        overall_status = "WARNING"
    else:
        overall_status = "PASS"

    # ---------------------------------------------------------
    # COLLECT ANOMALIES
    # Gather all issues with severity HIGH or CRITICAL into
    # a flat list so the dashboard can highlight them easily.
    # ---------------------------------------------------------
    anomalies = []

    # From completeness: flag columns with HIGH missing %
    for item in completeness:
        if item["severity"] in ("HIGH", "CRITICAL"):
            anomalies.append({
                "type": "MISSING_VALUES",
                "column": item["column"],
                "detail": f"{item['missing_pct']}% values missing",
                "severity": item["severity"]
            })

    # From duplicates: flag if severity is HIGH or CRITICAL
    if duplicates["severity"] in ("HIGH", "CRITICAL"):
        anomalies.append({
            "type": "DUPLICATE_ROWS",
            "column": "ALL",
            "detail": f"{duplicates['duplicate_count']} duplicate rows ({duplicates['duplicate_pct']}%)",
            "severity": duplicates["severity"]
        })

    # From outliers: flag columns with HIGH outlier %
    for item in outliers:
        if item["severity"] in ("HIGH", "CRITICAL"):
            anomalies.append({
                "type": "OUTLIERS",
                "column": item["column"],
                "detail": f"{item['outlier_count']} outliers detected ({item['outlier_pct']}%)",
                "severity": item["severity"]
            })

    # From type consistency: flag columns with >5% inconsistent values
    for item in type_consistency:
        if item["inconsistent_pct"] > 5:
            anomalies.append({
                "type": "TYPE_INCONSISTENCY",
                "column": item["column"],
                "detail": f"{item['inconsistent_count']} values don't match inferred type '{item['inferred_type']}'",
                "severity": "MEDIUM"
            })

    # ---------------------------------------------------------
    # BUILD FINAL REPORT
    # ---------------------------------------------------------
    return {
        "tool": "generic_analysis",
        "status": overall_status,
        "dataset_info": {
            "total_rows": len(df),
            "total_columns": len(df.columns),
            "column_names": df.columns.tolist()
        },
        "column_summary": column_summary,
        "completeness": completeness,
        "duplicates": duplicates,
        "type_consistency": type_consistency,
        "outliers": outliers,
        "health": health,
        "anomalies": anomalies,
        "row_scores_preview": df[
            ["Row_Quality_Score", "Row_Usability_Status"]
        ].head(20).to_dict(orient="records")
    }
