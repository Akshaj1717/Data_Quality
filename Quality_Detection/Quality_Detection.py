"""
Employee Data Quality Pipeline

This module performs end-to-end data quality checks on an employee dataset.
It includes:
- Schema validation
- Completeness checks
- Accuracy validation
- Consistency rules
- Timeliness checks
- Statistical anomaly detection
"""

import pandas as pd
import numpy as np
import re
from datetime import timedelta

# ============================================================
# CONFIGURATION SECTION
# ============================================================
# These values define what "good data" looks like.
# In real systems, these often live in config files or databases.

EXPECTED_COLUMNS = [
    "Employee_ID", "First_Name", "Last_Name", "Age",
    "Department_Region", "Status", "Join_Date",
    "Salary", "Email", "Phone",
    "Performance_Score", "Remote_Work"
]

VALID_STATUS = {"Active", "Inactive", "Pending"}
VALID_PERFORMANCE = {"Poor", "Average", "Good", "Excellent"}

AGE_RANGE = (18, 70)               # Realistic working age bounds
SALARY_RANGE = (30000, 250000)     # Salary sanity bounds
Z_SCORE_THRESHOLD = 3              # Used for anomaly detection


# ============================================================
# DATA LOADING & NORMALIZATION
# ============================================================

def load_data(path):
    """
    Load the CSV file and normalize key columns so checks work correctly.
    """
    df = pd.read_csv(path)

    # Convert Join_Date to datetime; invalid values become NaT
    df["Join_Date"] = pd.to_datetime(df["Join_Date"], errors="coerce")

    # Phone numbers should be treated as strings, not integers
    df["Phone"] = df["Phone"].astype(str)

    return df

# ============================================================
# 1. SCHEMA VALIDATION
# ============================================================

def schema_validation(df):
    """
    Ensures the dataset has the expected structure.
    Checks:
    - Required columns exist
    - Employee_ID is unique
    """
    errors = []

    # Check for missing columns
    for col in EXPECTED_COLUMNS:
        if col not in df.columns:
            errors.append(f"Missing column: {col}")

    # Employee_ID should be unique across rows
    duplicate_ids = df["Employee_ID"].duplicated().sum()
    if duplicate_ids > 0:
        errors.append(f"{duplicate_ids} duplicate Employee_ID values")

    return errors

# ============================================================
# 2. COMPLETENESS CHECKS
# ============================================================

def completeness_checks(df):
    """
    Calculates the percentage of missing values per column.
    Only returns columns where missing values exist.
    """
    # isnull().mean() gives fraction of missing values per column
    return df.isnull().mean()[lambda x: x > 0].to_dict()

# ============================================================
# 3. ACCURACY CHECKS
# ============================================================

def accuracy_checks(df):
    """
    Validates that values fall within expected ranges or formats.
    """
    issues = {}

    # Age should be within working age limits
    invalid_age = df[
        (df["Age"] < AGE_RANGE[0]) |
        (df["Age"] > AGE_RANGE[1])
    ]
    if not invalid_age.empty:
        issues["invalid_age"] = len(invalid_age)

    # Salary should fall within reasonable bounds
    invalid_salary = df[
        (df["Salary"] < SALARY_RANGE[0]) |
        (df["Salary"] > SALARY_RANGE[1])
    ]
    if not invalid_salary.empty:
        issues["invalid_salary"] = len(invalid_salary)

    # Email format validation using regex
    email_regex = r"^[\w\.-]+@[\w\.-]+\.\w+$"
    invalid_email = df[~df["Email"].str.match(email_regex, na=False)]
    if not invalid_email.empty:
        issues["invalid_email"] = len(invalid_email)

    # Phone number should be digits only, optional +, reasonable length
    invalid_phone = df[
        ~df["Phone"].str.match(r"^\+?\d{10,15}$")
    ]
    if not invalid_phone.empty:
        issues["invalid_phone"] = len(invalid_phone)

    return issues

# ============================================================
# 4. CONSISTENCY CHECKS
# ============================================================

def consistency_checks(df):
    """
    Checks logical consistency between columns.
    """
    issues = []

    # Business rule:
    # Inactive employees should not have "Excellent" performance
    inconsistent_perf = df[
        (df["Status"] == "Inactive") &
        (df["Performance_Score"] == "Excellent")
    ]

    if not inconsistent_perf.empty:
        issues.append(
            f"{len(inconsistent_perf)} inactive employees marked Excellent"
        )

    return issues

# ============================================================
# 5. TIMELINESS CHECKS
# ============================================================

def timeliness_checks(df):
    """
    Ensures dates make sense relative to today.
    """
    today = pd.Timestamp.today()

    # Employees cannot join in the future
    future_joins = df[df["Join_Date"] > today]

    # Employees joining more than 40 years ago are suspicious
    old_joins = df[df["Join_Date"] < today - timedelta(days=40*365)]

    return {
        "future_join_dates": len(future_joins),
        "very_old_join_dates": len(old_joins)
    }

# ============================================================
# 6. ANOMALY DETECTION
# ============================================================

def anomaly_detection(df, column):
    """
    Detects statistical outliers using Z-score.
    Useful for catching extreme salaries or ages.
    """
    # Z-score calculation
    z_scores = (df[column] - df[column].mean()) / df[column].std()

    # Values beyond threshold are considered anomalies
    anomalies = df[np.abs(z_scores) > Z_SCORE_THRESHOLD]

    return anomalies[
        ["Employee_ID", column]
    ].to_dict(orient="records")


# ============================================================
# FULL DATA QUALITY PIPELINE
# ============================================================

def run_employee_dq_pipeline(csv_path):
    """
    Runs all data quality checks and returns a unified report.
    """
    df = load_data(csv_path)

    return {
        "schema_errors": schema_validation(df),
        "missing_values": completeness_checks(df),
        "accuracy_issues": accuracy_checks(df),
        "consistency_issues": consistency_checks(df),
        "timeliness_issues": timeliness_checks(df),
        "salary_anomalies": anomaly_detection(df, "Salary"),
        "age_anomalies": anomaly_detection(df, "Age")
    }
