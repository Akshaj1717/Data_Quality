import pandas as pd

REQUIRED_COLUMNS = [
    "Employee_ID",
    "First_Name",
    "Last_Name",
    "Email",
    "Department_Region"
]

OPTIONAL_COLUMNS = [
    "Phone",
    "Salary",
    "HireDate"
]


def check_schema(df: pd.DataFrame):
    """
    Runs schema-level quality checks:
    - Required columns
    - Missing values
    - Duplicate primary keys
    """

    issues = []

    # Normalize column names
    df.columns = df.columns.str.strip()

    # -----------------------------
    # 1. Required column checks
    # -----------------------------
    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            issues.append({
                "category": "SCHEMA_MISSING_COLUMN",
                "column": col,
                "severity": "CRITICAL",
                "message": f"Required column '{col}' is missing"
            })

    # If required columns are missing, stop early
    if issues:
        return issues

    # -----------------------------
    # 2. Missing value checks
    # -----------------------------
    for col in REQUIRED_COLUMNS:
        missing_pct = df[col].isna().mean() * 100

        if missing_pct > 0:
            issues.append({
                "category": "MISSING_VALUES",
                "column": col,
                "missing_pct": round(missing_pct, 2),
                "severity": "HIGH",
                "message": f"{missing_pct:.2f}% missing values"
            })

    # -----------------------------
    # 3. Primary key uniqueness
    # -----------------------------
    if "Employee_ID" in df.columns:
        duplicate_count = df["Employee_ID"].duplicated().sum()

        if duplicate_count > 0:
            issues.append({
                "category": "DUPLICATE_PRIMARY_KEY",
                "column": "Employee_ID",
                "count": int(duplicate_count),
                "severity": "CRITICAL",
                "message": "Duplicate Employee_ID values detected"
            })

    return issues
