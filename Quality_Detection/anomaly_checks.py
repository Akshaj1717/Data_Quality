import pandas as pd
import re

EMAIL_REGEX = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"

def check_anomalies(df: pd.DataFrame):
    """
        Runs anomaly-level data quality checks:
        - Invalid email formats
        - Invalid phone numbers
        - Negative / zero salaries
        - Salary outliers (IQR)
    """
    issues = []

    #Normalize Column Names
    df.columns = df.columns.str.strip()

    # -----------------------------
    # 1. Invalid email formats
    # -----------------------------
    if "Email" in df.columns:
        invalid_emails = ~df["Email"].astype(str).str.match(EMAIL_REGEX)
        invalid_count = invalid_emails.sum()

    if invalid_count > 0:
        issues.append({
            "category": "INVALID_EMAIL_FORMAT",
            "column": "Email",
            "count": int(invalid_count),
            "severity": "MEDIUM",
            "message": "Invalid email format detected"
        })

    # -----------------------------
    # 2. Invalid phone numbers
    # -----------------------------
    if "Phone" in df.columns:
        cleaned_phone = (
            df["Phone"]
            .astype(str)
            .str.replace(r"\D", "", regex=True)
        )

        invalid_phones = cleaned_phone.str.len() < 10
        invalid_phone_count = invalid_phones.sum()

        if invalid_phone_count > 0:
            issues.append({
                "category": "INVALID_PHONE_NUMBER",
                "column": "Phone",
                "count": int(invalid_phone_count),
                "severity": "LOW",
                "message": "Phone numbers with fewer than 10 digits"
            })

    # -----------------------------
    # 3. Invalid salaries
    # -----------------------------
    if "Salary" in df.columns:
        salary = pd.to_numeric(df["Salary"], errors="coerce")

        negative_salary_count = (salary <= 0).sum()

        if negative_salary_count > 0:
            issues.append({
                "category": "INVALID_SALARY",
                "column": "Salary",
                "count": int(negative_salary_count),
                "severity": "HIGH",
                "message": "Salary must be greater than 0"
            })

    # -----------------------------
    # 4. Salary outliers (IQR)
    # -----------------------------
    q1 = salary.quantile(0.25)
    q3 = salary.quantile(0.75)
    iqr = q3- q1

    upper_bound = q3 + 1.5 * iqr
    lower_bound = q1 - 1.5 * iqr

    outliers = (salary < lower_bound) | (salary > upper_bound)
    outlier_count = outliers.sum()

    if outlier_count > 0:
        issues.append({
            "category": "SALARY_OUTLIER",
            "column": "Salary",
            "count": int(outlier_count),
            "severity": "MEDIUM",
            "message": "Salary outliers detected using IQR"
        })

    return issues

