import pandas as pd
from .schema_checks import check_schema


def run_schema_checks(csv_path: str):
    """
    Loads data and runs schema checks.
    """

    df = pd.read_csv(csv_path)

    issues = check_schema(df)

    status = "PASS" if len(issues) == 0 else "FAIL"

    return {
        "tool": "schema",
        "status": status,
        "issue_count": len(issues),
        "issues": issues
    }
