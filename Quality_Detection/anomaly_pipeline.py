import pandas as pd
from .anomaly_checks import check_anomalies


def run_anomaly_checks(csv_path: str):
    """
    Loads data and runs anomaly checks.
    """

    df = pd.read_csv(csv_path)

    issues = check_anomalies(df)

    status = "PASS" if len(issues) == 0 else "FAIL"

    return {
        "tool": "anomalies",
        "status": status,
        "issue_count": len(issues),
        "issues": issues
    }


