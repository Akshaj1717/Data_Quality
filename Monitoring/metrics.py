import pandas as pd

def compute_resolution_engine(df: pd.DataFrame) -> dict:
    """
    Computes high level metrics for monitoring data quality outcomes.
    """

    total = len(df)

    metrics = {
        "total_rows": total,
        "accepted": int((df["Resolution_Action" == "ACCEPT"]).sum()),
        "standardized": int((df["Resolution_Action"] == "STANDARDIZE").sum()),
        "quarantined": int ((df["Resolution_Action"] == "QUARANTINE").sum())
    }

    metrics["accept_rate"] = round(metrics["accepted"] / total, 3)
    metrics["standardize_rate"] = round(metrics["standardized"] / total, 3)
    metrics["quarantine_rate"] = round(metrics["quarantined"] / total, 3)

    return metrics