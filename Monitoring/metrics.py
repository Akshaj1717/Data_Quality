import pandas as pd

def compute_resolution_engine(df: pd.DataFrame) -> dict:
    """
    Computes high level metrics for monitoring data quality outcomes.
    """

    total = len(df)

    if total == 0 or "Resolution_Action" not in df.columns:
        return {
            "total_rows": total,
            "accepted": 0,
            "standardized": 0,
            "quarantined": 0,
            "accept_rate": 0.0,
            "standardize_rate": 0.0,
            "quarantine_rate": 0.0,
        }

    accepted = int((df["Resolution_Action"] == "ACCEPT").sum())
    standardized = int((df["Resolution_Action"] == "STANDARDIZE").sum())
    quarantined = int((df["Resolution_Action"] == "QUARANTINE").sum())

    metrics = {
        "total_rows": total,
        "accepted": accepted,
        "standardized": standardized,
        "quarantined": quarantined,
        # Rates are guarded against division-by-zero
        "accept_rate": round(accepted / total, 3) if total > 0 else 0.0,
        "standardize_rate": round(standardized / total, 3) if total > 0 else 0.0,
        "quarantine_rate": round(quarantined / total, 3) if total > 0 else 0.0,
    }

    metrics["accept_rate"] = round(metrics["accepted"] / total, 3)
    metrics["standardize_rate"] = round(metrics["standardized"] / total, 3)
    metrics["quarantine_rate"] = round(metrics["quarantined"] / total, 3)

    return metrics