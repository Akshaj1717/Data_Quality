import pandas as pd

def classify_dataset_health(df: pd.DataFrame):
    """
    Determines overall dataset health.
    """

    avg_score = df["Row_Quality_Score"].mean()
    bad_row_pct = (df["Row_Quality_Score"] < 70).mean() * 100

    if avg_score >= 85 and bad_row_pct <= 5:
        status = "GOOD"
    elif avg_score >= 70 and bad_row_pct <= 20:
        status = "DEGRADED"
    else:
        status = "FAIL"

    return {
        "dataset_health": status,
        "average_row_score": round(avg_score, 2),
        "bad_row_percentage": round(bad_row_pct, 2),
        "rows_analyzed": len(df)
    }
