import pandas as pd
from pathlib import Path

HISTORY_FILE = Path("outputs/quality_hhistory.csv")

def load_history() -> pd.DataFrame:
    """
    Loads historical quality metrics.
    """
    if not HISTORY_FILE.exists():
        return pd.DataFrame

    return pd.read_csv(HISTORY_FILE)

def compute_trends(window: int = 5) -> dict:
    """
    Computes trends over the last N runs.
    """

    df = load_history()

    if len(df) < 2:
        return {"status": "insufficient_data"}

    recent = df.tail(window)

    trends = {
        "avg_score_trend": recent["average_score"].iloc[-1] - recent["average_score"].iloc[0],
        "bad_rows_trend": recent["bad_rows"].iloc[-1] - recent["bad_rows"].iloc[0],
        "warning_rows_trend": recent["warning_rows"].iloc[-1] - recent["warning_rows"].iloc[0],
    }

    trends["direction"] = (
        "DEGRADING" if trends["bad_rows_trend"] > 0 else "IMPROVING"
    )

    return trends