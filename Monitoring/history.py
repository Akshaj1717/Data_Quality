import pandas as pd
from datetime import datetime
from pathlib import Path
HISTORY_FILE = Path("outputs/quality_hhistory.csv")

def log_run_metrics(metrics: dict):
    """
    Appends current run metrics to history file.
    """

    record = metrics.copy()
    record["timestamp"] = datetime.utcnow().isoformat()

    df = pd.DataFrame([record])

    if HISTORY_FILE.exists():
        df.to_csv(HISTORY_FILE, mode="a", header=False, index=False)
    else:
        df.to_csv(HISTORY_FILE, index=False)