import pandas as pd
from datetime import datetime

def quarantine_rows(
    df: pd.DataFrame,
    reason: str,
    source: str = "resolution_engine"
) ->  pd.DataFrame:
    """
    Moves problematic rows into quarantine with metadata.
    """

    quarantine_df = df.copy()

    quarantine_df["Quarantine_Reason"] = reason
    quarantine_df["Quarantine_Source"] = source
    quarantine_df["Quarantine_Timestamp"] = datetime.utcnow().isoformat()

    return quarantine_df