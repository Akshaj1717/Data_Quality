import pandas as pd

def resolve_row(row: pd.Series) -> dict:
    """
    Determines the resolution action for a single row.
    Returns action, reason, and confidence score.
    """
    score = row["Row_Quality_Score"]

    # -----------------------------
    # Hard-fail conditions
    # -----------------------------
    if row.get("SSN_Valid") is False:
        return{
            "Resolution_Action": "QUARANTINE",
            "Resolution_Reason": "Invalid SSN",
            "Resolution_Confidence": 0.95
        }

    if score < 70:
        return{
            "Resolution_Action": "QUARANTINE",
            "Resolution_Reason": "Low data quality score",
            "Resolution_Confidence": 0.9
        }

    # -----------------------------
    # Deduplication required
    # -----------------------------
    if row.get("Is Duplicate") is True:
        return{
            "Resolution_Action": "DEDUPE",
            "Resolution_Reason": "Duplicate Employee_ID",
            "Resolution_Confidence": 0.85
        }

    # -----------------------------
    # Warning-level acceptance
    # -----------------------------
    if score < 85:
        return{
            "Resolution_Action": "ACCEPT_WITH_WARNING",
            "Resolution_Reason": "Minor data quality issues",
            "Resolution_Confidence": 0.7
        }

    # -----------------------------
    # Clean record
    # -----------------------------
    return{
        "Resolution_Action": "ACCEPT",
        "Resolution_Reason": "Meets all quality thresholds",
        "Resolution_Confidence": 0.99
    }

def apply_resolution_engine(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies resolution logic to the entire dataset.
    """
    resolutions = df.apply(resolve_row, axis=1, result_type="expand")

    df["Resolution_Action"] = resolutions["Resolution_Action"]
    df["Resolution_Reason"] = resolutions["Resolution_Reason"]
    df["Resolution_Confidence"] = resolutions["Resolution_Confidence"]

    return df