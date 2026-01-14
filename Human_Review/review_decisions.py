import pandas as pd
from datetime import datetime

def apply_review_decision(
        df: pd.DataFrame,
        employee_id: str,
        decision: str,
        notes: str | None = None
) -> pd.DataFrame:
    """
    Applies a human decision to ONE row in the dataset.
    Parameters:
        df → full dataset
        employee_id → which row to update
        decision → APPROVE, REJECT, FIX
        notes → optional explanation from human
    Returns:
        Updated DataFrame
    """

    mask = df["Employee_ID"] == employee_id

    #Human Approval
    if decision == "APPROVE":
        df.loc[mask, "Resolution_Action"] = "ACCEPT"
        df.loc[mask, "Human_Reviewed"] = True
    #Human Rejection
    elif decision == "REJECT":
        df.loc[mask, "Resolution_Action"] = "REJECTED"
        df.loc[mask, "Human_Reviewed"] = True
    #Human says row needs fixing
    elif decision == "FIX":
        df.loc[mask, "Resolution_Action"] = "NEEDS_FIX"
        df.loc[mask, "Human_Reviewed"] = True

    df.loc[mask, "Review_Notes"] = notes
    df.loc[mask, "Review_Timestamp"] = datetime.utcnow().isoformat()

    return df