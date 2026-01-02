import pandas as pd

def select_best_record(group: pd.DataFrame) -> pd.Series:
    """
    Selects the best record from a duplicate group.
    """

    #Highest quality score wins
    best = group.sort_values(
        by=["Row_Quality_Score"],
        ascending=False
    )

    #If tie, most complete row
    best["non_null_count"] = best.notna().sum(axis=1)
    best = best.sort_values(
        by=["Row_Quality_Score", "non_null_count"],
        ascending=False
    )