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

    #If still tie, earliest hire date
    if "HireDate" in best.columns:
        best = best.sort_values(
            by="HireDate",
            ascending=True,
            na_position="last"
        )

    return best.iloc

def deduplicate_by_employee_id(df: pd.DataFrame):
    """
    Deduplicates records with the same Employee_ID.
    """

    deduped_rows = []
    removed_rows = []

    for emp_id, group in df.grouby("Employee_ID"):
        if len(group):
            deduped_rows.append(group.iloc[0])
        else:
            winner = select_best_record(group)
            deduped_rows.append(winner)

            losers = group.drop(winner.name)
            removed_rows.append(losers)

    deduped_df = pd.DataFrame(deduped_rows)
    removed_df = pd.concat(removed_rows) if removed_rows else pd.DataFrame

    return deduped_df, removed_df