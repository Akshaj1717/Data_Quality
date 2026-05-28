import pandas as pd

def build_review_queue(df: pd.DataFrame) -> pd.DataFrame:
    """
    This function selects rows that REQUIRE human review.
    Input:
            df → full dataset after resolution engine
    Output:
            DataFrame containing only rows needing review
    """

    review_df = df[
        (df["Resolution_Action"] == "QUARANTINE") |
        (df["Resolution_Confidence"] < 0.8)
    ].copy()

    if "Human_Reviewed" in review_df.columns:
        review_df = review_df[
            (review_df["Human_Reviewed"] != True) &
            (review_df["Human_Reviewed"] != "True") &
            (review_df["Human_Reviewed"] != 1)
        ]

    review_df = review_df[
        [
            "Employee_ID",
            "Row_Quality_Score",
            "Resolution_Action",
            "Resolution_Reason",
            "Resolution_Confidence"
        ]
    ]

    review_df.rename(
        columns={
            "Employee_ID": "employee_id",
            "Row_Quality_Score": "current_score",
            "Resolution_Reason": "issue_reason"
        },
        inplace=True
    )

    return review_df
