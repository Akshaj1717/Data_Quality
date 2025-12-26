import pandas as pd


def calculate_row_quality_scores(df: pd.DataFrame):
    """
    Adds a Row_Quality_Score column (0–100) based on per-record issues.
    """

    scores = []

    # Precompute duplicates
    duplicate_ids = df["Employee_ID"].duplicated(keep=False)

    for idx, row in df.iterrows():
        score = 100

        # Missing required fields
        for col in ["Employee_ID", "First_Name", "Last_Name", "Email", "Department_Region"]:
            if pd.isna(row[col]) or str(row[col]).strip() == "":
                score -= 25

        # Invalid email
        if "@" not in str(row["Email"]):
            score -= 20

        # Invalid phone
        if "Phone" in df.columns:
            phone = str(row["Phone"])
            if phone and not phone.isdigit():
                score -= 15

        # Duplicate primary key
        if duplicate_ids.iloc[idx]:
            score -= 30

        scores.append(max(score, 0))

    df["Row_Quality_Score"] = scores
    return df
