import pandas as pd


def calculate_row_quality_scores(df: pd.DataFrame):
    """
    Adds Row_Quality_Score and Row_Usability_Status columns.
    """

    scores = []

    duplicate_ids = df["Employee_ID"].duplicated(keep=False)

    for idx, row in df.iterrows():
        score = 100

        # Missing required fields
        for col in ["Employee_ID", "First_Name", "Last_Name", "Email", "Department_Region"]:
            if pd.isna(row[col]) or str(row[col]).strip() == "":
                score -= 25

        # Invalid email
        if not pd.isna(row["Email"]) and "@" not in str(row["Email"]):
            score -= 20

        #Important Fields
        if "Age" in df.columns:
            if pd.isna(row["Age"]):
                score -= 10

        if "Join_Date" in df.columns:
            if pd.isna(row["Join_Date"]):
                score -= 10

        # Invalid phone
        if "Phone" in df.columns:
            phone = str(row["Phone"])
            if phone and not phone.isdigit():
                score -= 15
        #Invalid Salary
        if "Salary" in df.columns:
            if pd.isna(row["Salary"]):
                score -= 5

        # Duplicate primary key
        if duplicate_ids.iloc[idx]:
            score -= 30

        scores.append(max(score, 0))

    df["Row_Quality_Score"] = scores

    # -----------------------------
    # Usability classification
    # -----------------------------
    def classify(score):
        if score >= 85:
            return "GOOD"
        elif score >= 70:
            return "WARNING"
        return "BAD"

    df["Row_Usability_Status"] = df["Row_Quality_Score"].apply(classify)

    return df
