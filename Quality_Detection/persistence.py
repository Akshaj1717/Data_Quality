import pandas as pd
from datetime import datetime
import os

OUTPUT_PATH = "outputs/quality_results.csv"

def persist_quality_results(df: pd.DataFrame):
    os.makedirs("outputs", exist_ok=True)

    df = df.copy()
    df["Analysis_Timestamp"] = datetime.utcnow()

    columns_to_save = [
        "Employee_ID",
        "Row_Quality_Score",
        "Row_Usability_Status",
        "Analysis_Timestamp"
    ]

    df[columns_to_save].to_csv(
        OUTPUT_PATH,
        mode='w',
        index=False
    )

    return OUTPUT_PATH