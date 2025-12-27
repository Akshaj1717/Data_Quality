import pandas as pd
import requests

API_URL = "http://localhost:8000/analyze/health"

def fetch_quality_results(cs_path: str):
    response = requests.post(API_URL, json={"csv_path": "C:\Users\aksha\MCP_DATA_Quality\Messy_Employee_dataset_v2.csv"})
    response.raise_for_status()

    data = response.json()
    df = pd.read_csv(data["stored at"])

    return data["summary"], df