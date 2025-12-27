import requests
import pandas as pd

API_URL = "http://127.0.0.1:8000/analyze/health"

def fetch_quality_results(csv_path: str):
    response = requests.post(
        API_URL,
        json={"csv_path": csv_path}
    )
    response.raise_for_status()

    data = response.json()

    # Summary comes directly from API
    summary = data["summary"]

    # Preview rows → DataFrame
    df = pd.DataFrame(data["preview"])

    return summary, df
