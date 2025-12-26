from fastapi import FastAPI
from pydantic import BaseModel
from Quality_Detection.Quality_Detection import run_employee_dq_pipeline

app = FastAPI(title="Employee Data Quality MCP", version="0.1")

class DatasetRequest(BaseModel):
    csv_path: str

@app.post("/analyze")
def analyze(req: DatasetRequest):
    """
    MCP Tool:
    Runs employee data quality checks on a CSV file
    """
    result = run_employee_dq_pipeline(req.csv_path)
    return{
        "tool": "analyze_employee_data",
        "status": "success",
        "result": result
    }

@app.get("/health")
def health():
    return {"status": "ok"}

#Running schema check
from Quality_Detection.schema_pipeline import run_schema_checks
class AnalyzeRequest(BaseModel):
    csv_path: str
@app.post("/analyze/schema")
def analyze_schema(req: AnalyzeRequest):
    return run_schema_checks(req.csv_path)

from Quality_Detection.anomaly_pipeline import run_anomaly_checks
@app.post("/analyze/anomalies")
def analyze_anomalies(req: AnalyzeRequest):
    return run_anomaly_checks(req.csv_path)

from Quality_Detection.full_pipeline import run_full_analysis
@app.post("/analyze/full")
def analyze_full(req: AnalyzeRequest):
    return run_full_analysis(req.csv_path)

from Quality_Detection.row_scoring import calculate_row_quality_scores
from Quality_Detection.Quality_Detection import load_data
@app.post("/analyze/score")
def analyze_score(req: AnalyzeRequest):
    # Load dataset
    df = load_data(req.csv_path)

    # Apply row-level scoring
    df = calculate_row_quality_scores(df)

    return {
        "tool": "score",
        "rows_analyzed": len(df),
        "average_score": round(df["Row_Quality_Score"].mean(), 2),
        "min_score": int(df["Row_Quality_Score"].min()),
        "max_score": int(df["Row_Quality_Score"].max()),
        "preview": df[
            ["Employee_ID", "Row_Quality_Score"]
        ].head(10).to_dict(orient="records")
    }

    return run_full_analysis(req.csv_path)



