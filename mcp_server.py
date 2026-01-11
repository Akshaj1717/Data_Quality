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

from Quality_Detection.row_scoring import calculate_row_quality_scores
from Quality_Detection.Quality_Detection import load_data
from Quality_Detection.health import classify_dataset_health
from Quality_Detection.persistence import persist_quality_results
@app.post("/analyze/health")
def analyze_health(req: AnalyzeRequest):
    """
    Runs dataset health analysis:
    - Loads dataset
    - Computes row-level quality scores
    - Classifies row usability
    - Computes overall dataset health
    - Persists results to a quality table
    """

    df = load_data(req.csv_path)

    df = calculate_row_quality_scores(df)

    health = classify_dataset_health(df)

    output_path = persist_quality_results(df)

    return {
        "tool": "health",
        "summary": health,
        "row_counts": {
            "usable_rows": int((df["Row_Usability_Status"] == "GOOD").sum()),
            "warning_rows": int((df["Row_Usability_Status"] == "WARNING").sum()),
            "bad_rows": int((df["Row_Usability_Status"] == "BAD").sum())
        },
        "stored_at": output_path,
        "preview": df[
            ["Employee_ID", "Row_Quality_Score", "Row_Usability_Status"]
        ].head(10).to_dict(orient="records")
    }

from Monitoring.metrics import computer_resolution_engine
from Monitoring.alerts import evaluate_alerts
from Monitoring.history import log_run_metrics
from Resolution_Strategy.resolution_engine import ResolutionEngine
from Resolution_Strategy.rules import RESOLUTION_RULES
@app.post("/monitor/run")
def monitor_dataset(req: AnalyzeRequest):
    """
    Runs resolution + monitoring on a dataset
    """

    df = load_data(req.csv_path)
    df = calculate_row_quality_scores(df)

    engine = ResolutionEngine(RESOLUTION_RULES)
    cleaned_df, quarantined_df = engine.resolve(df)

    metrics = computer_resolution_engine(cleaned_df)
    alerts = evaluate_alerts(metrics)
    log_run_metrics(metrics)

    return {
        "tool": "monitor",
        "metrics": metrics,
        "alerts": alerts,
        "cleaned_rows": len(cleaned_df),
        "quarantined_rows": len(quarantined_df)
    }






