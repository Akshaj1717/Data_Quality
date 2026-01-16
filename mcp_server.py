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
from Monitoring.history import log_run_metrics
from Monitoring.trends import compute_trends
from Monitoring.sla import evaluate_sla
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

    metrics = {
        "total_rows": len(df),
        "usable_rows": int((df["Row_Usability_Status"] == "GOOD").sum()),
        "warning_rows": int((df["Row_Usability_Status"] == "WARNING").sum()),
        "bad_rows": int((df["Row_Usability_Status"] == "BAD").sum()),
        "average_score": round(df["Row_Quality_Score"].mean(), 2),
    }

    # Persist run history
    log_run_metrics(metrics)

    # Compute trends across runs
    trends = compute_trends()

    # Evaluate SLA
    sla = evaluate_sla(metrics)

    return {
        "tool": "health",
        "summary": health,
        "row_counts": {
            "usable_rows": int((df["Row_Usability_Status"] == "GOOD").sum()),
            "warning_rows": int((df["Row_Usability_Status"] == "WARNING").sum()),
            "bad_rows": int((df["Row_Usability_Status"] == "BAD").sum()),
            "metrics": metrics,
            "trends": trends,
            "sla": sla,
        },
        "stored_at": output_path,
        "preview": df[
            ["Employee_ID", "Row_Quality_Score", "Row_Usability_Status"]
        ].head(10).to_dict(orient="records")
    }

from Monitoring.metrics import compute_resolution_engine
from Monitoring.alerts import evaluate_alerts
from Monitoring.history import log_run_metrics
from Resolution_Strategy.resolution_engine import ResolutionEngine
from Resolution_Strategy.rules import RESOLUTION_RULES
@app.post("/monitor/run")
def monitor_dataset(req: AnalyzeRequest):
    """
    Runs resolution + monitoring on a dataset
    """

    print("STEP 1: loading data")
    df = load_data(req.csv_path).head(1000)

    print("STEP 2: scoring rows")
    df = calculate_row_quality_scores(df)

    print("STEP 3: resolving")
    engine = ResolutionEngine(RESOLUTION_RULES)
    cleaned_df, quarantined_df = engine.resolve(df)

    print("STEP 4: computing metrics")
    metrics = compute_resolution_engine(cleaned_df)
    alerts = evaluate_alerts(metrics)
    log_run_metrics(metrics)

    print("done")
    return {
        "tool": "monitor",
        "metrics": metrics,
        "alerts": alerts,
        "cleaned_rows": len(cleaned_df),
        "quarantined_rows": len(quarantined_df)
    }

import pandas as pd
from Human_Review.review_queue import build_review_queue
from Human_Review.review_decisions import apply_review_decision
from Human_Review.review_models import ReviewDecision
DATA_PATH = "outputs/quality_results.csv"
@app.post("/review/queue")
def get_review_queue(req: AnalyzeRequest):
    """
    Returns all rows that need human review.
    """
    df = pd.read_csv(DATA_PATH)
    review_queue = build_review_queue(df)
    return{
        "count": len(review_queue),
        "items": review_queue.to_dict(orient="records")
    }

@app.post("/review/decision")
def submit_review_decision(decision: ReviewDecision):
    """
    Receives a decision from a human reviewer.
    """
    df = pd.read_csv(DATA_PATH)
    df = apply_review_decision(
        df,
        employee_id=decision.employee_id,
        decision=decision.decision,
        notes=decision.reviewer_notes
    )

    df.to_csv(DATA_PATH, index=False)

    return {
        "status": "success",
        "employee_id": decision.employee_id,
        "decision": decision.decision
    }






