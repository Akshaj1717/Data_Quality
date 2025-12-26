from .schema_pipeline import run_schema_checks
from .anomaly_pipeline import run_anomaly_checks
from .scoring import calculate_quality_score

def run_full_analysis(csv_path: str):
    schema_result  = run_schema_checks(csv_path)

    #stop if schema fails
    if schema_result["status"] == "FAIl":
        return{
            "tool": "full",
            "status": "FAIL",
            "reason": "Schema validation failed",
            "schema": schema_result,
            "anomalies": None,
            "quality_score": 0
        }

    anomaly_result = run_anomaly_checks(csv_path)

    score = calculate_quality_score(
        schema_result["issues"],
        anomaly_result["issues"]
    )

    return {
        "tool": "full",
        "status": "PASS" if score >= 70 else "WARN",
        "schema": schema_result,
        "anomalies": anomaly_result,
        "quality_score": score
    }