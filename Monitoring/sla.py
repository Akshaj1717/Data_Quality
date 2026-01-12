def evaluate_sla(current_metrics: dict) -> dict:
    """
    Evaluates dataset metrics against SLA thresholds.
    """

    sla = {
        "max_bad_rows_pct": 0.05,
        "min_avg_score": 85,
    }

    total_rows = current_metrics["total_rows"]
    bad_pct = current_metrics["bad_rows"] / total_rows

    violations = []

    if bad_pct > sla["max_bad_rows_pct"]:
        violations.append("BAD_ROW+PERCENTAGE_EXCEEDED")

    if current_metrics ["average_score"] < sla["min_avg_score"]:
        violations.append("AVERAGE_SCORE_TOO_LOW")

    return {
        "sla_status": "BREACHED" if violations else "OK",
        "violations": violations,
    }