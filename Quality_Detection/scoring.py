SEVERITY_WEIGHTS = {
    "CRITICAL": 30,
    "HIGH": 20,
    "MEDIUM": 10,
    "LOW": 5
}

def calculate_quality_score(schema_issues, anomaly_issues):
    """
    Calculates a quality score from 0–100 based on issue severity.
    """

    score = 100
    all_issues = schema_issues + anomaly_issues

    for issue in all_issues:
        weight = SEVERITY_WEIGHTS.get(issue["severity"], 0)
        score -= weight

    return max(score, 0)