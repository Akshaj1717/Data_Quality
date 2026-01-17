def evaluate_alerts(metrics: dict) -> list:
    """
    Evaluates metrics against thresholds and returns alert messages.
    """

    alerts = []

    if metrics["quarantine_rate"] > 0.20:
        alerts.append({
            "level": "HIGH",
            "message": "Quarantine rate exceeded 20%"
        })

    if metrics["standardize_rate"] > 0.30:
        alerts.append({
            "level": "MEDIUM",
            "message": "High standardization rate indicates upstream issues"
        })

    if metrics["accept_rate"] < 0.60:
        alerts.append({
            "level": "HIGH",
            "message": "Low accept rate - data quality degrading"
        })

    return alerts

