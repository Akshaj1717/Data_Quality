import json
from datetime import datetime
from pathlib import Path

#Location where audits will be stored
AUDIT_LOG_PATH = Path("outputs/audit_log.jsonl")

#Ensures that the directory exists
AUDIT_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

def log_event(
        action: str,
        source: str,
        reason: str,
        record_id: str | int | None = None,
        severity: str = "INFO",
        metadata: dict | None = None
):
    """
    Writes a single audit event to a JSON Lines file.
    """
    event = {
        "timestamp": datetime.utcnow().isoformat(),
        "action": action,
        "source": source,
        "reason": reason,
        "record_id": record_id,
        "severity": severity,
        "metadata": metadata or {}
    }

    with open(AUDIT_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(event) + "\n")