# Quality_Detection/resolution/rules.py

RESOLUTION_RULES = {
    "INVALID_SSN": {
        "severity": "CRITICAL",
        "action": "QUARANTINE"
    },
    "DUPLICATE_EMPLOYEE_ID": {
        "severity": "CRITICAL",
        "action": "QUARANTINE"
    },
    "MISSING_AGE": {
        "severity": "WARNING",
        "action": "ACCEPT_WITH_WARNING"
    },
    "MISSING_HIRE_DATE": {
        "severity": "WARNING",
        "action": "ACCEPT_WITH_WARNING"
    },
    "LOW_SCORE": {
        "severity": "CRITICAL",
        "action": "QUARANTINE"
    }
}

