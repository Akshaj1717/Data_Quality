import requests

SSN_API_URL = "http://localhost:8001/validate-ssn"

def validate_ssn_via_mcp(ssn: str) -> bool:
    """
    Calls external MCP SSN Validation Server.
    """
    if not ssn or str(ssn).strip() == "":
        return False
    try:
        response = requests.post(
            SSN_API_URL,
            json={"ssn": ssn},
            timeout=2
        )
        response.raise_for_status()
        return response.json()["valid"]
    except Exception:
        # Fail-safe: treat SSN as invalid if service fails
        return False
