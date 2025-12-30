# mcp_server.py
import re
from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn

# -----------------------------
# SSN VALIDATION LOGIC
# -----------------------------
SSN_REGEX = re.compile(r"^(?!000|666|9\d\d)(\d{3})[- ]?(?!00)(\d{2})[- ]?(?!0000)(\d{4})$")

def is_valid_ssn(ssn: str) -> bool:
    """
    Validates a U.S. Social Security Number (SSN).
    Rules enforced:
    - Must be 9 digits (with or without dashes)
    - Area number cannot be 000, 666, or 900–999
    - Group number cannot be 00
    - Serial number cannot be 0000
    """
    return bool(SSN_REGEX.match(ssn))


# -----------------------------
# FASTAPI REST API
# -----------------------------
app = FastAPI(title="MCP Server with SSN Validation API")

class SSNRequest(BaseModel):
    ssn: str

class SSNResponse(BaseModel):
    ssn: str
    valid: bool


@app.post("/validate-ssn", response_model=SSNResponse)
async def validate_ssn(request: SSNRequest):
    valid = is_valid_ssn(request.ssn)
    return SSNResponse(ssn=request.ssn, valid=valid)


# -----------------------------
# MCP SERVER (MINIMAL EXAMPLE)
# -----------------------------
# This is a placeholder MCP server structure.
# You can expand it with tools, prompts, resources, etc.

class MCPServer:
    def __init__(self):
        print("MCP Server initialized")

    def start(self):
        print("MCP Server running (placeholder)")


# -----------------------------
# ENTRY POINT
# -----------------------------
if __name__ == "__main__":
    # Start REST API
    print("Starting REST API on http://localhost:8001 ...")
    uvicorn.run(app, host="localhost", port=8001)

    # Start MCP server (if needed)
    # mcp = MCPServer()
    # mcp.start()
