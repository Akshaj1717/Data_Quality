"""
Main Application Entry Point
==============================
This is the COMBINED entry point that serves BOTH:
  1. All existing endpoints from mcp_server.py (employee-specific analysis)
  2. The new generic upload endpoints from generic_api.py

How to run:
  uvicorn app_main:app --reload --port 8000

This replaces running "uvicorn mcp_server:app" — you get everything
the old server had PLUS the new upload functionality.

Architecture Note:
  - mcp_server.py is NOT modified — it stays exactly as-is
  - We import its FastAPI app and add our new router to it
  - The existing endpoints (/analyze, /analyze/health, etc.) still work
  - New endpoints (/upload/analyze, /upload/files) are added on top
"""

# Import the existing FastAPI app from mcp_server.py
# This gives us all the employee-specific endpoints that already exist
from mcp_server import app

# Import the new generic upload router from generic_api.py
from generic_api import router as generic_router

# =============================================================
# MOUNT THE NEW ROUTER ONTO THE EXISTING APP
# =============================================================
# include_router() adds all routes from the generic_router
# to the existing app. The routes will be available at their
# defined prefix (/upload/analyze, /upload/files).
#
# This is the key design pattern that lets us extend the API
# without touching mcp_server.py at all.

app.include_router(generic_router)


# =============================================================
# SUMMARY OF ALL AVAILABLE ENDPOINTS
# =============================================================
# Existing (from mcp_server.py — unchanged):
#   POST /analyze            — Employee DQ pipeline
#   GET  /health             — Health check
#   POST /analyze/schema     — Schema validation
#   POST /analyze/anomalies  — Anomaly detection
#   POST /analyze/full       — Full analysis
#   POST /analyze/score      — Row scoring
#   POST /analyze/health     — Health classification
#   POST /monitor/run        — Resolution + monitoring
#   POST /review/queue       — Human review queue
#   POST /review/decision    — Submit review decision
#
# New (from generic_api.py):
#   POST /upload/analyze     — Upload any CSV + get quality report
#   GET  /upload/files       — List previously uploaded files
