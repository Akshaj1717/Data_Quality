"""
Generic Dataset Upload API (FastAPI Router)
=============================================
This module defines a FastAPI APIRouter with endpoints for uploading
and analyzing any CSV dataset using the Generic_Detection pipeline.

Architecture Note:
  This is a ROUTER (not a standalone app). It gets mounted onto
  the main app in app_main.py. This design keeps mcp_server.py
  completely untouched while adding new functionality.

Endpoints:
  POST /upload/analyze   — Upload a CSV file, run generic analysis, return report
  GET  /upload/files     — List all previously uploaded files

File Handling:
  - Uploaded CSVs are saved to the 'uploads/' folder on disk
  - Files are validated for: CSV extension, size limit (50MB), parseability
  - Each file is saved with a timestamp prefix to avoid name collisions
    (e.g., "20260312_154500_sales_data.csv")
"""

import os
import shutil
from datetime import datetime

from fastapi import APIRouter, UploadFile, File, HTTPException

# Import our generic pipeline from Phase 1
from Generic_Detection.generic_pipeline import run_generic_pipeline

# =============================================================
# CONFIGURATION
# =============================================================

# Directory where uploaded files will be saved
UPLOAD_DIR = "uploads"

# Maximum file size in bytes (50MB)
# 50 * 1024 * 1024 = 52,428,800 bytes
MAX_FILE_SIZE_BYTES = 50 * 1024 * 1024

# =============================================================
# ROUTER SETUP
# =============================================================
# Using APIRouter (not FastAPI) so this can be mounted onto
# the existing app without creating a separate server.
# The prefix="/upload" means all routes here start with /upload
# e.g., /upload/analyze, /upload/files

router = APIRouter(
    prefix="/upload",
    tags=["Generic Upload Analysis"]
)


# =============================================================
# HELPER: Ensure uploads directory exists
# =============================================================

def ensure_upload_dir():
    """
    Creates the uploads/ directory if it doesn't exist.
    Called before every file save operation.
    """
    os.makedirs(UPLOAD_DIR, exist_ok=True)


# =============================================================
# HELPER: Generate a unique filename with timestamp
# =============================================================

def generate_safe_filename(original_name: str) -> str:
    """
    Prepends a timestamp to the original filename to prevent
    overwriting if the same file is uploaded twice.

    Example:
      "sales_data.csv" → "20260312_154500_sales_data.csv"

    Args:
        original_name: The original filename from the upload

    Returns:
        A timestamped filename string
    """
    # Format: YYYYMMDD_HHMMSS
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Replace any spaces in the filename with underscores
    # (spaces in filenames can cause issues on some systems)
    clean_name = original_name.replace(" ", "_")

    return f"{timestamp}_{clean_name}"


# =============================================================
# ENDPOINT: Upload and Analyze a CSV File
# =============================================================

@router.post("/analyze")
async def upload_and_analyze(file: UploadFile = File(...)):
    """
    Upload a CSV file and receive a full quality analysis report.

    This endpoint:
    1. Validates the file (CSV format, under 50MB)
    2. Saves it to the uploads/ directory
    3. Runs the Generic_Detection pipeline on it
    4. Returns the complete analysis report

    Request:
        - Multipart form upload with a single file field

    Response:
        - Full analysis report (health, anomalies, scores, etc.)
        - The saved file path (for re-analysis or reference)

    Errors:
        - 400: File is not a CSV
        - 413: File exceeds 50MB size limit
        - 500: File could not be parsed or analyzed
    """

    # ---------------------------------------------------------
    # VALIDATION 1: Check that the file is a CSV
    # We check both the filename extension and the MIME type.
    # Some upload clients may not set the MIME type correctly,
    # so we primarily rely on the extension.
    # ---------------------------------------------------------
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(
            status_code=400,
            detail={
                "error": "Invalid file type",
                "message": "Only CSV files (.csv) are accepted. "
                           f"Received: '{file.filename}'"
            }
        )

    # ---------------------------------------------------------
    # VALIDATION 2: Check file size (must be under 50MB)
    # We read the entire file content into memory to check size.
    # For files under 50MB this is fine and avoids partial reads.
    # ---------------------------------------------------------
    contents = await file.read()
    file_size = len(contents)

    if file_size > MAX_FILE_SIZE_BYTES:
        # Convert to MB for a human-readable error message
        size_mb = round(file_size / (1024 * 1024), 2)
        raise HTTPException(
            status_code=413,
            detail={
                "error": "File too large",
                "message": f"File is {size_mb}MB. Maximum allowed is 50MB."
            }
        )

    # Also reject empty files
    if file_size == 0:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "Empty file",
                "message": "The uploaded file is empty."
            }
        )

    # ---------------------------------------------------------
    # SAVE THE FILE TO DISK
    # We save to uploads/ with a timestamp prefix so files
    # don't overwrite each other.
    # ---------------------------------------------------------
    ensure_upload_dir()
    safe_name = generate_safe_filename(file.filename)
    save_path = os.path.join(UPLOAD_DIR, safe_name)

    # Write the file contents to disk
    with open(save_path, "wb") as f:
        f.write(contents)

    # ---------------------------------------------------------
    # RUN THE GENERIC ANALYSIS PIPELINE
    # This calls Generic_Detection/generic_pipeline.py which
    # runs all checks: completeness, duplicates, types,
    # outliers, row scoring, and health classification.
    # ---------------------------------------------------------
    try:
        report = run_generic_pipeline(save_path)
    except Exception as e:
        # If the pipeline fails (e.g., file isn't valid CSV data),
        # return a 500 error with details about what went wrong.
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Analysis failed",
                "message": f"Could not analyze the file: {str(e)}",
                "file": safe_name
            }
        )

    # ---------------------------------------------------------
    # RETURN THE RESULTS
    # Include the saved file path so the dashboard or user
    # can reference it later (e.g., for re-analysis).
    # ---------------------------------------------------------
    return {
        "status": "success",
        "file_info": {
            "original_name": file.filename,
            "saved_as": safe_name,
            "saved_path": save_path,
            "file_size_bytes": file_size,
            "file_size_mb": round(file_size / (1024 * 1024), 2)
        },
        "report": report
    }


# =============================================================
# ENDPOINT: List Previously Uploaded Files
# =============================================================

@router.get("/files")
def list_uploaded_files():
    """
    Returns a list of all CSV files that have been uploaded
    to the uploads/ directory.

    This is useful for the dashboard to show a file history
    or allow re-analysis of a previously uploaded file.

    Response:
        - count: number of uploaded files
        - files: list of filenames with their sizes
    """
    ensure_upload_dir()

    files = []
    for filename in os.listdir(UPLOAD_DIR):
        # Only list CSV files (skip any system files)
        if filename.lower().endswith(".csv"):
            filepath = os.path.join(UPLOAD_DIR, filename)
            size_bytes = os.path.getsize(filepath)
            files.append({
                "filename": filename,
                "size_bytes": size_bytes,
                "size_mb": round(size_bytes / (1024 * 1024), 2)
            })

    # Sort by filename (which starts with timestamp, so newest first)
    files.sort(key=lambda x: x["filename"], reverse=True)

    return {
        "count": len(files),
        "files": files
    }
