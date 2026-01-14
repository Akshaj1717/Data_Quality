## Overview
A modular Data Quality Analysis system built in Python with an MCP-style API.
Performs schema validation, anomaly detection, dataset-level and row-level quality scoring,
and structured issue reporting. Designed to simulate real-world data quality pipelines
with extensible monitoring and resolution capabilities.

## Dataset Health Output
Each analysis run generates a persistent quality table containing:

- Row-level quality scores (0–100)
- Usability classification (GOOD / WARNING / BAD)
- Dataset-level health summary
- Resolution decisions and confidence levels

This enables downstream filtering, reporting, governance, and auditability.

## Dataset Quality Dashboard
An interactive dashboard for visualizing row-level and dataset-level data quality.

**Features**
- Overall dataset health classification
- Row-level quality scoring
- Usability breakdown (GOOD / WARNING / BAD)
- Trend-aware inspection table
- Built with Streamlit and powered by a FastAPI backend

## Agentic Data Quality Resolution (MCP-Inspired)
This project implements an end-to-end data quality detection, monitoring,
and resolution framework inspired by Model Context Protocol (MCP) concepts.

The system treats data quality tools as modular, context-aware services capable of:
- Detecting issues
- Explaining decisions
- Triggering automated actions
- Escalating uncertain cases to human review

## Core System Capabilities
The pipeline operates across five integrated stages:

1. **Detection** – Schema validation, completeness, anomalies
2. **Scoring** – Row-level and dataset-level health metrics
3. **Monitoring** – Persistent metrics, trends, dashboards, and alerts
4. **Resolution** – Rule-based fixes, deduplication, quarantine
5. **Auditability** – Full traceability of automated and human decisions

## Human-in-the-Loop Review
High-risk or low-confidence records are automatically routed to a human review queue.
Human reviewers can approve, reject, or request fixes, with all decisions logged
for governance and audit purposes.

## MCP-Inspired Architecture
Key MCP-style concepts implemented include:

- Modular analysis tools exposed as APIs
- External validation services (e.g., SSN Validation API)
- Context-aware resolution logic
- Intelligent decision boundaries (auto-fix vs quarantine)
- Persistent audit logs and quality history

