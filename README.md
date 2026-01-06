### Overview
A modular Data Quality Analysis system built in Python with an MCP-style API. Performs schema validation, anomaly detection, dataset-level and row-level quality scoring, and structured issue reporting. Designed to simulate real-world data quality pipelines and extensible health monitoring.


### Dataset Health Output

The system generates a persistent quality table per analysis run, including:
- Row-level quality scores
- Usability classification
- Dataset health summary

This enables downstream filtering, reporting, and auditability.

## Dataset Quality Dashboard

Interactive dashboard for visualizing row-level and dataset-level data quality.

  ### Features
  - Overall dataset health classification
  - Row-level quality scoring (0–100)
  - Usability breakdown (GOOD / WARNING / BAD)
  - Scrollable inspection table
  
  Built with Streamlit and powered by a FastAPI backend.

# MCP Data Quality & Resolution System

This project implements an end-to-end **data quality monitoring and resolution framework**, inspired by Model Context Protocol (MCP) concepts.  
It is designed to detect, explain, monitor, and resolve data quality issues in structured datasets.

---

## What This System Does

The system performs **automated data quality management** across five core stages:

1. **Detection** – Identify data quality issues
2. **Scoring** – Assign health scores to rows and datasets
3. **Monitoring** – Visualize quality trends via a dashboard
4. **Resolution** – Fix or isolate bad data using rules
5. **Auditability** – Log every automated decision

---

## MCP-Inspired Architecture

This project treats data quality tools as **modular, context-aware services**, similar to MCP components:

- External validation services (e.g., SSN Validation API)
- Rule-based resolution engines
- Intelligent decision boundaries (auto-fix vs quarantine)
- Full traceability through audit logs

---

