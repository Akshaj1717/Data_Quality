### Overview
A modular Data Quality Analysis system built in Python with an MCP-style API. Performs schema validation, anomaly detection, dataset-level and row-level quality scoring, and structured issue reporting. Designed to simulate real-world data quality pipelines and extensible health monitoring.


### Dataset Health Output

The system generates a persistent quality table per analysis run, including:
- Row-level quality scores
- Usability classification
- Dataset health summary

This enables downstream filtering, reporting, and auditability.
