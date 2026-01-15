import pandas as pd
from Resolution_Strategy.rules import RESOLUTION_RULES
from Resolution_Strategy.standardization import apply_standardization
from Resolution_Strategy.deduplication import deduplicate_by_employee_id
from Resolution_Strategy.quarantine import quarantine_rows
from Audit.audit_log import log_event

class ResolutionEngine:
    """
    Orchestrates the resolution phase of the data quality pipeline.

    This engine decides how each row should be handled based on:
    - Row quality score
    - Usability status
    - Resolution rules
    """

    def __init__(self, rules: RESOLUTION_RULES):
        """
        Rules:
            - Centralized configuration defining thresholds and behaviors
        """
        self.rules = rules

    def resolve(self, df: pd.DataFrame):
        """
        Main entry point for resolving a dataset.

        Returns:
            - cleaned_df: rows safe to use
            - quarantined_df: rows isolated for manual review
        """

        quarantined_rows = []

        df = deduplicate_by_employee_id(df)

        resolved_rows = []

        for _, row in df.iterrows():
            decision = self._decide_action(row)

            if decision == "ACCEPT":
                log_event(
                    row["Employee_ID"],
                    action="ACCEPT",
                    reason="Row passed quality thresholds",
                    severity="INFO",
                )
                resolved_rows.append(row)
            elif decision == "STANDARDIZE":
                standardized = apply_standardization(row)

                log_event(
                    row["Employee_ID"],
                    action="STANDARDIZE",
                    reason="Minor quality issues auto-corrected",
                    severity="LOW",
                )
                resolved_rows.append(row)
            elif decision == "QUARANTINE":
                quarantined = quarantine_rows(row)

                log_event(
                    row["Employee_ID"],
                    action="QUARANTINE",
                    reason="Row failed quality thresholds",
                    severity="HIGH",
                )
                quarantined_rows.append(quarantined)

        cleaned_df = pd.DataFrame(resolved_rows)
        quarantined_df = pd.DataFrame(quarantined_rows)

        return cleaned_df, quarantined_df

    def _decide_action(self, row: pd.Series) -> str:
        """
        Determines what should happen to a single row.
        """

        score = row["Row_Quality_Score"]
        status = row["Row_Usability_Status"]

        if status == "BAD":
            return "QUARANTINE"

        if status == "WARNING":
            if score >= self.rules.RESOLUTION_RULES:
                return "STANDARDIZE"
            return "QUARANTINE"

        if status == "GOOD":
            return "ACCEPT"

        return "QUARANTINE"
