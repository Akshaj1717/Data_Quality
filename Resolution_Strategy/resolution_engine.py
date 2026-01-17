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

        df = deduplicate_by_employee_id(df)[0]

        resolved_rows = []

        df = df.copy()
        df["Resolution_Action"] = None

        for _, row in df.iterrows():
            decision = self._decide_action(row)

            if decision == "ACCEPT":
                row["Resolution_Action"] = "ACCEPT"
                resolved_rows.append(row.to_dict())

            elif decision == "STANDARDIZE":
                standardized = apply_standardization(row)
                standardized["Resolution_Action"] = "STANDARDIZE"
                resolved_rows.append(standardized.to_dict())

            elif decision == "QUARANTINE":
                quarantined = quarantine_rows(row, reason="Row failed quality thresholds")
                quarantined["Resolution_Action"] = "QUARANTINE"
                quarantined_rows.append(quarantined.to_dict())

        cleaned_df = pd.DataFrame(resolved_rows)
        print(cleaned_df.columns.tolist())
        print(cleaned_df["Resolution_Action"].value_counts())
        quarantined_df = pd.DataFrame(quarantined_rows)

        return cleaned_df, quarantined_df

    def _decide_action(self, row: pd.Series) -> str:
        """
        Determines what should happen to a single row.
        """

        score = row["Row_Quality_Score"]
        status = row["Row_Usability_Status"]

        # Bad rows always quarantined
        if status == "BAD":
            return "QUARANTINE"

        # Warning rows may be standardized
        if status == "WARNING":
            if score >= self.rules["STANDARDIZE_MIN_SCORE"]:
                return "STANDARDIZE"
            return "QUARANTINE"

        # Good rows are accepted
        if status == "GOOD":
            return "ACCEPT"

        return "QUARANTINE"

