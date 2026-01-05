import pandas as pd
from datetime import datetime
from audit_log import log_event

#These are the helper functions

def standardize_email(email: str) -> str:
    return email.strip().lower()

def standardize_phone(phone: str) -> str:
    digits = "".join(filter(str.isdigit, phone))
    if len(digits) == 10:
        return digits
    return phone

def standardize_department(dept: str) -> str:
    return dept.strip().title()

def standardize_date(value):
    try:
        return pd.to_datetime(value).date().isoformat()
    except Exception:
        return value

#This is the main engine that will apply the changes

def apply_standardization(df: pd.DateFrame) -> pd.DataFrame:
    """
    Applies safe, rule based standardization.
    All changes are logged to the audit log.
    """

    df = df.copy()

    for idx, row in df.iterrows():
        record_id = row.get("Employee_ID", f"row_{idx}")


        # Email Standardization
        if "Email" in df.columns and pd.notna(row["Email"]):
            new_email = standardize_email(row["Email"])
            if new_email != row["Email"]:
                log_event(
                    action="STANDARDIZE_EMAIL",
                    source="standardization",
                    reason="Normalized email casing/spacing",
                    record_id=record_id,
                    severity="INFO",
                    metadata={"before": row["Email"], "after": new_email}
                )
                df.at[idx, "Email"] = new_email

        # Phone Number Standardization
        if "Phone" in df.columns and pd.notna(row["Phone"]):
            new_phone = standardize_phone(str(row["Phone"]))

            if new_phone != row["Phone"]:
                log_event(
                    action="STANDARDIZE_PHONE",
                    source="standardization",
                    reason="Removed non-numeric characters from phone number",
                    record_id=record_id,
                    severity="INFO",
                    metadata={"before": row["Phone"], "after": new_phone}
                )
                df.at[idx, "Phone"] = new_phone


