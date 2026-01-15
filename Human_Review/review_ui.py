import streamlit as st
import pandas as pd
import requests
from pathlib import Path

API_BASE_URL = "http://localhost:8000"
QUARANTINE_FILE = Path("outputs/quarantine.csv")

st.set_page_config(
    page_title="Human Review Dashboard",
    layout="wide"
)

st.title("Human Data Quality Review")
st.write(
    "This interface allows a human to review the data that has been quarantined "
    "and approve or reject the automated data quality decision that system makes."
)

# Loading the quarantined data

if not QUARANTINE_FILE.exists():
    st.warning("No quarantined data found.")
    st.stop()

df = pd.read_csv(QUARANTINE_FILE)

if df.empty:
    st.success("No rows currentyl require human review")
    st.stop()

# Rows that require review

st.subheader("Rows Pending Review")

st.dataframe(df, use_container_width=True)

row_index = st.selectbox(
    "Select a row to review: ",
    df.index.tolist()
)

selected_row = df.loc[row_index]

# Showing details of the selected row

st.subheader("Selected Row Details")

st.json(selected_row.to_dict())

# Human decision options

st.subheader("Human Decision")

approve = st.button("Approve (Valid)")
reject = st.button("Reject (Invalid, will stay quarantined)")

# Handling human's decision

if approve or reject:
    decision = "approve" if approve else "reject"

    payload = {
        "row_index": int(row_index),
        "decision": decision,
        "reviewer": "human_reviewer"
    }

    try:
        response = requests.post(
            f"{API_BASE_URL}/review/decision",
            json=payload,
            timeout=5
        )

        response.raise_for_status()

        st.success(f"Decision '{decision}' submitted successfully.")

    except Exception as e:
        st.error(f"Failed to submit decision: {e}")

