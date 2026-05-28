import streamlit as st
import pandas as pd
import requests

API_BASE_URL = "http://localhost:8000"

st.set_page_config(
    page_title="Human Review Dashboard",
    layout="wide"
)

# Custom Glassmorphic Premium Styles
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;800&family=Plus+Jakarta+Sans:wght@300;400;600;700&display=swap');
    
    .stApp {
        background: radial-gradient(circle at 80% 20%, #1e1b4b 0%, #0f172a 60%, #020617 100%);
    }
    
    h1, h2, h3, p, span, label, div {
        font-family: 'Plus Jakarta Sans', sans-serif !important;
    }
    
    h1 {
        font-family: 'Outfit', sans-serif !important;
        background: linear-gradient(135deg, #a855f7 0%, #6366f1 50%, #3b82f6 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-weight: 800 !important;
        font-size: 3rem !important;
        letter-spacing: -0.03em;
        margin-bottom: 0.2rem !important;
    }
    
    .subtitle {
        color: #94a3b8;
        font-size: 1.15rem;
        margin-bottom: 2rem;
    }
    
    /* Card/Container styling */
    div.stDataFrame, div.stJson {
        border-radius: 16px !important;
        border: 1px solid rgba(255, 255, 255, 0.08) !important;
        background-color: rgba(15, 23, 42, 0.6) !important;
        backdrop-filter: blur(16px) !important;
        padding: 10px;
        box-shadow: 0 10px 30px rgba(0,0,0,0.2) !important;
    }
    
    /* Custom button styling */
    div.stButton > button {
        border-radius: 10px !important;
        padding: 0.6rem 1.8rem !important;
        font-weight: 600 !important;
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1) !important;
        border: 1px solid rgba(255, 255, 255, 0.1) !important;
        color: #ffffff !important;
    }
    
    div.stButton > button:hover {
        transform: translateY(-2px) !important;
        box-shadow: 0 8px 24px rgba(99, 102, 241, 0.3) !important;
        border-color: rgba(99, 102, 241, 0.5) !important;
    }
    
    /* Input formatting */
    input, select, textarea {
        background-color: rgba(15, 23, 42, 0.8) !important;
        border: 1px solid rgba(255, 255, 255, 0.1) !important;
        border-radius: 10px !important;
        color: #f8f9fa !important;
    }
</style>
""", unsafe_allow_html=True)

st.title("Human Data Quality Review")
st.markdown("<p class='subtitle'>Review quarantined records, inspect validation issues, and overwrite automated decisions with full audit traceability.</p>", unsafe_allow_html=True)

# Dataset selection
csv_path = st.text_input("Dataset CSV File Path:", "Messy_Employee_dataset_v2.csv")

if not csv_path:
    st.warning("Please provide a valid dataset path.")
    st.stop()

# Query review queue from FastAPI
try:
    response = requests.post(
        f"{API_BASE_URL}/review/queue",
        json={"csv_path": csv_path},
        timeout=5
    )
    response.raise_for_status()
    queue_data = response.json()
    items = queue_data.get("items", [])
    df = pd.DataFrame(items)
except Exception as e:
    st.error(f"Could not connect to FastAPI server. Ensure the server is running on {API_BASE_URL}. Error: {e}")
    st.stop()

if df.empty:
    st.success("🎉 Excellent! No rows currently require human review.")
    st.stop()

# Layout splits
col1, col2 = st.columns([3, 2], gap="large")

with col1:
    st.subheader("📋 Pending Review Queue")
    st.dataframe(df, use_container_width=True)

with col2:
    st.subheader("🔍 Inspect & Decide")
    
    employee_id = st.selectbox(
        "Select an Employee ID to review:",
        df["employee_id"].tolist()
    )
    
    selected_row = df[df["employee_id"] == employee_id].iloc[0]
    
    st.markdown("### Record Metadata")
    st.json(selected_row.to_dict())
    
    # Reviewer inputs
    st.markdown("### Action Decision")
    review_notes = st.text_input("Reviewer Notes (highly recommended):", placeholder="Explain the rationale for this change...")
    
    btn_col1, btn_col2, btn_col3 = st.columns(3)
    
    approve = btn_col1.button("🟢 Approve", help="Marks record as ACCEPT and resolves quarantine.")
    reject = btn_col2.button("🔴 Reject", help="Confirms record is invalid, keeping it quarantined.")
    fix = btn_col3.button("🟡 Request Fix", help="Flags record as NEEDS_FIX.")
    
    if approve or reject or fix:
        decision = "APPROVE" if approve else "REJECT" if reject else "FIX"
        
        payload = {
            "employee_id": str(employee_id),
            "decision": decision,
            "review_notes": review_notes if review_notes else "Reviewed via Human Dashboard"
        }
        
        try:
            res = requests.post(
                f"{API_BASE_URL}/review/decision",
                json=payload,
                timeout=5
            )
            res.raise_for_status()
            st.balloons()
            st.success(f"Successfully submitted decision '{decision}' for employee {employee_id}!")
            # Trigger rerun to refresh queue
            st.button("🔄 Refresh Queue")
        except Exception as e:
            st.error(f"Failed to submit decision: {e}")
