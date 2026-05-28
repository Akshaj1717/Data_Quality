import streamlit as st
import plotly.express as px
from data_loader import fetch_quality_results

st.set_page_config(page_title="Dataset Quality Dashboard", layout="wide")

# Custom Glassmorphic Premium Styles
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;800&family=Plus+Jakarta+Sans:wght@300;400;600;700&display=swap');
    
    .stApp {
        background: radial-gradient(circle at 10% 20%, #1e1b4b 0%, #0f172a 60%, #020617 100%);
    }
    
    h1, h2, h3, p, span, label, div {
        font-family: 'Plus Jakarta Sans', sans-serif !important;
    }
    
    h1 {
        font-family: 'Outfit', sans-serif !important;
        background: linear-gradient(135deg, #3b82f6 0%, #6366f1 50%, #ec4899 100%);
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
    
    /* Metrics display */
    div[data-testid="stMetricValue"] {
        font-family: 'Outfit', sans-serif !important;
        font-size: 2.2rem !important;
        font-weight: 700 !important;
        color: #ffffff !important;
    }
    
    div[data-testid="stMetricLabel"] {
        color: #94a3b8 !important;
        font-size: 0.95rem !important;
        font-weight: 600 !important;
    }

    /* Card container */
    .metric-card {
        background: rgba(255, 255, 255, 0.03);
        border: 1px solid rgba(255, 255, 255, 0.08);
        border-radius: 16px;
        padding: 20px;
        backdrop-filter: blur(12px);
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.2);
    }
    
    /* Input & Buttons */
    input, select, textarea {
        background-color: rgba(15, 23, 42, 0.8) !important;
        border: 1px solid rgba(255, 255, 255, 0.1) !important;
        border-radius: 10px !important;
        color: #f8f9fa !important;
    }
    
    div.stButton > button {
        border-radius: 10px !important;
        padding: 0.6rem 2rem !important;
        font-weight: 600 !important;
        background: linear-gradient(135deg, #3b82f6 0%, #6366f1 100%) !important;
        color: white !important;
        border: none !important;
        transition: all 0.3s ease !important;
    }
    
    div.stButton > button:hover {
        transform: translateY(-2px) !important;
        box-shadow: 0 8px 24px rgba(99, 102, 241, 0.4) !important;
    }
</style>
""", unsafe_allow_html=True)

st.title("Dataset Quality Health Dashboard")
st.markdown("<p class='subtitle'>Real-time data completeness profiling, row usability classification, and statistical health metrics monitoring.</p>", unsafe_allow_html=True)

# Path input
csv_path = st.text_input("CSV Dataset File Path:", "Messy_Employee_dataset_v2.csv")

if st.button("Analyze Dataset"):
    with st.spinner("Analyzing dataset health..."):
        try:
            summary, df = fetch_quality_results(csv_path)
            
            # Health Score and Classification section
            st.markdown("### 📊 Dataset Overview")
            
            col_health, col_stats = st.columns([1, 3], gap="large")
            
            with col_health:
                st.metric("Health Status", summary["dataset_health"])
                st.metric("Average Score", f"{summary['average_row_score']}%")
                st.metric("Bad Rows Rate", f"{summary['bad_row_percentage']}%")
            
            with col_stats:
                col1, col2, col3 = st.columns(3)
                col1.metric("GOOD Rows", (df["Row_Usability_Status"] == "GOOD").sum())
                col2.metric("WARNING Rows", (df["Row_Usability_Status"] == "WARNING").sum())
                col3.metric("BAD Rows", (df["Row_Usability_Status"] == "BAD").sum())
                
                # Usability Distribution chart
                status_counts = (
                    df["Row_Usability_Status"]
                    .value_counts()
                    .reset_index()
                )
                status_counts.columns = ["Status", "Count"]

                fig = px.pie(
                    status_counts,
                    names="Status",
                    values="Count",
                    color="Status",
                    color_discrete_map={"GOOD": "#10b981", "WARNING": "#f59e0b", "BAD": "#ef4444"},
                    hole=0.4,
                )
                fig.update_layout(
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    font_color="#f8f9fa",
                    margin=dict(t=30, b=0, l=0, r=0),
                    height=240,
                )
                st.plotly_chart(fig, use_container_width=True)
            
            # Quality score distribution
            st.subheader("📈 Row Quality Score Distribution")
            st.bar_chart(df["Row_Quality_Score"])

            # Data Table
            st.subheader("🔍 Usability Breakdown Registry")
            st.dataframe(
                df[["Employee_ID", "Row_Quality_Score", "Row_Usability_Status"]],
                use_container_width=True
            )
            
        except Exception as e:
            st.error(f"Failed to fetch dataset analysis from FastAPI. Ensure the backend server is running on http://127.0.0.1:8000. Error: {e}")