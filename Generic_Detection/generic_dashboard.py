import streamlit as st
import requests
import pandas as pd
import plotly.express as px

# Configuration
API_URL = "http://127.0.0.1:8000/upload/analyze"

# Page config
st.set_page_config(
    page_title="Universal Data Quality Dashboard",
    page_icon="📊",
    layout="wide"
)

# Header
st.title("📊 Universal Data Quality Dashboard")
st.markdown("""
This dashboard evaluates your dataset against core data engineering pillars:
**Completeness, Uniqueness, Consistency, and Validity (Accuracy).**
Upload any CSV to identify anomalies and make informed, data-driven decisions.
""")

# Setup Sidebar for File Upload
with st.sidebar:
    st.header("Upload Dataset")
    uploaded_file = st.file_uploader("Choose a CSV file (Max 200MB)", type=["csv"])
    analyze_button = st.button("Analyze Dataset", type="primary", use_container_width=True)

# Main Dashboard Content
if uploaded_file is not None and analyze_button:
    with st.spinner("Running deep data quality analysis..."):
        # Send the file to our FastAPI endpoint
        files = {"file": (uploaded_file.name, uploaded_file.getvalue(), "text/csv")}
        try:
            response = requests.post(API_URL, files=files)
            response.raise_for_status()
            data = response.json()
            report = data["report"]
            
            # --- 1. OVERALL HEALTH & DATASET INFO ---
            st.markdown("---")
            col1, col2, col3, col4 = st.columns(4)
            
            health = report["health"]
            status = health["dataset_health"]
            
            # Dynamic color coding for health status
            status_color = "🟢" if status == "GOOD" else "🟡" if status == "DEGRADED" else "🔴"
            
            col1.metric("Overall Health", f"{status_color} {status}")
            col2.metric("Average Quality Score", f"{health['average_row_score']} / 100")
            col3.metric("Total Rows", f"{report['dataset_info']['total_rows']:,}")
            col4.metric("Total Columns", f"{report['dataset_info']['total_columns']:,}")
            
            # --- 2. CRITICAL ANOMALIES ---
            anomalies = report.get("anomalies", [])
            if anomalies:
                st.subheader("🚨 Detected Anomalies")
                for anomaly in anomalies:
                    severity = anomaly["severity"]
                    # Choose alert type based on severity
                    if severity == "CRITICAL":
                        st.error(f"**{anomaly['type']}** (Column: `{anomaly['column']}`): {anomaly['detail']}")
                    elif severity == "HIGH":
                        st.warning(f"**{anomaly['type']}** (Column: `{anomaly['column']}`): {anomaly['detail']}")
                    else:
                        st.info(f"**{anomaly['type']}** (Column: `{anomaly['column']}`): {anomaly['detail']}")
            else:
                st.success("No high or critical anomalies detected! The dataset looks structurally sound.")
                
            st.markdown("---")
            st.subheader("Deep Dive: Data Quality Pillars")
            
            # We use tabs to organize the different pillars of data quality
            tab1, tab2, tab3, tab4, tab5 = st.tabs([
                "Completeness", "Uniqueness", "Consistency", "Validity (Outliers)", "Row Preview"
            ])
            
            # ----------- TAB 1: COMPLETENESS -----------
            with tab1:
                st.markdown("**Core Question:** *Is the data missing?*")
                comp_data = report["completeness"]
                df_comp = pd.DataFrame(comp_data)
                
                if not df_comp.empty and df_comp["missing_count"].sum() > 0:
                    fig_comp = px.bar(
                        df_comp, 
                        x="column", 
                        y="missing_pct", 
                        color="severity",
                        color_discrete_map={"LOW": "green", "MEDIUM": "orange", "HIGH": "red"},
                        title="Missing Values by Column (%)",
                        labels={"missing_pct": "Missing (%)", "column": "Column"}
                    )
                    st.plotly_chart(fig_comp, use_container_width=True)
                else:
                    st.success("Perfect completeness! 100% of data is present.")
            
            # ----------- TAB 2: UNIQUENESS -----------
            with tab2:
                st.markdown("**Core Question:** *Are there duplicate records?*")
                dup_data = report["duplicates"]
                
                col_dup1, col_dup2 = st.columns(2)
                col_dup1.metric("Duplicate Rows", f"{dup_data['duplicate_count']:,}")
                col_dup2.metric("Duplication Rate", f"{dup_data['duplicate_pct']}%")
                
                if dup_data["duplicate_count"] > 0:
                    # Visual representation of duplicates vs unique
                    df_pie = pd.DataFrame({
                        "Type": ["Unique Rows", "Duplicate Rows"],
                        "Count": [dup_data["total_rows"] - dup_data["duplicate_count"], dup_data["duplicate_count"]]
                    })
                    fig_dup = px.pie(
                        df_pie, 
                        names="Type", 
                        values="Count",
                        hole=0.4,
                        color="Type",
                        color_discrete_map={"Unique Rows": "teal", "Duplicate Rows": "salmon"},
                        title="Dataset Uniqueness Breakdown"
                    )
                    st.plotly_chart(fig_dup, use_container_width=True)
                else:
                    st.success("Perfect uniqueness! 0 duplicate rows detected.")
                    
            # ----------- TAB 3: CONSISTENCY -----------
            with tab3:
                st.markdown("**Core Question:** *Are values consistently formatted within their columns?*")
                type_data = report["type_consistency"]
                df_type = pd.DataFrame(type_data)
                
                if not df_type.empty and df_type["inconsistent_count"].sum() > 0:
                    fig_type = px.bar(
                        df_type,
                        x="column",
                        y="inconsistent_pct",
                        hover_data=["inferred_type", "inconsistent_count"],
                        title="Type Inconsistency by Column (%)",
                        labels={"inconsistent_pct": "Inconsistent Types (%)", "column": "Column"},
                        color="inconsistent_pct",
                        color_continuous_scale="Reds"
                    )
                    st.plotly_chart(fig_type, use_container_width=True)
                else:
                    st.success("Perfect consistency! All columns have uniform data types.")
                    
            # ----------- TAB 4: VALIDITY (OUTLIERS) -----------
            with tab4:
                st.markdown("**Core Question:** *Do values fall within expected/statistical boundaries?*")
                outliers_data = report["outliers"]
                df_outliers = pd.DataFrame(outliers_data)
                
                if not df_outliers.empty and df_outliers["outlier_count"].sum() > 0:
                    fig_out = px.bar(
                        df_outliers,
                        x="column",
                        y="outlier_pct",
                        color="severity",
                        color_discrete_map={"LOW": "green", "MEDIUM": "orange", "HIGH": "red"},
                        hover_data=["outlier_count", "lower_bound", "upper_bound"],
                        title="Statistical Outliers by Numeric Column (%)",
                        labels={"outlier_pct": "Outliers (%)", "column": "Column"}
                    )
                    st.plotly_chart(fig_out, use_container_width=True)
                else:
                    st.info("No extreme statistical outliers detected in numeric columns, or no numeric columns present.")

            # ----------- TAB 5: ROW PREVIEW -----------
            with tab5:
                st.markdown("**Preview of Row-Level Scores** (First 20 rows)")
                df_preview = pd.DataFrame(report["row_scores_preview"])
                
                def color_status(val):
                    color = 'green' if val == 'GOOD' else 'orange' if val == 'WARNING' else 'red'
                    return f'color: {color}; font-weight: bold'
                
                if "Row_Usability_Status" in df_preview.columns:
                    st.dataframe(
                        df_preview.style.map(color_status, subset=['Row_Usability_Status']),
                        use_container_width=True
                    )
                else:
                    st.dataframe(df_preview, use_container_width=True)

        except requests.exceptions.RequestException as e:
            st.error(f"Failed to connect to the analysis server. Is the FastAPI server running?\nError: {e}")
        except Exception as e:
            st.error(f"An unexpected error occurred during analysis: {e}")
elif not uploaded_file:
    # Landing page instructions
    st.info("👈 Upload a CSV file in the sidebar to begin the analysis.")
