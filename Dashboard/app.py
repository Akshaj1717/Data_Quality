import streamlit as st
import plotly.express as px
from data_loader import fetch_quality_results

st.set_page_config(page_title="Dataset Quality Dashboard", layout="wide")

st.title("Datset Quality Health Dashboard")

csv_path =  st.text_input("CSV File Path:", r"C:\Users\aksha\MCP_DATA_Quality\Messy_Employee_dataset_v2.csv"
)

if st.button("Analyze Dataset"):
    summary, df = fetch_quality_results(csv_path)

    st.subheader("Row Usability Distribution")

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
        title="Row Usability Breakdown",
        hole=0.4,  # donut style (optional but nice)
    )

    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Overall Dataset Health")
    st.metric("Health Status", summary["dataset_health"])

    col1, col2, col3 = st.columns(3)
    col1.metric("GOOD Rows", (df["Row_Usability_Status"] == "GOOD").sum())
    col2.metric("WARNING Rows", (df["Row_Usability_Status"] == "WARNING").sum())
    col3.metric("BAD Rows", (df["Row_Usability_Status"] == "BAD").sum())

    st.subheader("Row Quality Score Distribution")
    st.bar_chart(df["Row_Quality_Score"])

    st.subheader("Row Usability Breakdown")
    st.dataframe(
        df[["Employee_ID", "Row_Quality_Score", "Row_Usability_Status"]],
        use_container_width=True
    )