import streamlit as st
import os
import sys
import time

sys.path.append(os.path.join(os.path.dirname(__file__), 'Rag_agent', 'services'))

from chat_service import get_query_response, analyze_failure
from app.api.endpoints.airflow import AirflowClient

st.set_page_config(
    page_title="DataSight | AI Observability", 
    layout="wide", 
    page_icon="⚡",
    initial_sidebar_state="collapsed"
)

# Custom CSS for a completely fresh, modern look
st.markdown("""
<style>
    /* Main Background & Text */
    .stApp {
        background-color: #0E1117;
        color: #FAFAFA;
    }
    
    /* Headers */
    h1 {
        font-family: 'Inter', sans-serif;
        font-weight: 800;
        letter-spacing: -1px;
        background: -webkit-linear-gradient(45deg, #FF4B4B, #FF8080);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    h2, h3, h4 {
        font-family: 'Inter', sans-serif;
        font-weight: 600;
    }
    
    /* Status Cards */
    .status-card {
        background-color: #1E1E2E;
        border-radius: 12px;
        padding: 24px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.3);
        border: 1px solid #2B2B36;
        transition: transform 0.2s ease;
    }
    .status-card:hover {
        transform: translateY(-2px);
    }
    
    /* Status Labels */
    .badge-success { background: rgba(40, 167, 69, 0.15); color: #28a745; border: 1px solid #28a745; padding: 4px 12px; border-radius: 50px; font-weight: bold; font-size: 0.85em; }
    .badge-failed { background: rgba(220, 53, 69, 0.15); color: #dc3545; border: 1px solid #dc3545; padding: 4px 12px; border-radius: 50px; font-weight: bold; font-size: 0.85em; }
    .badge-running { background: rgba(255, 193, 7, 0.15); color: #ffc107; border: 1px solid #ffc107; padding: 4px 12px; border-radius: 50px; font-weight: bold; font-size: 0.85em; }
    
    /* DAG Name formatting */
    .dag-name { font-size: 1.25em; font-weight: bold; margin-bottom: 8px; color: #FFFFFF; }
    .dag-time { font-size: 0.85em; color: #8B8D98; margin-bottom: 16px; }
    
    /* Alert Panel */
    .alert-panel {
        background-color: rgba(220, 53, 69, 0.05);
        border-left: 4px solid #dc3545;
        padding: 20px;
        border-radius: 0 12px 12px 0;
        margin-bottom: 24px;
    }
</style>
""", unsafe_allow_html=True)

# Application Header
col_logo, col_title = st.columns([1, 10])
with col_title:
    st.title("DataSight AI")
    st.markdown("<p style='color: #8B8D98; font-size: 1.2em; margin-bottom: 2rem;'>Autonomous Airflow Pipeline Monitoring & Diagnostics</p>", unsafe_allow_html=True)

# Initialize Airflow Client
@st.cache_resource
def get_airflow_client():
    return AirflowClient()

client = get_airflow_client()

# Fetch Data
try:
    with st.spinner("Connecting to Apache Airflow Cluster..."):
        summary = client.get_dashboard_summary()
        failures = client.get_active_failures()

    # Stats ROW
    st.markdown("### 📊 System Overview")
    stat1, stat2, stat3, stat4 = st.columns(4)
    total_dags = len(summary)
    failed_count = len([d for d in summary if d['status'] == 'failed'])
    success_count = len([d for d in summary if d['status'] == 'success'])
    running_count = len([d for d in summary if d['status'] == 'running'])

    stat1.metric("Total Pipelines", total_dags)
    stat2.metric("Healthy", success_count, f"{(success_count/max(1, total_dags))*100:.0f}%" if total_dags > 0 else "0%", delta_color="normal")
    stat3.metric("Running", running_count, None, delta_color="off")
    stat4.metric("Failures Detected", failed_count, f"{failed_count} active", delta_color="inverse")

    st.markdown("<hr style='border-color: #2B2B36; margin: 2rem 0;'>", unsafe_allow_html=True)


    # Main Content Layout
    col_main, col_side = st.columns([1.5, 1])

    # LEFT COLUMN: Pipeline Grid
    with col_main:
        st.markdown("### 🚦 Active Pipelines")
        
        if not summary:
            st.info("No active DAGs found. Please ensure Airflow is running.")
        else:
            # Create a grid for DAG cards
            for i in range(0, len(summary), 2):
                cols = st.columns(2)
                for j in range(2):
                    if i + j < len(summary):
                        dag = summary[i+j]
                        
                        status_class = f"badge-{dag['status']}"
                        icon = "🟢" if dag['status'] == 'success' else "🔴" if dag['status'] == 'failed' else "🟡"
                        
                        with cols[j]:
                            st.markdown(f"""
                            <div class="status-card" style="margin-bottom: 20px;">
                                <div class="dag-name">{dag['dag_id']}</div>
                                <div class="dag-time">Last exec: {dag['execution_date'][:19]}</div>
                                <div><span class="{status_class}">{icon} {dag['status'].upper()}</span></div>
                            </div>
                            """, unsafe_allow_html=True)
                            
except Exception as e:
    st.error(f"**Connection Error:** Could not connect to Apache Airflow. Please ensure the cluster is running (`docker-compose up -d`) and the credentials in `.env` are correct.")
    summary = []
    failures = []
    
    # Empty column placeholder so the rest of the UI doesn't crash
    col_main, col_side = st.columns([1.5, 1])
    with col_main:
        st.info("Waiting for Airflow Connection...")

# RIGHT COLUMN: AI Alerts & Diagnostics
with col_side:
    st.markdown("### 🚨 AI Agent Insights")
    
    if not failures:
        st.markdown(f"""
        <div class="status-card" style="text-align: center; padding: 40px 20px;">
            <div style="font-size: 3em; margin-bottom: 10px;">✨</div>
            <h4 style="color: #4CAF50;">System Nominal</h4>
            <p style="color: #8B8D98;">The AI Agent is monitoring. No anomalies detected in recent DAG runs.</p>
        </div>
        """, unsafe_allow_html=True)
    else:
        for failure in failures:
            st.markdown(f"""
            <div class="alert-panel">
                <h4 style="margin-top:0; color: #dc3545;">Anomaly Detected</h4>
                <p style="margin-bottom:5px;"><b>DAG:</b> `{failure['dag_id']}`</p>
                <p><b>Task:</b> `{failure['task_id']}`</p>
            </div>
            """, unsafe_allow_html=True)
            
            cache_key = f"diag_v2_{failure['run_id']}_{failure['task_id']}"
            
            if cache_key not in st.session_state:
                with st.spinner("🧠 AI Synthesizing Solution from Documentation..."):
                    diagnosis = analyze_failure(failure['dag_id'], failure['task_id'], failure['logs'])
                    st.session_state[cache_key] = diagnosis
                    
            with st.expander("🤖 View AI Resolution Guide", expanded=True):
                st.markdown(st.session_state[cache_key].replace("Agent response:", "").strip())
                
            with st.expander("📜 View Raw Traceback", expanded=False):
                truncated_logs = failure['logs'][-1500:] if len(failure['logs']) > 1500 else failure['logs']
                st.code(truncated_logs, language="text")

# Floating Refresh Button
st.markdown("<br>", unsafe_allow_html=True)
col_ref1, col_ref2, col_ref3 = st.columns([1,1,1])
with col_ref2:
    if st.button("🔄 Sync with Cluster", use_container_width=True):
        st.rerun()
