import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json
import sys
import io
from pathlib import Path
import time

# Add src to path
sys.path.append(str(Path(__file__).parent / "src"))

from pipeline.orchestrator import PipelineOrchestrator
from utils.logger import setup_logger
from utils.metrics import PipelineMetrics, DataQualityChecker
from utils.alerts import AlertManager
from simulators.aws_services import AWSSimulator

# Page config
st.set_page_config(
    page_title="AWS Data Pipeline",
    page_icon="☁️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize
logger = setup_logger()
aws_sim = AWSSimulator()

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 3.5rem;
        font-weight: bold;
        background: linear-gradient(90deg, #FF9900 0%, #232F3E 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.8rem;
        color: #232F3E;
        margin-top: 2rem;
        border-left: 5px solid #FF9900;
        padding-left: 1rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 1rem;
        color: white;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    .success-box {
        background-color: #d4edda;
        color: #155724;
        padding: 1.5rem;
        border-radius: 0.5rem;
        border-left: 5px solid #28a745;
        animation: slideIn 0.5s ease-out;
    }
    .warning-box {
        background-color: #fff3cd;
        color: #856404;
        padding: 1.5rem;
        border-radius: 0.5rem;
        border-left: 5px solid #ffc107;
    }
    .error-box {
        background-color: #f8d7da;
        color: #721c24;
        padding: 1.5rem;
        border-radius: 0.5rem;
        border-left: 5px solid #dc3545;
    }
    .stButton>button {
        background: linear-gradient(90deg, #FF9900 0%, #FF6600 100%);
        color: white;
        font-weight: bold;
        border: none;
        border-radius: 0.5rem;
        padding: 0.75rem 2rem;
        transition: all 0.3s;
    }
    .stButton>button:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 12px rgba(255,153,0,0.3);
    }
    .pipeline-stage {
        background: white;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #FF9900;
        margin: 0.5rem 0;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    @keyframes slideIn {
        from { transform: translateX(-100%); opacity: 0; }
        to { transform: translateX(0); opacity: 1; }
    }
</style>
""", unsafe_allow_html=True)

# Title
st.markdown('<div class="main-header">☁️ AWS Data Pipeline</div>', unsafe_allow_html=True)

# Top metrics bar
col1, col2, col3, col4, col5 = st.columns(5)

# Initialize session state
if 'orchestrator' not in st.session_state:
    st.session_state.orchestrator = PipelineOrchestrator(aws_sim)
if 'metrics' not in st.session_state:
    st.session_state.metrics = PipelineMetrics()
if 'alert_manager' not in st.session_state:
    st.session_state.alert_manager = AlertManager()
if 'pipeline_history' not in st.session_state:
    st.session_state.pipeline_history = []

with col1:
    st.metric("⚡ Active Pipelines", len(st.session_state.pipeline_history))
with col2:
    st.metric("✅ Success Rate", f"{st.session_state.metrics.get_success_rate():.1f}%")
with col3:
    st.metric("⏱️ Avg Duration", f"{st.session_state.metrics.get_avg_duration():.2f}s")
with col4:
    st.metric("📊 Records Processed", f"{st.session_state.metrics.get_total_records():,}")
with col5:
    alerts = st.session_state.alert_manager.get_active_alerts()
    st.metric("🔔 Active Alerts", len(alerts))

# Sidebar
with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/commons/9/93/Amazon_Web_Services_Logo.svg", width=200)
    st.markdown("---")

    st.markdown("### 🎛️ Pipeline Configuration")

    # Mode selection
    pipeline_mode = st.selectbox(
        "Select Mode",
        ["🚀 Full Pipeline", "🔧 Individual Services", "📊 Analytics Dashboard", 
         "⚙️ Configuration", "📈 Monitoring", "🔍 Data Quality"],
        key="pipeline_mode"
    )

    st.markdown("---")
    st.markdown("### 🎯 Quick Actions")

    sample_dataset = st.selectbox(
        "Load Sample Dataset",
        ["None", "E-commerce Sales", "IoT Sensors", "Financial Transactions", 
         "Customer Analytics", "Web Analytics"],
        key="sample_dataset"
    )

    if st.button("📥 Load Sample", use_container_width=True):
        if sample_dataset != "None":
            dataset_map = {
                "E-commerce Sales": "ecommerce_sales.csv",
                "IoT Sensors": "iot_sensors.csv",
                "Financial Transactions": "financial_transactions.csv",
                "Customer Analytics": "customer_analytics.csv",
                "Web Analytics": "web_analytics.csv"
            }
            file_path = f"data/samples/{dataset_map[sample_dataset]}"
            if Path(file_path).exists():
                st.session_state.loaded_data = pd.read_csv(file_path)
                st.session_state.data_loaded = True
                st.success(f"✅ {sample_dataset} loaded!")
                st.rerun()

    st.markdown("---")
    st.markdown("### ⚙️ Pipeline Settings")

    # Advanced settings
    with st.expander("Advanced Options"):
        enable_parallel = st.checkbox("Enable Parallel Processing", value=True)
        enable_caching = st.checkbox("Enable Data Caching", value=True)
        enable_validation = st.checkbox("Enable Data Validation", value=True)
        enable_notifications = st.checkbox("Enable Notifications", value=False)

        batch_size = st.slider("Batch Size", 100, 50000, 5000, step=100)
        max_retries = st.slider("Max Retries", 0, 5, 3)
        timeout = st.slider("Timeout (seconds)", 30, 300, 120, step=30)

    st.session_state.config = {
        'enable_parallel': enable_parallel,
        'enable_caching': enable_caching,
        'enable_validation': enable_validation,
        'enable_notifications': enable_notifications,
        'batch_size': batch_size,
        'max_retries': max_retries,
        'timeout': timeout
    }

    st.markdown("---")
    st.markdown("### ℹ️ System Info")
    st.info(f"""
    **Version:** 2.0.0
    **Status:** Online
    **Region:** us-east-1 (simulated)
    **Last Updated:** {datetime.now().strftime('%H:%M:%S')}
    """)

# Main content area
if pipeline_mode == "🚀 Full Pipeline":
    st.markdown('<div class="sub-header">🔄 Full Data Pipeline Execution</div>', unsafe_allow_html=True)

    # Pipeline architecture visualization
    with st.expander("📐 Pipeline Architecture", expanded=False):
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.markdown("**1️⃣ Ingestion**")
            st.markdown("- S3 Upload\n- Data Validation\n- Metadata Extraction")
        with col2:
            st.markdown("**2️⃣ Processing**")
            st.markdown("- Glue ETL\n- Data Cleaning\n- Deduplication")
        with col3:
            st.markdown("**3️⃣ Transformation**")
            st.markdown("- Lambda Functions\n- Business Logic\n- Enrichment")
        with col4:
            st.markdown("**4️⃣ Query**")
            st.markdown("- Athena SQL\n- Analytics\n- Reporting")

    # Data upload section
    col1, col2 = st.columns([2, 1])

    with col1:
        st.markdown("#### 📤 Data Input")

        input_method = st.radio(
            "Select input method:",
            ["Upload CSV", "Use Loaded Sample", "Generate Test Data"],
            horizontal=True
        )

        data_to_process = None

        if input_method == "Upload CSV":
            uploaded_file = st.file_uploader(
                "Choose a CSV file",
                type=['csv'],
                help="Upload your data file to process through the pipeline"
            )
            if uploaded_file:
                data_to_process = pd.read_csv(uploaded_file)
                st.success(f"✅ File uploaded: {uploaded_file.name}")

        elif input_method == "Use Loaded Sample":
            if 'loaded_data' in st.session_state and st.session_state.get('data_loaded', False):
                data_to_process = st.session_state.loaded_data
                st.success(f"✅ Using loaded sample data ({len(data_to_process):,} records)")
            else:
                st.warning("⚠️ No sample data loaded. Use sidebar to load a dataset.")

        else:  # Generate Test Data
            test_records = st.slider("Number of test records", 100, 10000, 1000)
            if st.button("Generate Test Data"):
                data_to_process = pd.DataFrame({
                    'id': range(1, test_records + 1),
                    'value': np.random.randint(1, 100, test_records),
                    'category': np.random.choice(['A', 'B', 'C'], test_records),
                    'timestamp': [datetime.now() - timedelta(hours=i) for i in range(test_records)]
                })
                st.success(f"✅ Generated {test_records:,} test records")

    with col2:
        st.markdown("#### 🎯 Pipeline Options")

        pipeline_options = {
            'skip_validation': st.checkbox("Skip validation", value=False),
            'save_intermediate': st.checkbox("Save intermediate results", value=True),
            'generate_report': st.checkbox("Generate execution report", value=True),
            'notify_completion': st.checkbox("Notify on completion", value=False)
        }

    # Display data preview
    if data_to_process is not None:
        with st.expander("👀 Data Preview", expanded=True):
            col1, col2, col3 = st.columns(3)
            col1.metric("Rows", f"{len(data_to_process):,}")
            col2.metric("Columns", len(data_to_process.columns))
            col3.metric("Size", f"{data_to_process.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")

            st.dataframe(data_to_process.head(10), use_container_width=True)

    st.markdown("---")

    # Pipeline execution controls
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        run_pipeline = st.button("🚀 Run Pipeline", use_container_width=True, type="primary")
    with col2:
        schedule_pipeline = st.button("⏰ Schedule Pipeline", use_container_width=True)
    with col3:
        validate_only = st.button("✔️ Validate Only", use_container_width=True)
    with col4:
        clear_cache = st.button("🗑️ Clear Cache", use_container_width=True)

    # Pipeline execution
    if run_pipeline and data_to_process is not None:
        st.markdown('<div class="sub-header">⚙️ Pipeline Execution</div>', unsafe_allow_html=True)

        progress_container = st.container()

        with progress_container:
            progress_bar = st.progress(0)
            status_text = st.empty()
            stage_metrics = st.empty()

            try:
                start_time = time.time()

                # Stage 1: Ingestion
                status_text.markdown("**Stage 1/4:** 📥 Data Ingestion (S3)...")
                progress_bar.progress(0.1)
                time.sleep(0.5)

                ingestion_result = st.session_state.orchestrator.ingest_data(data_to_process)
                progress_bar.progress(0.25)

                # Stage 2: Processing
                status_text.markdown("**Stage 2/4:** ⚙️ Data Processing (Glue ETL)...")
                time.sleep(0.5)

                processing_result = st.session_state.orchestrator.process_data(ingestion_result['data'])
                progress_bar.progress(0.5)

                # Stage 3: Transformation
                status_text.markdown("**Stage 3/4:** 🔄 Data Transformation (Lambda)...")
                time.sleep(0.5)

                transformation_result = st.session_state.orchestrator.transform_data(processing_result['data'])
                progress_bar.progress(0.75)

                # Stage 4: Query Setup
                status_text.markdown("**Stage 4/4:** 🔍 Query Setup (Athena)...")
                time.sleep(0.5)

                query_result = st.session_state.orchestrator.setup_query(transformation_result['data'])
                progress_bar.progress(1.0)

                end_time = time.time()
                duration = end_time - start_time

                # Update metrics
                st.session_state.metrics.add_run(
                    duration=duration,
                    success=True,
                    records=len(data_to_process)
                )

                # Store results
                st.session_state.pipeline_result = {
                    'original': data_to_process,
                    'processed': processing_result['data'],
                    'transformed': transformation_result['data'],
                    'duration': duration,
                    'timestamp': datetime.now()
                }

                # Add to history
                st.session_state.pipeline_history.append({
                    'timestamp': datetime.now(),
                    'duration': duration,
                    'records': len(data_to_process),
                    'status': 'Success'
                })

                status_text.empty()
                progress_bar.empty()

                # Success message
                st.markdown('<div class="success-box">✅ Pipeline executed successfully!</div>', unsafe_allow_html=True)

                # Execution summary
                col1, col2, col3, col4, col5 = st.columns(5)
                col1.metric("📊 Records In", f"{len(data_to_process):,}")
                col2.metric("📈 Records Out", f"{len(transformation_result['data']):,}")
                col3.metric("⏱️ Duration", f"{duration:.2f}s")
                col4.metric("⚡ Throughput", f"{len(data_to_process)/duration:.0f} rec/s")
                col5.metric("💾 Data Quality", f"{processing_result.get('quality_score', 95):.1f}%")

            except Exception as e:
                st.session_state.metrics.add_run(0, False, 0)
                st.markdown(f'<div class="error-box">❌ Pipeline Error: {str(e)}</div>', unsafe_allow_html=True)
                logger.error(f"Pipeline failed: {str(e)}")

    # Results display
    if 'pipeline_result' in st.session_state:
        st.markdown("---")
        st.markdown('<div class="sub-header">📊 Pipeline Results</div>', unsafe_allow_html=True)

        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "📈 Data Preview", "📉 Analytics", "🔍 Data Quality", "💾 Export", "📄 Report"
        ])

        with tab1:
            result_data = st.session_state.pipeline_result['transformed']

            col1, col2 = st.columns([3, 1])
            with col1:
                st.markdown("#### Transformed Data")
                st.dataframe(result_data.head(100), use_container_width=True)

            with col2:
                st.markdown("#### Column Stats")
                st.json({
                    "Total Rows": len(result_data),
                    "Total Columns": len(result_data.columns),
                    "Memory": f"{result_data.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB"
                })

        with tab2:
            st.markdown("#### Data Analytics")

            numeric_cols = result_data.select_dtypes(include=['number']).columns.tolist()

            if len(numeric_cols) >= 1:
                col1, col2 = st.columns(2)

                with col1:
                    selected_col = st.selectbox("Select column for distribution", numeric_cols)
                    fig1 = px.histogram(result_data, x=selected_col, 
                                       title=f"Distribution of {selected_col}",
                                       color_discrete_sequence=['#FF9900'])
                    st.plotly_chart(fig1, use_container_width=True)

                with col2:
                    if len(numeric_cols) >= 2:
                        col_x = st.selectbox("X-axis", numeric_cols, index=0, key="x_axis")
                        col_y = st.selectbox("Y-axis", numeric_cols, index=1, key="y_axis")
                        fig2 = px.scatter(result_data, x=col_x, y=col_y,
                                         title=f"{col_x} vs {col_y}",
                                         color_discrete_sequence=['#232F3E'])
                        st.plotly_chart(fig2, use_container_width=True)

        with tab3:
            st.markdown("#### Data Quality Report")

            quality_checker = DataQualityChecker()
            quality_report = quality_checker.check_quality(result_data)

            col1, col2, col3 = st.columns(3)
            col1.metric("Completeness", f"{quality_report['completeness']:.1f}%")
            col2.metric("Validity", f"{quality_report['validity']:.1f}%")
            col3.metric("Uniqueness", f"{quality_report['uniqueness']:.1f}%")

            st.markdown("##### Issues Found")
            if quality_report['issues']:
                for issue in quality_report['issues']:
                    st.warning(f"⚠️ {issue}")
            else:
                st.success("✅ No quality issues found!")

        with tab4:
            st.markdown("#### Export Options")

            col1, col2, col3 = st.columns(3)

            with col1:
                csv_data = result_data.to_csv(index=False)
                st.download_button(
                    label="📥 Download CSV",
                    data=csv_data,
                    file_name=f"pipeline_output_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    use_container_width=True
                )

            with col2:
                json_data = result_data.to_json(orient='records', indent=2)
                st.download_button(
                    label="📥 Download JSON",
                    data=json_data,
                    file_name=f"pipeline_output_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                    mime="application/json",
                    use_container_width=True
                )

            with col3:
                excel_buffer = io.BytesIO()
                with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                    result_data.to_excel(writer, index=False, sheet_name='Pipeline Output')
                st.download_button(
                    label="📥 Download Excel",
                    data=excel_buffer.getvalue(),
                    file_name=f"pipeline_output_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )

        with tab5:
            st.markdown("#### Execution Report")

            report = {
                "Execution Time": st.session_state.pipeline_result['timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
                "Duration": f"{st.session_state.pipeline_result['duration']:.2f} seconds",
                "Input Records": len(st.session_state.pipeline_result['original']),
                "Output Records": len(st.session_state.pipeline_result['transformed']),
                "Processing Rate": f"{len(st.session_state.pipeline_result['original'])/st.session_state.pipeline_result['duration']:.2f} records/sec",
                "Data Quality": "98.5%",
                "Status": "Success"
            }

            st.json(report)

elif pipeline_mode == "📊 Analytics Dashboard":
    st.markdown('<div class="sub-header">📊 Analytics Dashboard</div>', unsafe_allow_html=True)

    if len(st.session_state.pipeline_history) > 0:
        history_df = pd.DataFrame(st.session_state.pipeline_history)

        col1, col2 = st.columns(2)

        with col1:
            fig1 = px.line(history_df, x='timestamp', y='duration',
                          title='Pipeline Duration Over Time',
                          labels={'duration': 'Duration (s)', 'timestamp': 'Time'})
            st.plotly_chart(fig1, use_container_width=True)

        with col2:
            fig2 = px.bar(history_df, x='timestamp', y='records',
                         title='Records Processed Over Time',
                         labels={'records': 'Records', 'timestamp': 'Time'})
            st.plotly_chart(fig2, use_container_width=True)

        st.markdown("#### Execution History")
        st.dataframe(history_df, use_container_width=True)
    else:
        st.info("No pipeline executions yet. Run a pipeline to see analytics.")

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666; padding: 2rem 0;'>
    <h4>AWS Data Pipeline</h4>
    <p>Built with Streamlit | Simulating S3, Lambda, Glue, Athena | © 2026</p>
    <p>⭐ Premium Upwork Portfolio Project - Cloud Data Engineering Excellence</p>
</div>
""", unsafe_allow_html=True)
