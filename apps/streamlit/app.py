import streamlit as st
import pandas as pd
import json
import requests
import plotly.express as px
import plotly.graph_objects as go
from typing import List, Dict, Any, Optional, Tuple
import os
import re
from datetime import datetime
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session

# ================================
# 🎨 CONFIGURATION & STYLING
# ================================

st.set_page_config(
    page_title="TechUp Analytics Copilot", 
    page_icon="🤖", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for a sleek, modern look
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 10px;
        color: white;
        margin-bottom: 2rem;
    }
    .chat-message {
        padding: 1rem;
        border-radius: 10px;
        margin: 1rem 0;
        border-left: 4px solid #667eea;
    }
    .user-message {
        background: #f0f2f6;
        border-left-color: #667eea;
    }
    .assistant-message {
        background: #e8f4fd;
        border-left-color: #1f77b4;
    }
    .stButton > button {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        border-radius: 5px;
    }
</style>
""", unsafe_allow_html=True)

# ================================
# 🔧 CORE CONFIGURATION
# ================================

DEFAULT_WAREHOUSE = "PIPELINE_WH"
CORTEX_ANALYST_ENDPOINT = "/api/v2/cortex/analyst/message"

# ================================
# 🧠 SNOWFLAKE SESSION & UTILITIES
# ================================

@st.cache_resource(show_spinner=False)
def get_session() -> Session:
    """Get Snowflake session - active session in Snowflake, or build from env for local dev"""
    try:
        return get_active_session()
    except Exception:
        if os.getenv("SNOWFLAKE_ACCOUNT"):
            conn_params = {
                "account": os.getenv("SNOWFLAKE_ACCOUNT"),
                "user": os.getenv("SNOWFLAKE_USER"),
                "password": os.getenv("SNOWFLAKE_PASSWORD"),
                "role": os.getenv("SNOWFLAKE_ROLE"),
                "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
                "database": os.getenv("SNOWFLAKE_DATABASE"),
                "schema": os.getenv("SNOWFLAKE_SCHEMA"),
            }
            return Session.builder.configs(conn_params).create()
        return Session.builder.getOrCreate()

def get_databases(session: Session) -> List[str]:
    """Get list of available databases"""
    try:
        rows = session.sql("SHOW DATABASES").collect()
        return [row['name'] for row in rows if not row['name'].startswith('SNOWFLAKE')]
    except:
        return []

def get_schemas(session: Session, database: str) -> List[str]:
    """Get schemas in a database"""
    try:
        rows = session.sql(f"SHOW SCHEMAS IN DATABASE {database}").collect()
        return [row['name'] for row in rows if not row['name'].startswith('INFORMATION_SCHEMA')]
    except:
        return []





# ================================
# 🎨 VISUALIZATION ENGINE
# ================================

def create_visualization(df: pd.DataFrame, chart_type: str = 'auto') -> go.Figure:
    """Create beautiful visualizations from data"""
    
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(text="No data to display", xref="paper", yref="paper", x=0.5, y=0.5)
        return fig
    
    # Auto-detect best chart type
    if chart_type == 'auto':
        if len(df.columns) == 2:
            numeric_cols = df.select_dtypes(include=['number']).columns
            if len(numeric_cols) == 1:
                chart_type = 'bar'
            else:
                chart_type = 'line'
        else:
            chart_type = 'table'
    
    # Create visualization based on type
    if chart_type == 'bar':
        x_col = df.columns[0]
        y_col = df.columns[1] if len(df.columns) > 1 else df.columns[0]
        fig = px.bar(df, x=x_col, y=y_col, 
                    title=f'{y_col.title()} by {x_col.title()}',
                    color_discrete_sequence=['#667eea'])
        
    elif chart_type == 'line':
        x_col = df.columns[0]
        y_col = df.columns[1] if len(df.columns) > 1 else df.columns[0]
        fig = px.line(df, x=x_col, y=y_col,
                     title=f'{y_col.title()} Trend',
                     color_discrete_sequence=['#667eea'])
        
    elif chart_type == 'pie':
        fig = px.pie(df, names=df.columns[0], values=df.columns[1],
                    title='Distribution',
                    color_discrete_sequence=px.colors.sequential.Blues_r)
        
    else:  # table
        fig = go.Figure(data=[go.Table(
            header=dict(values=list(df.columns),
                       fill_color='#667eea',
                       font=dict(color='white'),
                       align='left'),
            cells=dict(values=[df[col] for col in df.columns],
                      fill_color='lavender',
                      align='left'))
        ])
    
    # Style the figure
    fig.update_layout(
        template='plotly_white',
        title_font_size=16,
        title_x=0.5,
        margin=dict(l=20, r=20, t=40, b=20)
    )
    
    return fig

# ================================
# 💾 PIPELINE INTEGRATION
# ================================

def create_pipeline_from_analysis(session: Session, sql: str, pipeline_name: str, target_table: str) -> bool:
    """Create a dynamic table pipeline from analysis SQL"""
    try:
        # Insert into PIPELINE_CONFIG
        pipeline_sql = f"""
        INSERT INTO PIPELINE_CONFIG (
            pipeline_id, 
            source_table_name, 
            transformation_sql_snippet, 
            target_dt_name, 
            lag_minutes, 
            warehouse, 
            status
        ) VALUES (
            '{pipeline_name}',
            'ANALYTICS_SOURCE',
            $${sql}$$,
            '{target_table}',
            15,
            '{DEFAULT_WAREHOUSE}',
            'PENDING'
        )
        """
        
        session.sql(pipeline_sql).collect()
        return True
        
    except Exception as e:
        st.error(f"Failed to create pipeline: {str(e)}")
        return False


# ------------------------------
# Steward approval helpers
# ------------------------------
PROHIBITED_TOKENS = (
    ' use ', ' set ', ' create ', ' alter ', ' drop ', ' grant ', ' revoke ', ' call ',
    ' copy ', ' insert ', ' update ', ' delete ', ' merge ', ' truncate '
)

def validate_pipeline_sql(sql_text: str) -> Tuple[bool, str]:
    s = (sql_text or '').strip()
    if not s:
        return False, 'SQL is empty'
    s_lower = ' ' + s.lower() + ' '
    if ';' in s_lower:
        return False, 'Only a single statement without semicolons is allowed'
    if not (s.lstrip().lower().startswith('select') or s.lstrip().lower().startswith('with')):
        return False, 'SQL must start with SELECT or WITH'
    for token in PROHIBITED_TOKENS:
        if token in s_lower:
            return False, f"Unsupported token found: {token.strip()}"
    return True, ''

def create_pipeline_with_overrides(session: Session, sql: str, pipeline_name: str, target_table: str, lag_minutes: int, warehouse: str) -> bool:
    try:
        insert_sql = f"""
        INSERT INTO PIPELINE_CONFIG (
            pipeline_id, 
            source_table_name, 
            transformation_sql_snippet, 
            target_dt_name, 
            lag_minutes, 
            warehouse, 
            status
        ) VALUES (
            '{pipeline_name}',
            'ANALYTICS_SOURCE',
            $${sql}$$,
            '{target_table}',
            {lag_minutes},
            '{warehouse or DEFAULT_WAREHOUSE}',
            'PENDING'
        )
        """
        session.sql(insert_sql).collect()
        return True
    except Exception as e:
        st.error(f"Failed to insert pipeline config: {e}")
        return False


# ================================
# 🧩 PIPELINE FACTORY INSTALLER (INLINE)
# ================================

def install_pipeline_factory(session: Session, warehouse: str) -> None:
    # Create PIPELINE_CONFIG only; DTs are created directly on approval.
    session.sql(
        """
create table if not exists PIPELINE_CONFIG (
  pipeline_id                varchar          not null,
  source_table_name          varchar          not null,
  transformation_sql_snippet string           not null,
  target_dt_name             varchar          not null,
  lag_minutes                number(10,0)     not null,
  warehouse                  varchar          not null,
  status                     varchar          not null,
  created_at                 timestamp_ltz    default current_timestamp(),
  constraint PIPELINE_CONFIG_PK primary key (pipeline_id)
);
        """
    ).collect()
    return


# ================================
# 🤖 CORTEX COMPLETE SQL GENERATOR
# ================================

def extract_sql_from_text(text: str) -> str:
    t = (text or '').strip()
    # Strip code fences/backticks
    if t.startswith('```'):
        t = t.strip('`')
    # Heuristic: find first SELECT/WITH
    lower = t.lower()
    idx = lower.find('select')
    if idx == -1:
        idx = lower.find('with')
    if idx >= 0:
        t = t[idx:]
    # Single statement only
    if t.endswith(';'):
        t = t[:-1]
    return t.strip()

def generate_sql_with_complete(session: Session, database: str, schemas: list[str], user_prompt: str) -> tuple[bool, str, str]:
    """Simple Cortex COMPLETE call without schema introspection"""
    
    system_rules = (
        "Return a single SELECT or WITH query only. No semicolons. "
        "Do NOT use USE/SET or any DDL/DML (CREATE/ALTER/DROP/INSERT/UPDATE/DELETE/MERGE/TRUNCATE/CALL/GRANT/REVOKE/COPY). "
        "Use fully qualified identifiers: DB.SCHEMA.TABLE. "
        "Make realistic queries that would return actual data."
    )
    
    schema_context = f"Database: {database}, Schemas: {', '.join(schemas or [])}" if schemas else f"Database: {database}"
    
    full_prompt = f"""
{system_rules}

Context: {schema_context}

User request: {user_prompt}

Generate a SQL query that would answer this request using tables from the specified database/schemas.
"""
    
    try:
        res = session.sql(
            f"select snowflake.cortex.complete('mistral-large', $$ {full_prompt} $$) as c"
        ).collect()[0][0]
        sql_text = extract_sql_from_text(res)
        ok, err = validate_pipeline_sql(sql_text)
        if not ok:
            return False, '', f"Validation failed: {err}"
        return True, sql_text, ''
    except Exception as e:
        return False, '', f"Model call failed: {e}"

# ================================
# 🎭 MAIN APPLICATION
# ================================

def main():
    # Initialize session
    session = get_session()
    
    # Header
    st.markdown("""
    <div class="main-header">
        <h1>🤖 TechUp Analytics Copilot</h1>
        <p>Powered by Snowflake Cortex Analyst | Natural Language → Insights → Automated Pipelines</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Sidebar Configuration
    with st.sidebar:
        st.header("🎯 Data Source Configuration")
        
        # Source database selection
        databases = get_databases(session)
        if databases:
            source_db = st.selectbox("📊 Source Database", databases, key="source_db")
            st.session_state['source_database'] = source_db
            
            # Source schema selection (multiselect)
            source_schemas = get_schemas(session, source_db)
            if source_schemas:
                selected_schemas = st.multiselect("🗂️ Source Schemas", source_schemas, default=source_schemas[:1] if source_schemas else [], key="source_schemas")
                st.session_state['source_schemas'] = selected_schemas
                
                if selected_schemas:
                    st.success(f"✅ Ready to generate SQL from {source_db} with {len(selected_schemas)} schema(s)")
                else:
                    st.warning("Please select at least one source schema")
            else:
                st.warning("No schemas found in source database")
        else:
            st.error("No databases accessible")
        
        st.header("🎯 Dynamic Table Target")
        
        # Target database selection
        if databases:
            target_db = st.selectbox("📊 Target Database", databases, key="target_db")
            st.session_state['target_database'] = target_db
            
            # Target schema selection
            target_schemas = get_schemas(session, target_db)
            if target_schemas:
                target_schema = st.selectbox("🗂️ Target Schema", target_schemas, key="target_schema")
                st.session_state['target_schema'] = target_schema
                
                st.info(f"Dynamic Tables will be created in: **{target_db}.{target_schema}**")
            else:
                st.warning("No schemas found in target database")
        
        # Pipeline configuration
        st.header("⚙️ Pipeline Settings")
        default_warehouse = st.text_input("Default Warehouse", value=DEFAULT_WAREHOUSE)
        st.session_state['default_warehouse'] = default_warehouse
        
        # Show current context
        if st.session_state.get('source_database') and st.session_state.get('source_schemas'):
            with st.expander("📋 Current Context"):
                st.write(f"**Source:** {st.session_state['source_database']} → {', '.join(st.session_state['source_schemas'])}")
                if st.session_state.get('target_database') and st.session_state.get('target_schema'):
                    st.write(f"**Target:** {st.session_state['target_database']}.{st.session_state['target_schema']}")
    
    # Main chat interface
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.header("💬 Analytics Conversation")
        
        # Initialize chat history
        if 'messages' not in st.session_state:
            st.session_state.messages = []
        
        # Display chat history
        for message in st.session_state.messages:
            role = message["role"]
            content = message["content"]
            
            if role == "user":
                st.markdown(f"""
                <div class="chat-message user-message">
                    <strong>🧑 You:</strong> {content}
                </div>
                """, unsafe_allow_html=True)
            else:
                st.markdown(f"""
                <div class="chat-message assistant-message">
                    <strong>🤖 Analytics Copilot:</strong> {content}
                </div>
                """, unsafe_allow_html=True)
        
        # Chat input (Cortex COMPLETE) - compatible with older Streamlit
        st.subheader("💬 Ask for Data Analysis")
        user_input = st.text_input("Describe the data/insight you need:", placeholder="e.g., Show me sales trends by month", key="user_query")
        
        if st.button("🧠 Generate SQL", type="primary") and user_input:
            st.session_state.messages.append({"role": "user", "content": user_input})
            with st.spinner("🧠 Generating SQL..."):
                ok, sql_text, err = generate_sql_with_complete(
                    session=session,
                    database=st.session_state.get('source_database') or '',
                    schemas=st.session_state.get('source_schemas') or [],
                    user_prompt=user_input,
                )
            if not ok:
                st.error(err)
            else:
                st.session_state['last_sql'] = sql_text
                st.session_state['pending_sql'] = sql_text
                st.session_state.messages.append({"role": "assistant", "content": sql_text})
                st.success("✅ SQL generated! Check the results panel →")
    
    with col2:
        st.header("📊 Live Results")
        
        # Steward approval / overrides
        st.subheader("✅ Steward Approval")
        pending_sql = st.session_state.get('pending_sql')
        if pending_sql:
            ok, err = validate_pipeline_sql(pending_sql)
            if not ok:
                st.error(f"Validation failed: {err}")
            st.text_area("Proposed SQL (edit before approval)", value=pending_sql, key="pending_sql_editor", height=180)
            
            # Use target database/schema from sidebar
            target_db = st.session_state.get('target_database', 'DB')
            target_schema = st.session_state.get('target_schema', 'SCHEMA')
            target_dt_name = st.text_input("Target Dynamic Table", value=f"{target_db}.{target_schema}.APPROVED_DT")
            pipeline_name = st.text_input("Pipeline ID", value=f"approved_{datetime.now().strftime('%Y%m%d_%H%M')}" )
            lag_minutes = st.number_input("Lag minutes", min_value=1, max_value=1440, value=10)
            warehouse = st.text_input("Warehouse", value=st.session_state.get('default_warehouse', DEFAULT_WAREHOUSE))
            approve_cols = st.columns(3)
            with approve_cols[0]:
                if st.button("Approve & Create DT", type="primary"):
                    new_sql = st.session_state.get('pending_sql_editor')
                    ok2, err2 = validate_pipeline_sql(new_sql)
                    if not ok2:
                        st.error(f"Validation failed: {err2}")
                    else:
                        # Directly create the Dynamic Table (no SP/Task)
                        dt_sql = f"""
create or replace dynamic table {target_dt_name}
warehouse = {warehouse}
lag = '{int(lag_minutes)} minutes'
as
{new_sql}
"""
                        try:
                            session.sql(dt_sql).collect()
                            st.success("Dynamic Table created.")
                            st.session_state.pop('pending_sql', None)
                            st.session_state['last_sql'] = new_sql
                        except Exception as e:
                            st.error(f"DT creation failed: {e}")
            with approve_cols[1]:
                if st.button("Clear Proposal"):
                    st.session_state.pop('pending_sql', None)
                    st.info("Proposal cleared")
            with approve_cols[2]:
                if st.button("(Optional) Insert to PIPELINE_CONFIG"):
                    if create_pipeline_with_overrides(session, new_sql, pipeline_name, target_dt_name, int(lag_minutes), warehouse):
                        st.success("Inserted into PIPELINE_CONFIG (PENDING)")

        # Execute SQL and show results
        if 'last_sql' in st.session_state:
            try:
                with st.spinner("Executing analysis..."):
                    results = session.sql(st.session_state['last_sql']).collect()
                    df = pd.DataFrame(results)
                    
                if not df.empty:
                    # Show metrics if numeric data
                    numeric_cols = df.select_dtypes(include=['number']).columns
                    if len(numeric_cols) > 0:
                        st.subheader("📈 Key Metrics")
                        metrics_cols = st.columns(min(len(numeric_cols), 3))
                        for i, col in enumerate(numeric_cols[:3]):
                            with metrics_cols[i]:
                                st.metric(
                                    label=col.replace('_', ' ').title(),
                                    value=f"{df[col].sum():,.0f}" if df[col].dtype in ['int64', 'float64'] else df[col].iloc[0]
                                )
                    
                    # Create visualization
                    st.subheader("📊 Visualization")
                    fig = create_visualization(df)
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Show raw data
                    with st.expander("🔍 Raw Data"):
                        st.dataframe(df, use_container_width=True)
                    
                    # Pipeline creation
                    st.subheader("🚀 Create Pipeline")
                    col_a, col_b = st.columns(2)
                    with col_a:
                        pipeline_name = st.text_input("Pipeline Name", value=f"analysis_{datetime.now().strftime('%Y%m%d_%H%M')}")
                    with col_b:
                        target_db = st.session_state.get('target_database', 'DB')
                        target_schema = st.session_state.get('target_schema', 'SCHEMA')
                        target_table = st.text_input("Target Table", value=f"{target_db}.{target_schema}.{pipeline_name.upper()}_DT")
                    
                    if st.button("🎯 Create Dynamic Table Pipeline", type="primary"):
                        if create_pipeline_from_analysis(session, st.session_state['last_sql'], pipeline_name, target_table):
                            st.success("🎉 Pipeline created! It will be processed by the orchestrator.")
                            st.balloons()
                else:
                    st.info("No data returned from query")
                    
            except Exception as e:
                st.error(f"Query execution failed: {str(e)}")
        
        # Suggestions
        if 'suggestions' in st.session_state:
            st.subheader("💡 Suggested Questions")
            for suggestion in st.session_state['suggestions']:
                if st.button(suggestion, key=f"sug_{hash(suggestion)}"):
                    # Set the suggestion in the input field
                    st.session_state['user_query'] = suggestion
                    st.info(f"Suggestion added to input: {suggestion}")

if __name__ == "__main__":
    main()
