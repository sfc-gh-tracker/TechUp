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
import yaml

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

def get_tables(session: Session, database: str, schema: str) -> List[Dict]:
    """Get tables with metadata for semantic model"""
    try:
        # Set context
        session.sql(f"USE DATABASE {database}").collect()
        session.sql(f"USE SCHEMA {schema}").collect()
        
        tables_info = []
        
        # Try different approaches to find tables
        table_queries = [
            "SHOW TABLES",
            f"SHOW TABLES IN SCHEMA {database}.{schema}",
            f"SELECT table_name FROM {database}.information_schema.tables WHERE table_schema = '{schema}' AND table_type = 'BASE TABLE'"
        ]
        
        tables = []
        for query in table_queries:
            try:
                result = session.sql(query).collect()
                if result:
                    if 'name' in result[0].as_dict():
                        tables = [{'name': row['name']} for row in result]
                    elif 'TABLE_NAME' in result[0].as_dict():
                        tables = [{'name': row['TABLE_NAME']} for row in result]
                    break
            except Exception as e:
                st.write(f"Debug: Query failed: {query} - {e}")
                continue
        
        if not tables:
            st.warning(f"No tables found in {database}.{schema}. This could be due to:")
            st.write("- Schema is empty")
            st.write("- Insufficient permissions")
            st.write("- Schema contains only views/external tables")
            return []
        
        st.info(f"Found {len(tables)} table(s) in {database}.{schema}")
        
        # Get table metadata
        for table in tables[:10]:  # Limit to 10 tables for demo
            table_name = table['name']
            try:
                columns = session.sql(f"DESCRIBE TABLE {table_name}").collect()
                table_info = {
                    'name': table_name,
                    'columns': [{'name': col['name'], 'type': col['type']} for col in columns[:20]]
                }
                tables_info.append(table_info)
                st.write(f"✅ Analyzed table: {table_name} ({len(table_info['columns'])} columns)")
            except Exception as e:
                st.write(f"⚠️ Could not analyze table {table_name}: {e}")
                # Add table anyway with minimal info
                tables_info.append({
                    'name': table_name,
                    'columns': [{'name': 'id', 'type': 'NUMBER'}]  # Fallback
                })
                continue
                
        return tables_info
        
    except Exception as e:
        st.error(f"Error accessing {database}.{schema}: {e}")
        st.write("**Troubleshooting tips:**")
        st.write("- Verify the database and schema exist")
        st.write("- Check you have USAGE privilege on the database and schema")
        st.write("- Ensure you have SELECT privilege on tables")
        st.write("- Try a different schema with known data")
        return []

def create_sample_data(session: Session):
    """Create sample tables for demo purposes"""
    try:
        # Create a demo schema
        session.sql("CREATE SCHEMA IF NOT EXISTS DEMO").collect()
        session.sql("USE SCHEMA DEMO").collect()
        
        # Create sample customers table
        session.sql("""
        CREATE OR REPLACE TABLE CUSTOMERS AS
        SELECT 
            ROW_NUMBER() OVER (ORDER BY RANDOM()) as customer_id,
            'Customer_' || customer_id as customer_name,
            CASE WHEN RANDOM() < 0.3 THEN 'Premium' 
                 WHEN RANDOM() < 0.7 THEN 'Standard' 
                 ELSE 'Basic' END as customer_type,
            DATEADD(day, -RANDOM()*365, CURRENT_DATE) as created_date,
            ROUND(RANDOM() * 10000, 2) as lifetime_value
        FROM TABLE(GENERATOR(ROWCOUNT => 100))
        """).collect()
        
        # Create sample orders table
        session.sql("""
        CREATE OR REPLACE TABLE ORDERS AS
        SELECT 
            ROW_NUMBER() OVER (ORDER BY RANDOM()) as order_id,
            MOD(ROW_NUMBER() OVER (ORDER BY RANDOM()), 100) + 1 as customer_id,
            DATEADD(day, -RANDOM()*180, CURRENT_DATE) as order_date,
            ROUND(RANDOM() * 1000 + 50, 2) as order_amount,
            CASE WHEN RANDOM() < 0.8 THEN 'Completed' 
                 WHEN RANDOM() < 0.95 THEN 'Pending' 
                 ELSE 'Cancelled' END as status
        FROM TABLE(GENERATOR(ROWCOUNT => 500))
        """).collect()
        
        # Create sample products table  
        session.sql("""
        CREATE OR REPLACE TABLE PRODUCTS AS
        SELECT 
            ROW_NUMBER() OVER (ORDER BY RANDOM()) as product_id,
            'Product_' || product_id as product_name,
            CASE WHEN RANDOM() < 0.4 THEN 'Electronics' 
                 WHEN RANDOM() < 0.7 THEN 'Clothing' 
                 ELSE 'Home & Garden' END as category,
            ROUND(RANDOM() * 500 + 10, 2) as price,
            ROUND(RANDOM() * 1000) as stock_quantity
        FROM TABLE(GENERATOR(ROWCOUNT => 50))
        """).collect()
        
        st.info("✅ Created sample tables: CUSTOMERS, ORDERS, PRODUCTS in DEMO schema")
        
    except Exception as e:
        st.error(f"Failed to create sample data: {e}")

# ================================
# 🎯 SEMANTIC MODEL GENERATION
# ================================

def generate_semantic_model(database: str, schema: str, tables: List[Dict]) -> str:
    """Generate a semantic model YAML for Cortex Analyst"""
    
    model = {
        'name': f'{database}_{schema}_model',
        'description': f'Semantic model for {database}.{schema} - Auto-generated for TechUp Analytics Copilot',
        'tables': []
    }
    
    for table in tables:
        table_def = {
            'name': table['name'],
            'description': f'Table containing {table["name"].lower().replace("_", " ")} data',
            'base_table': {
                'database': database,
                'schema': schema,
                'table': table['name']
            },
            'dimensions': [],
            'measures': []
        }
        
        # Auto-generate dimensions and measures based on column types
        for col in table['columns']:
            col_name = col['name']
            col_type = col['type'].upper()
            
            if any(keyword in col_name.upper() for keyword in ['ID', 'KEY', 'CODE', 'NAME', 'TYPE', 'STATUS', 'CATEGORY']):
                # Dimension
                table_def['dimensions'].append({
                    'name': col_name.lower(),
                    'expr': col_name,
                    'description': f'{col_name.replace("_", " ").title()} dimension',
                    'data_type': 'TEXT' if 'VARCHAR' in col_type or 'TEXT' in col_type else 'NUMBER'
                })
            elif any(keyword in col_name.upper() for keyword in ['AMOUNT', 'PRICE', 'COST', 'REVENUE', 'TOTAL', 'COUNT', 'QUANTITY']):
                # Measure
                table_def['measures'].append({
                    'name': f'total_{col_name.lower()}',
                    'expr': f'SUM({col_name})',
                    'description': f'Total {col_name.replace("_", " ").lower()}',
                    'data_type': 'NUMBER'
                })
            elif 'DATE' in col_type or 'TIMESTAMP' in col_type:
                # Time dimension
                table_def['dimensions'].append({
                    'name': f'{col_name.lower()}_date',
                    'expr': f'DATE({col_name})',
                    'description': f'{col_name.replace("_", " ").title()} date',
                    'data_type': 'DATE'
                })
        
        # Add default measures if none found
        if not table_def['measures']:
            table_def['measures'].append({
                'name': 'record_count',
                'expr': 'COUNT(*)',
                'description': f'Total number of records in {table["name"]}',
                'data_type': 'NUMBER'
            })
        
        model['tables'].append(table_def)
    
    return yaml.dump(model, default_flow_style=False, sort_keys=False)

# ================================
# 🤖 CORTEX ANALYST INTEGRATION
# ================================

def call_cortex_analyst(session: Session, message: str, semantic_model_yaml: str, conversation_history: List[Dict] = None) -> Dict:
    """Call Cortex Analyst API with semantic model"""
    try:
        # For demo purposes, we'll simulate the Cortex Analyst response
        # In production, you'd make the actual REST API call to Cortex Analyst
        return simulate_cortex_analyst_response(message, semantic_model_yaml)
        
    except Exception as e:
        return {
            'error': f'Failed to call Cortex Analyst: {str(e)}',
            'message': 'Please check your permissions and semantic model.'
        }

def simulate_cortex_analyst_response(message: str, semantic_model: str) -> Dict:
    """Simulate Cortex Analyst response for demo"""
    
    # Parse the semantic model to understand available data
    try:
        model_data = yaml.safe_load(semantic_model)
        tables = model_data.get('tables', [])
    except:
        tables = []
    
    # Generate contextual SQL based on the message
    if any(word in message.lower() for word in ['total', 'sum', 'revenue', 'sales']):
        # Revenue/sales query
        if tables:
            table_name = tables[0]['name']
            measures = tables[0].get('measures', [])
            revenue_measure = next((m for m in measures if 'amount' in m['name'] or 'revenue' in m['name']), measures[0] if measures else None)
            
            if revenue_measure:
                sql = f"""
SELECT 
    DATE_TRUNC('month', created_date) as month,
    {revenue_measure['expr']} as total_revenue
FROM {table_name}
WHERE created_date >= CURRENT_DATE - 365
GROUP BY DATE_TRUNC('month', created_date)
ORDER BY month DESC
LIMIT 12
"""
            else:
                sql = f"SELECT COUNT(*) as total_records FROM {table_name}"
        else:
            sql = "SELECT CURRENT_DATE as today, 'No tables available' as message"
            
        response_text = f"Here's the revenue analysis you requested. I found patterns showing seasonal trends in your data."
        
    elif any(word in message.lower() for word in ['top', 'best', 'highest']):
        # Top performers query
        if tables:
            table_name = tables[0]['name']
            dimensions = tables[0].get('dimensions', [])
            measures = tables[0].get('measures', [])
            
            name_dim = next((d for d in dimensions if 'name' in d['name']), dimensions[0] if dimensions else None)
            value_measure = next((m for m in measures if 'total' in m['name']), measures[0] if measures else None)
            
            if name_dim and value_measure:
                sql = f"""
SELECT 
    {name_dim['expr']} as name,
    {value_measure['expr']} as value
FROM {table_name}
GROUP BY {name_dim['expr']}
ORDER BY {value_measure['expr']} DESC
LIMIT 10
"""
            else:
                sql = f"SELECT * FROM {table_name} LIMIT 10"
        else:
            sql = "SELECT 'No data' as result"
            
        response_text = f"I've identified the top performers based on your criteria. These results show clear leaders in your dataset."
        
    else:
        # General exploration query
        if tables:
            table_name = tables[0]['name']
            sql = f"""
SELECT *
FROM {table_name}
LIMIT 100
"""
        else:
            sql = "SELECT CURRENT_TIMESTAMP as timestamp, 'Welcome to TechUp Analytics!' as message"
            
        response_text = f"Here's an overview of your data. I can help you dive deeper into specific areas you're interested in."
    
    return {
        'message': {
            'role': 'assistant',
            'content': [
                {'type': 'text', 'text': response_text},
                {'type': 'suggestions', 'suggestions': [
                    'Show me monthly trends',
                    'What are the top categories?',
                    'Analyze performance by region',
                    'Create a pipeline for this analysis'
                ]}
            ]
        },
        'request_id': f'req_{datetime.now().strftime("%Y%m%d_%H%M%S")}',
        'sql': sql.strip()
    }

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
        st.header("🎯 Configuration")
        
        # Database selection
        databases = get_databases(session)
        if databases:
            selected_db = st.selectbox("📊 Database", databases)
            st.session_state['database'] = selected_db
            
            # Schema selection (multiselect)
            schemas = get_schemas(session, selected_db)
            if schemas:
                selected_schemas = st.multiselect("🗂️ Schemas", schemas, default=schemas[:1] if schemas else [])
                st.session_state['schemas'] = selected_schemas
                
                if selected_schemas:
                    st.success(f"✅ Ready to generate SQL for {selected_db} with {len(selected_schemas)} schema(s)")
                else:
                    st.warning("Please select at least one schema")
            else:
                st.warning("No schemas found in selected database")
        else:
            st.error("No databases accessible")
        
        # Pipeline configuration
        st.header("⚙️ Pipeline Settings")
        pipeline_warehouse = st.text_input("Warehouse", value=DEFAULT_WAREHOUSE)
        
        # Demo data option
        st.header("🎯 Demo Mode")
        if st.button("🚀 Create Sample Data", help="Creates sample tables for testing"):
            with st.spinner("Creating sample data..."):
                create_sample_data(session)
                st.success("Sample data created! Refresh and try again.")
        
        # Show current context
        if st.session_state.get('database') and st.session_state.get('schemas'):
            with st.expander("📋 Current Context"):
                st.write(f"**Database:** {st.session_state['database']}")
                st.write(f"**Schemas:** {', '.join(st.session_state['schemas'])}")
    
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
        
        # Chat input (Cortex COMPLETE)
        user_input = st.chat_input("Describe the data/insight you need…")
        
        if user_input:
            st.session_state.messages.append({"role": "user", "content": user_input})
            with st.spinner("🧠 Generating SQL..."):
                ok, sql_text, err = generate_sql_with_complete(
                    session=session,
                    database=st.session_state.get('database') or '',
                    schemas=st.session_state.get('schemas') or [],
                    user_prompt=user_input,
                )
            if not ok:
                st.error(err)
            else:
                st.session_state['last_sql'] = sql_text
                st.session_state['pending_sql'] = sql_text
                st.session_state.messages.append({"role": "assistant", "content": sql_text})
            st.rerun()
    
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
            default_schema = st.session_state.get('schemas', ['SCHEMA'])[0] if st.session_state.get('schemas') else 'SCHEMA'
            target_dt_name = st.text_input("Target Dynamic Table (DB.SCHEMA.NAME)", value=f"{st.session_state.get('database', 'DB')}.{default_schema}.APPROVED_DT")
            pipeline_name = st.text_input("Pipeline ID", value=f"approved_{datetime.now().strftime('%Y%m%d_%H%M')}" )
            lag_minutes = st.number_input("Lag minutes", min_value=1, max_value=1440, value=10)
            warehouse = st.text_input("Warehouse", value=DEFAULT_WAREHOUSE)
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
                if st.button("Reject Proposal"):
                    st.session_state.pop('pending_sql', None)
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
                        default_schema = st.session_state.get('schemas', ['SCHEMA'])[0] if st.session_state.get('schemas') else 'SCHEMA'
                        target_table = st.text_input("Target Table", value=f"{st.session_state.get('database', 'DB')}.{default_schema}.{pipeline_name.upper()}_DT")
                    
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
                    # Simulate clicking the suggestion
                    st.session_state.messages.append({"role": "user", "content": suggestion})
                    st.rerun()

if __name__ == "__main__":
    main()
