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

def _simple_sql_auto_repair(sql_text: str) -> str:
    """Heuristically fix common LLM SQL formatting issues (dangling parens, code fences)."""
    if not sql_text:
        return sql_text
    # Drop code-fence cruft and stray closing paren lines
    filtered_lines = []
    for raw in sql_text.splitlines():
        s = raw.strip()
        if not s:
            filtered_lines.append(raw)
            continue
        if s in ('```', '```sql', 'sql'):
            continue
        if s == ')' or s.startswith('),') or s.startswith(');') or s.startswith(') '):
            # Likely a dangling closing paren from truncated CTE or subquery
            continue
        filtered_lines.append(raw)
    s = "\n".join(filtered_lines).strip()
    # Remove leading unmatched ')' characters at document start
    while s and s.lstrip().startswith(')'):
        # Drop first line if it starts with ')'
        first_newline = s.find('\n')
        if first_newline == -1:
            s = s.lstrip(') ').lstrip()
            break
        first_line = s[:first_newline]
        if first_line.lstrip().startswith(')'):
            s = s[first_newline+1:]
        else:
            break
    # Trim unmatched trailing ')'
    open_paren = s.count('(')
    close_paren = s.count(')')
    while close_paren > open_paren and s.endswith(')'):
        s = s[:-1].rstrip()
        close_paren -= 1
    return s

def _strip_narrative_lines(sql_text: str) -> str:
    """Remove obvious non-SQL narrative lines the model may include inside code."""
    if not sql_text:
        return sql_text
    allowed_starts = (
        'with', 'select', 'from', 'where', 'group', 'having', 'order', 'limit', 'qualify',
        'join', 'inner', 'left', 'right', 'full', 'cross', 'on', 'union', 'except',
        'intersect', 'window', 'using', 'as', 'values', 'lateral', 'apply', 'pivot', 'unpivot'
    )
    out = []
    for raw in sql_text.splitlines():
        s = raw.strip()
        if not s:
            out.append(raw)
            continue
        if s.startswith('--') or s.startswith('/*'):
            out.append(raw)
            continue
        first = s.split()[0].lower()
        if s.startswith('(') or first in allowed_starts:
            out.append(raw)
            continue
        # Drop narrative lines like "The following query ..."
        if first.isalpha():
            continue
        out.append(raw)
    return '\n'.join(out)

# ================================
# 📚 Known tables/columns context
# ================================

def _sql_literal(value: str) -> str:
    v = '' if value is None else str(value)
    return "'" + v.replace("'", "''") + "'"

def build_known_tables_context(session: Session, database: str, schemas: list[str], max_tables: int = 40, max_columns: int = 10) -> str:
    """Return a compact text card of existing tables/columns in selected schemas (no USE statements)."""
    try:
        if not database or not schemas:
            return ''
        schema_list = ','.join(_sql_literal(s) for s in schemas)
        tables_q = f"""
select table_schema, table_name
from {database}.information_schema.tables
where table_schema in ({schema_list})
  and table_type in ('BASE TABLE','VIEW')
order by table_schema, table_name
limit {max_tables}
"""
        tables = session.sql(tables_q).collect()
        if not tables:
            return ''
        # Build set for columns query
        table_keys = [(t['TABLE_SCHEMA'], t['TABLE_NAME']) for t in tables]
        # Construct IN lists
        tn_list = ','.join(_sql_literal(tn) for _, tn in table_keys)
        cols_q = f"""
select table_schema, table_name, column_name, data_type, ordinal_position
from {database}.information_schema.columns
where table_schema in ({schema_list})
  and table_name in ({tn_list})
order by table_schema, table_name, ordinal_position
"""
        cols = session.sql(cols_q).collect()
        cols_map: dict[tuple[str,str], list[tuple[str,str]]] = {}
        for c in cols:
            key = (c['TABLE_SCHEMA'], c['TABLE_NAME'])
            lst = cols_map.setdefault(key, [])
            if len(lst) < max_columns:
                lst.append((c['COLUMN_NAME'], c['DATA_TYPE']))
        # Build card
        lines: list[str] = []
        lines.append(f"Known tables in {database} for schemas: {', '.join(schemas)}")
        for sch, tbl in table_keys:
            col_list = cols_map.get((sch, tbl), [])
            col_txt = ', '.join(f"{cn}({dt})" for cn, dt in col_list)
            lines.append(f"  {database}.{sch}.{tbl}: {col_txt}")
        return '\n'.join(lines)
    except Exception:
        return ''

def extract_sql_from_text(text: str) -> str:
    t = (text or '').strip()
    # Strip common code fences/backticks
    if t.startswith('```'):
        t = t.strip('`')
    # Keep only content from first SELECT/WITH
    lower = t.lower()
    start_idx = lower.find('select')
    if start_idx == -1:
        start_idx = lower.find('with')
    if start_idx >= 0:
        t = t[start_idx:]
    # Truncate at first top-level semicolon (paren-balanced, not inside quotes)
    def _truncate_top_level(sql: str) -> str:
        depth = 0
        in_single = False
        in_double = False
        prev_char = ''
        for i, ch in enumerate(sql):
            if ch == "'" and not in_double and prev_char != '\\':
                in_single = not in_single
            elif ch == '"' and not in_single and prev_char != '\\':
                in_double = not in_double
            elif not in_single and not in_double:
                if ch == '(':
                    depth += 1
                elif ch == ')':
                    depth -= 1
                elif ch == ';' and depth <= 0:
                    return sql[:i]
            prev_char = ch
        return sql
    def _reduce_to_single_top_level_select(sql: str) -> str:
        s = sql
        lower = s.lower()
        n = len(s)
        in_single = False
        in_double = False
        depth = 0
        prev = ''
        # find first top-level WITH or SELECT
        start = 0
        i = 0
        while i < n:
            ch = s[i]
            if ch == "'" and not in_double and prev != '\\':
                in_single = not in_single
            elif ch == '"' and not in_single and prev != '\\':
                in_double = not in_double
            elif not in_single and not in_double:
                if ch == '(':
                    depth += 1
                elif ch == ')':
                    depth -= 1
                elif depth == 0:
                    # check for tokens at top-level
                    if lower.startswith('with', i) or lower.startswith('select', i):
                        start = i
                        break
            prev = ch
            i += 1
        # find second top-level SELECT after start
        i = start + 6
        prev = ''
        in_single = False
        in_double = False
        depth = 0
        second_select_pos = -1
        while i < n:
            ch = s[i]
            if ch == "'" and not in_double and prev != '\\':
                in_single = not in_single
            elif ch == '"' and not in_single and prev != '\\':
                in_double = not in_double
            elif not in_single and not in_double:
                if ch == '(':
                    depth += 1
                elif ch == ')':
                    depth -= 1
                elif depth == 0:
                    if lower.startswith('select', i):
                        second_select_pos = i
                        break
            prev = ch
            i += 1
        if second_select_pos != -1:
            s = s[start:second_select_pos]
    else:
            s = s[start:]
        return s

    t = _truncate_top_level(t)
    t = _reduce_to_single_top_level_select(t)
    t = _strip_narrative_lines(t)
    return _simple_sql_auto_repair(t.strip())

def generate_sql_with_complete(
    session: Session,
    database: str,
    schemas: list[str],
    user_prompt: str,
    attempt: int = 1,
    error_context: str | None = None,
    previous_sql: str | None = None,
) -> tuple[bool, str, str]:
    """Enhanced Cortex COMPLETE call with retry-aware prompting and error repair."""
    
    system_rules = (
        "Return a single SELECT or WITH query only. No semicolons. "
        "Do NOT use USE/SET or any DDL/DML (CREATE/ALTER/DROP/INSERT/UPDATE/DELETE/MERGE/TRUNCATE/CALL/GRANT/REVOKE/COPY). "
        "Use fully qualified identifiers: DATABASE.SCHEMA.TABLE. "
        "Generate queries that will return actual data with realistic WHERE clauses and filters. "
        "Avoid queries that might return empty results. "
        "Ensure SQL syntax is valid and parentheses are balanced. Do not output stray ')' lines or code fences."
    )
    
    schema_context = f"Database: {database}, Schemas: {', '.join(schemas or [])}" if schemas else f"Database: {database}"
    known_card = build_known_tables_context(session, database, schemas)
    
    # Add attempt-specific guidance and error repair context
    attempt_guidance = ""
    if attempt > 1:
        attempt_guidance = f"""
This is attempt #{attempt}. Make a corrected query that is more likely to return rows.
If previous SQL or error info is included, fix the issue and output a single corrected query.
"""

    repair_block = ""
    if error_context or previous_sql:
        repair_lines = ["Context for repair:"]
        if previous_sql:
            repair_lines.append("Previous SQL:\n" + previous_sql)
        if error_context:
            repair_lines.append("Error observed:\n" + error_context)
        repair_lines.append("Requirements: \n- Fix the issue \n- Prefer simpler filters \n- Ensure it returns data \n- Output one SELECT/WITH query only")
        repair_block = "\n\n" + "\n".join(repair_lines)
    
            full_prompt = f"""
{system_rules}

Context: {schema_context}
Known objects (subset):
{known_card}

{attempt_guidance}{repair_block}

User request: {user_prompt}

Generate a working SQL query that will return actual data. Use realistic filters and common table/column names.
Only output SQL code. Do not include narrative sentences.
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
            
            # Source schema selection (multiselect)
            source_schemas = get_schemas(session, source_db)
            if source_schemas:
                selected_schemas = st.multiselect("🗂️ Source Schemas", source_schemas, default=source_schemas[:1] if source_schemas else [], key="source_schemas")
                
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
            
            # Target schema selection
            target_schemas = get_schemas(session, target_db)
            if target_schemas:
                target_schema = st.selectbox("🗂️ Target Schema", target_schemas, key="target_schema")
                
                st.info(f"Dynamic Tables will be created in: **{target_db}.{target_schema}**")
            else:
                st.warning("No schemas found in target database")
        
        # Pipeline configuration
        st.header("⚙️ Pipeline Settings")
        default_warehouse = st.text_input("Default Warehouse", value=DEFAULT_WAREHOUSE, key="default_warehouse")
        
        # Show current context
        if st.session_state.get('source_db') and st.session_state.get('source_schemas'):
            with st.expander("📋 Current Context"):
                st.write(f"**Source:** {st.session_state['source_db']} → {', '.join(st.session_state['source_schemas'])}")
                if st.session_state.get('target_db') and st.session_state.get('target_schema'):
                    st.write(f"**Target:** {st.session_state['target_db']}.{st.session_state['target_schema']}")
    
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
        user_input = st.text_area(
            "Describe the data/insight you need:",
            placeholder="e.g., Show me sales trends by month; include last 12 months",
            key="user_query",
            height=120
        )
        
        if st.button("🧠 Generate SQL", type="primary") and user_input:
            st.session_state.messages.append({"role": "user", "content": user_input})
            
            max_attempts = 3
            attempt = 0
            success = False
            
            last_sql = None
            last_error = None
            while attempt < max_attempts and not success:
                attempt += 1
                with st.spinner(f"🧠 Generating SQL (attempt {attempt}/{max_attempts})..."):
                    ok, sql_text, err = generate_sql_with_complete(
                session=session,
                        database=st.session_state.get('source_db') or '',
                        schemas=st.session_state.get('source_schemas') or [],
                        user_prompt=user_input,
                        attempt=attempt,
                        error_context=last_error,
                        previous_sql=last_sql
                    )

                # Always show the generated SQL for this attempt (even if invalid)
                shown_sql = _simple_sql_auto_repair(sql_text or '') if ok else (sql_text or '')
                if shown_sql.strip():
                    st.subheader(f"Attempt {attempt} - Generated SQL")
                    st.code(shown_sql, language='sql')
                    st.session_state['last_sql'] = shown_sql
                    st.session_state['pending_sql'] = shown_sql
                
                if not ok:
                    st.warning(f"Attempt {attempt} failed validation: {err}")
                    last_error = err
                    last_sql = shown_sql
                    continue
                
                # Test the generated SQL
                with st.spinner(f"🔍 Testing SQL (attempt {attempt}/{max_attempts})..."):
                    try:
                        # Last-mile heuristic repair before execution
                        candidate_sql = _simple_sql_auto_repair(_strip_narrative_lines(sql_text))
                        test_results = session.sql(candidate_sql).collect()
                        if len(test_results) == 0:
                            st.warning(f"Attempt {attempt}: SQL executed but returned no rows")
                            last_sql = sql_text
                            last_error = "The query returned zero rows. Broaden filters, adjust joins, or choose different tables."
                            continue
                        
                        # Success! SQL works and returns data
                        st.session_state['last_sql'] = candidate_sql
                        st.session_state['pending_sql'] = candidate_sql
                        st.session_state.messages.append({"role": "assistant", "content": candidate_sql})
                        
                        # Show preview of first 3 records
                        preview_df = pd.DataFrame(test_results[:3])
                        st.success(f"✅ SQL generated and tested successfully! Returns {len(test_results)} rows")
                        st.subheader("📋 Preview (First 3 Records)")
                        st.dataframe(preview_df, use_container_width=True)
                        
                        success = True
                        
        except Exception as e:
                        err_msg = str(e)
                        st.warning(f"Attempt {attempt}: SQL failed to execute - {err_msg}")
                        # Show the exact SQL we attempted to run
                        st.subheader(f"Attempt {attempt} - Executed SQL")
                        st.code(candidate_sql, language='sql')
                        # Provide structured error summary to model
                        last_sql = candidate_sql
                        last_error = (
                            "Execution failed. Error summary: " + err_msg +
                            "\nCommon causes: unbalanced parentheses at line starts, missing FROM, or truncated WITH CTE."
                        )
                        continue
            
            if not success:
                st.error(f"❌ Failed to generate working SQL after {max_attempts} attempts. Please try a different prompt or check your database/schema selections.")
                st.info("💡 **Tips for better results:**")
                st.write("- Be more specific about the data you want")
                st.write("- Mention specific table names if you know them")
                st.write("- Try simpler queries first")
                st.write("- Check that your selected schemas contain the relevant data")
                if last_sql:
                    st.subheader("Last Generated SQL")
                    st.code(last_sql, language='sql')
    
    with col2:
        st.header("ℹ️ Status & Tips")
        st.write("Generated SQL and preview will appear on the left after a successful run.")
        
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
