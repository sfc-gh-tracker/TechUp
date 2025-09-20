import streamlit as st
from typing import List
from snowflake.snowpark import Session
from snowflake.snowpark.functions import call_function
from snowflake_utils import (
    get_session,
    list_tables,
    fetch_schema_card,
    is_single_select,
    enforce_read_only,
    preview_query,
    explain_query,
    insert_pipeline_config,
)
from config import DEFAULT_WAREHOUSE, ALLOWED_TABLES, PREVIEW_LIMIT, CORTEX_MODEL, FEW_SHOTS

st.set_page_config(page_title="Pipeline Factory", layout="wide")

@st.cache_resource(show_spinner=False)
def _get_session() -> Session:
    return get_session()

session = _get_session()

st.title("Prompt → SQL → Dynamic Table")

with st.expander("Scope & Options", expanded=True):
    st.caption("Choose tables to ground the model. Fewer tables = better accuracy.")

    col_l, col_r = st.columns(2)
    with col_l:
        db_input = st.text_input("Database (optional)", placeholder="LOGISTICS_DW").strip().upper()
    with col_r:
        schema_input = st.text_input("Schema (optional)", placeholder="FLATTENED").strip().upper()

    allowed_tables: List[str] = []
    if db_input and schema_input:
        try:
            rows = session.sql(
                f"select table_name from {db_input}.information_schema.tables where table_schema = '{schema_input}' and table_type in ('BASE TABLE','VIEW') order by table_name"
            ).collect()
            available = [f"{db_input}.{schema_input}.{r['TABLE_NAME']}" for r in rows]
            allowed_tables = st.multiselect(
                "Allowed tables (DB.SCHEMA.TABLE)",
                options=available,
                default=[],
            )
        except Exception as e:
            st.error(f"Failed to list tables: {e}")
            allowed_tables = []
    else:
        # Fallback: manual entry
        allowed_tables_input = st.text_input(
            "Allowed tables (comma-separated, fully qualified DB.SCHEMA.TABLE)",
            value=", ".join(ALLOWED_TABLES) if ALLOWED_TABLES else "",
            placeholder="RAW.SALES.ORDERS, RAW.CRM.CUSTOMERS",
        )
        allowed_tables = [t.strip() for t in allowed_tables_input.split(",") if t.strip()]

    allowed_tables = [t.upper() for t in allowed_tables]

    default_wh = st.text_input("Warehouse", value=DEFAULT_WAREHOUSE).upper()
    target_dt_name = st.text_input("Target Dynamic Table name (DB.SCHEMA.NAME)").upper()
    lag_minutes = st.number_input("Lag (minutes)", min_value=1, max_value=1440, value=10)
    pipeline_id = st.text_input("Pipeline ID", placeholder="ORDERS_COMPLETE_NLP").upper()

st.subheader("Describe the data")
prompt = st.text_area("Prompt", height=140, placeholder="Show the latest order per customer in the last 30 days")

if st.button("Generate SQL with Cortex", type="primary"):
    if not allowed_tables:
        st.error("Please provide at least one allowed table.")
    elif not prompt.strip():
        st.error("Please enter a prompt.")
    else:
        with st.spinner("Calling Cortex..."):
            schema_card = fetch_schema_card(session, allowed_tables)
            system = "\n".join(FEW_SHOTS)
            full_prompt = f"""
You are a Snowflake SQL assistant.
Only output a single SELECT query.
Use fully qualified identifiers. Do not include comments or extra text.
You may only reference these tables:
{schema_card}

User request:
{prompt}
"""
            # Call Cortex via SQL function COMPLETE
            res = session.sql(
                f"select snowflake.cortex.complete('{CORTEX_MODEL}', $$ {system}\n\n{full_prompt} $$) as c"
            ).collect()[0][0]
            generated_sql = res.strip().strip('`')

        st.code(generated_sql, language="sql")
        st.session_state["generated_sql"] = generated_sql
        st.success("SQL generated. Validate and preview below.")

if "generated_sql" in st.session_state:
    sql_text = st.session_state["generated_sql"]

    st.subheader("Validation")
    col1, col2 = st.columns(2)
    with col1:
        is_select = is_single_select(sql_text)
        is_ro = enforce_read_only(sql_text)
        st.write(f"Single SELECT: {'✅' if is_select else '❌'}")
        st.write(f"Read-only: {'✅' if is_ro else '❌'}")
    with col2:
        try:
            plan = explain_query(session, sql_text)
            st.text_area("EXPLAIN USING TEXT", plan, height=180)
            explain_ok = True
        except Exception as e:
            st.error(f"Explain failed: {e}")
            explain_ok = False

    st.subheader("Preview")
    preview_ok = False
    if is_select and is_ro and explain_ok:
        try:
            rows = preview_query(session, sql_text, limit=PREVIEW_LIMIT)
            st.dataframe([row.as_dict() for row in rows], use_container_width=True)
            preview_ok = True
        except Exception as e:
            st.error(f"Preview failed: {e}")

    st.subheader("Create Pipeline")
    if not target_dt_name:
        st.info("Enter a target Dynamic Table name above.")
    can_create = is_select and is_ro and explain_ok and preview_ok and bool(target_dt_name) and bool(pipeline_id)

    if st.button("Insert into PIPELINE_CONFIG (PENDING)", disabled=not can_create):
        try:
            # Use first allowed table as source hint; the SP will use the full SELECT
            source_hint = allowed_tables[0] if allowed_tables else "PUBLIC.DUAL"
            insert_pipeline_config(
                session=session,
                pipeline_id=pipeline_id,
                target_dt_name=target_dt_name,
                lag_minutes=int(lag_minutes),
                warehouse=default_wh,
                sql_select=sql_text,
                source_hint=source_hint,
            )
            st.success("Inserted. The orchestrator will create the Dynamic Table shortly.")
        except Exception as e:
            st.error(f"Insert failed: {e}")
