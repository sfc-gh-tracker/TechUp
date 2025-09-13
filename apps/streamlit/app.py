import streamlit as st
from typing import List
import os
import re
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session

# ------------------------------
# Inlined config/constants
# ------------------------------
DEFAULT_WAREHOUSE = "PIPELINE_WH"
ALLOWED_TABLES = set()  # e.g., {"RAW.SALES.ORDERS", "RAW.CRM.CUSTOMERS"}
PREVIEW_LIMIT = 50
CORTEX_MODEL = "mistral-large"
FEW_SHOTS: List[str] = [
    "You are a SQL assistant for Snowflake. Only output a single SELECT statement with fully qualified identifiers. No DML/DDL, no comments.",
]

# ------------------------------
# Inlined utility functions
# ------------------------------
READ_ONLY_PATTERN = re.compile(r"\b(insert|update|delete|merge|create|alter|drop|truncate|grant|revoke|call)\b", re.I)


def get_session() -> Session:
    # Use active session in Snowflake Streamlit; for local dev, build from env
    try:
        return get_active_session()
    except Exception:
        if os.getenv("SNOWFLAKE_ACCOUNT"):
            from snowflake.snowpark import Session as _Session

            conn_params = {
                "account": os.getenv("SNOWFLAKE_ACCOUNT"),
                "user": os.getenv("SNOWFLAKE_USER"),
                "password": os.getenv("SNOWFLAKE_PASSWORD"),
                "role": os.getenv("SNOWFLAKE_ROLE"),
                "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
                "database": os.getenv("SNOWFLAKE_DATABASE"),
                "schema": os.getenv("SNOWFLAKE_SCHEMA"),
            }
            return _Session.builder.configs(conn_params).create()
        from snowflake.snowpark import Session as _Session

        return _Session.builder.getOrCreate()


def fetch_schema_card(session: Session, table_fqns: List[str]) -> str:
    if not table_fqns:
        return ""
    parts = []
    for fqn in table_fqns:
        db, sch, tbl = fqn.split(".")
        rows = session.sql(
            f"""
            select column_name, data_type
            from {db}.information_schema.columns
            where table_schema = '{sch}' and table_name = '{tbl}'
            order by ordinal_position
            """
        ).collect()
        cols = ", ".join([f"{r['COLUMN_NAME']} {r['DATA_TYPE']}" for r in rows])
        parts.append(f"- {fqn}: {cols}")
    return "\n".join(parts)


def is_single_select(sql_text: str) -> bool:
    s = sql_text.strip().rstrip(";")
    starts_ok = s[:6].lower() == "select" or s[:4].lower() == "with"
    has_semicolon = ";" in sql_text
    return starts_ok and not has_semicolon


def enforce_read_only(sql_text: str) -> bool:
    return READ_ONLY_PATTERN.search(sql_text) is None


def preview_query(session: Session, sql_text: str, limit: int = 50):
    limited = f"select * from ( {sql_text} ) limit {limit}"
    return session.sql(limited).collect()


def explain_query(session: Session, sql_text: str) -> str:
    result = session.sql(f"explain using text {sql_text}").collect()
    return "\n".join([r[0] for r in result])


def insert_pipeline_config(session: Session, pipeline_id: str, target_dt_name: str, lag_minutes: int, warehouse: str, sql_select: str, source_hint: str) -> None:
    sql = f"""
        insert into PIPELINE_CONFIG (
          pipeline_id, source_table_name, transformation_sql_snippet, target_dt_name, lag_minutes, warehouse, status
        ) values (
          '{pipeline_id}', '{source_hint}', $$ {sql_select} $$, '{target_dt_name}', {lag_minutes}, '{warehouse}', 'PENDING'
        )
    """
    session.sql(sql).collect()

st.set_page_config(page_title="Pipeline Factory", layout="wide")

@st.cache_resource(show_spinner=False)
def _get_session() -> Session:
    return get_session()

session = _get_session()

st.title("Prompt → SQL → Dynamic Table")

with st.expander("Scope & Options", expanded=True):
    st.caption("Choose tables to ground the model. Fewer tables = better accuracy.")
    # Allow manual entry of allowed tables for now
    allowed_tables_input = st.text_input(
        "Allowed tables (comma-separated, fully qualified DB.SCHEMA.TABLE)",
        value=", ".join(ALLOWED_TABLES) if ALLOWED_TABLES else "",
        placeholder="RAW.SALES.ORDERS, RAW.CRM.CUSTOMERS",
    )
    allowed_tables = [t.strip() for t in allowed_tables_input.split(",") if t.strip()]

    default_wh = st.text_input("Warehouse", value=DEFAULT_WAREHOUSE)
    target_dt_name = st.text_input("Target Dynamic Table name (DB.SCHEMA.NAME)")
    lag_minutes = st.number_input("Lag (minutes)", min_value=1, max_value=1440, value=10)
    pipeline_id = st.text_input("Pipeline ID", placeholder="orders_complete_nlp")

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
