import streamlit as st
from typing import List
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session

# Inline config (replaces external config.py)
DEFAULT_WAREHOUSE = "PIPELINE_WH"
ALLOWED_TABLES: List[str] = []
PREVIEW_LIMIT = 3
CORTEX_MODEL = "mistral-large"
FEW_SHOTS = [
    "You are a Snowflake SQL assistant. Only output a single SELECT query. Use fully qualified identifiers. Do not include comments or extra text."
]

# Inline utils (replaces external snowflake_utils.py)
def get_session() -> Session:
    return get_active_session()

def normalize_sql_for_validation(sql_text: str) -> str:
    s = (sql_text or "").strip()
    # Strip fenced code blocks like ```sql ... ```
    s = re.sub(r"^```[a-zA-Z]*\s*", "", s)
    s = re.sub(r"```\s*$", "", s)
    s = s.strip()
    # Allow a single trailing semicolon
    if s.endswith(";"):
        s = s[:-1]
    return s.strip()

def list_tables(session: Session, database: str, schema: str) -> List[str]:
    rows = session.sql(
        f"select table_name from {database}.information_schema.tables where table_schema = '{schema}' and table_type in ('BASE TABLE','VIEW') order by table_name"
    ).collect()
    return [f"{database}.{schema}.{r['TABLE_NAME']}" for r in rows]

def get_databases(session: Session) -> List[str]:
    try:
        rows = session.sql("show databases").collect()
        # Snowpark SHOW returns lower-case keys typically, but guard for both
        names = []
        for r in rows:
            if 'name' in r:
                names.append(r['name'])
            elif 'NAME' in r:
                names.append(r['NAME'])
        return sorted(names)
    except Exception:
        return []

def get_schemas(session: Session, database: str) -> List[str]:
    try:
        rows = session.sql(f"show schemas in database {database}").collect()
        names = []
        for r in rows:
            if 'name' in r:
                names.append(r['name'])
            elif 'NAME' in r:
                names.append(r['NAME'])
        return sorted(names)
    except Exception:
        return []

def fetch_schema_card(session: Session, allowed_tables: List[str]) -> str:
    lines: List[str] = []
    for full in allowed_tables:
        parts = [p.strip() for p in full.split('.')]
        if len(parts) != 3:
            continue
        db, sch, tbl = parts
        cols = session.sql(
            f"select column_name, data_type from {db}.information_schema.columns where table_schema = '{sch}' and table_name = '{tbl}' order by ordinal_position"
        ).collect()
        col_list = ", ".join([f"{c['COLUMN_NAME']}({c['DATA_TYPE']})" for c in cols])
        lines.append(f"{db}.{sch}.{tbl}: {col_list}")
    return "\n".join(lines)

def is_single_select(sql_text: str) -> bool:
    s = normalize_sql_for_validation(sql_text)
    if not s:
        return False
    # Disallow any additional semicolons inside the text
    if ';' in s:
        return False
    head = s.lstrip()[:6].upper()
    return head.startswith("SELECT") or s.lstrip()[:4].upper() == "WITH"

def enforce_read_only(sql_text: str) -> bool:
    s = " " + (sql_text or "").upper() + " "
    prohibited = [
        " INSERT ", " UPDATE ", " DELETE ", " MERGE ", " TRUNCATE ",
        " CREATE ", " ALTER ", " DROP ", " GRANT ", " REVOKE ",
        " COPY ", " CALL ", " USE ", " SET "
    ]
    return not any(tok in s for tok in prohibited)

def preview_query(session: Session, sql_text: str, limit: int = PREVIEW_LIMIT):
    clean = normalize_sql_for_validation(sql_text)
    return session.sql(f"select * from ({clean}) limit {limit}").collect()

def explain_query(session: Session, sql_text: str) -> str:
    clean = normalize_sql_for_validation(sql_text)
    rows = session.sql(f"EXPLAIN USING TEXT {clean}").collect()
    out: List[str] = []
    for r in rows:
        try:
            out.append(str(list(r.values())[0]))
        except Exception:
            out.append(str(r))
    return "\n".join(out)

def insert_pipeline_config(
    session: Session,
    pipeline_id: str,
    target_dt_name: str,
    lag_minutes: int,
    warehouse: str,
    sql_select: str,
    source_hint: str,
):
    # Insert minimal required fields; status starts as PENDING
    insert_sql = f"""
    INSERT INTO PIPELINE_CONFIG (
        pipeline_id,
        transformation_sql_snippet,
        target_dt_name,
        lag_minutes,
        warehouse,
        status
    ) VALUES (
        '{pipeline_id}',
        $${sql_select}$$,
        '{target_dt_name}',
        {lag_minutes},
        '{warehouse}',
        'PENDING'
    )
    """
    session.sql(insert_sql).collect()

st.set_page_config(page_title="Pipeline Factory", layout="wide")

@st.cache_resource(show_spinner=False)
def _get_session() -> Session:
    return get_session()

session = _get_session()

st.title("Prompt → SQL → Dynamic Table")

with st.expander("Scope & Options", expanded=True):
    st.caption("Choose tables to ground the model. Fewer tables = better accuracy.")

    # Searchable dropdowns for Database and Schema
    db_list = get_databases(session)
    selected_db = st.selectbox("Database", options=db_list) if db_list else ""
    schema_list = get_schemas(session, selected_db) if selected_db else []
    selected_schema = st.selectbox("Schema", options=schema_list) if schema_list else ""

    # Multiselect for allowed tables from the selected Database/Schema
    allowed_tables: List[str] = []
    if selected_db and selected_schema:
        available = list_tables(session, selected_db, selected_schema)
        allowed_tables = st.multiselect(
            "Allowed tables (DB.SCHEMA.TABLE)",
            options=available,
            default=[],
        )
    else:
        st.info("Select a Database and Schema to choose allowed tables.")

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
