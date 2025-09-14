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
# Deprecated: explicit table allowlist via UI (kept for compatibility)
ALLOWED_TABLES = set()  # e.g., {"RAW.SALES.ORDERS", "RAW.CRM.CUSTOMERS"}
PREVIEW_LIMIT = 50
CORTEX_MODEL = "mistral-large"
FEW_SHOTS: List[str] = [
    """You are an expert Snowflake SQL assistant. Follow these rules:
1. Only output a single SELECT statement
2. Use fully qualified table names (DATABASE.SCHEMA.TABLE)
3. Always include WHERE clauses to filter data meaningfully
4. Use appropriate JOINs when multiple tables are needed
5. Include realistic sample data filters (like date ranges, status filters, etc.)
6. No comments, explanations, or markdown formatting
7. Ensure the query will return actual rows from real data

Example patterns:
- For "latest orders": SELECT * FROM DB.SCHEMA.ORDERS WHERE ORDER_DATE >= CURRENT_DATE - 30
- For "customer analysis": SELECT C.*, COUNT(O.ORDER_ID) FROM DB.SCHEMA.CUSTOMERS C LEFT JOIN DB.SCHEMA.ORDERS O ON C.ID = O.CUSTOMER_ID GROUP BY C.ID
- Always add realistic filters to avoid empty results""",
]
MAX_GENERATION_ATTEMPTS = 5

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
    s_no_comments = strip_sql_comments(sql_text)
    s = s_no_comments.strip()
    s = s.rstrip(";")
    starts = s.lstrip().lower()
    starts_ok = starts.startswith("select") or starts.startswith("with")
    return starts_ok and not has_unquoted_semicolon(s_no_comments)


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


def try_generate_and_preview(session: Session, model: str, system: str, schema_card: str, user_prompt: str, preview_limit: int = 2, max_attempts: int = 5):
    errors: List[str] = []
    for attempt in range(1, max_attempts + 1):
        # Enhanced prompting with more context and examples
        attempt_context = ""
        if attempt > 1:
            attempt_context = f"""
PREVIOUS ATTEMPTS FAILED. Common issues to avoid:
- Empty result sets (add WHERE clauses with realistic filters)
- Invalid table/column references (check schema carefully)
- Syntax errors (verify JOIN conditions and column names)

This is attempt {attempt}/{max_attempts}. Be more conservative and add filters.
"""
        
        full_prompt = f"""
{attempt_context}
Available tables and columns in your scope:
{schema_card}

IMPORTANT: 
- Use ONLY the tables and columns listed above
- Add WHERE clauses to ensure results (like date filters, status filters, etc.)
- Use realistic sample conditions that would exist in real data
- For date columns, try filters like: WHERE date_col >= CURRENT_DATE - 30
- For status columns, try filters like: WHERE status = 'ACTIVE' OR status IN ('PENDING', 'COMPLETE')

User request: {user_prompt}

Generate a single SELECT statement that will return actual rows:
"""
        try:
            res = session.sql(
                f"select snowflake.cortex.complete('{model}', $$ {system}\n\n{full_prompt} $$) as c"
            ).collect()[0][0]
        except Exception as e:
            errors.append(f"Model call failed (attempt {attempt}): {e}")
            continue

        sql_text = normalize_model_sql(res)

        if not is_single_select(sql_text):
            errors.append(f"Generated SQL is not a single SELECT (attempt {attempt}).")
            continue
        if not enforce_read_only(sql_text):
            errors.append(f"Generated SQL is not read-only (attempt {attempt}).")
            continue

        try:
            rows = preview_query(session, sql_text, limit=preview_limit)
        except Exception as e:
            errors.append(f"Execution failed (attempt {attempt}): {e}")
            continue

        if rows and len(rows) > 0:
            return True, sql_text, rows, errors
        else:
            errors.append(f"Query returned 0 rows (attempt {attempt}). Try adding WHERE clauses with realistic filters.")

    return False, "", [], errors


def strip_sql_comments(text: str) -> str:
    # Remove -- line comments and /* */ block comments (not inside quotes)
    def _remove_block_comments(s: str) -> str:
        return re.sub(r"/\*[^*]*\*+(?:[^/*][^*]*\*+)*/", " ", s, flags=re.S)

    def _remove_line_comments(s: str) -> str:
        lines = []
        for line in s.splitlines():
            if "--" in line:
                idx = line.find("--")
                lines.append(line[:idx])
            else:
                lines.append(line)
        return "\n".join(lines)

    return _remove_line_comments(_remove_block_comments(text))


def has_unquoted_semicolon(text: str) -> bool:
    in_single = False
    in_double = False
    i = 0
    while i < len(text):
        ch = text[i]
        if ch == "'" and not in_double:
            # Handle escaped single quotes inside strings by doubling ''
            if in_single and i + 1 < len(text) and text[i + 1] == "'":
                i += 2
                continue
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        elif ch == ";" and not in_single and not in_double:
            return True
        i += 1
    return False


def normalize_model_sql(text: str) -> str:
    # Extract content from triple backticks if present
    m = re.search(r"```(?:sql)?\s*([\s\S]*?)```", text, flags=re.I)
    if m:
        text = m.group(1)
    # Remove single backticks wrappers
    text = text.strip().strip("`")
    # Remove leading language hints like sql\n
    text = re.sub(r"^(?i:sql)\n", "", text)
    # Trim comments/fences leftovers
    text = strip_sql_comments(text).strip()
    # Drop trailing semicolon (only one statement expected)
    if text.endswith(";"):
        text = text[:-1]
    return text.strip()


def list_databases(session: Session) -> List[str]:
    rows = session.sql(
        "select database_name from snowflake.information_schema.databases order by 1"
    ).collect()
    return [r[0] for r in rows]


def list_schemas_in_db(session: Session, database_name: str) -> List[str]:
    rows = session.sql(
        f"select schema_name from {database_name}.information_schema.schemata order by 1"
    ).collect()
    return [r[0] for r in rows]


def list_tables_in_db(session: Session, database_name: str) -> List[tuple[str, str]]:
    rows = session.sql(
        f"""
        select table_schema, table_name
        from {database_name}.information_schema.tables
        where table_type = 'BASE TABLE'
        order by 1, 2
        """
    ).collect()
    return [(r[0], r[1]) for r in rows]


def list_tables_in_schema(session: Session, database_name: str, schema_name: str) -> List[str]:
    rows = session.sql(
        f"""
        select table_name
        from {database_name}.information_schema.tables
        where table_type = 'BASE TABLE' and table_schema = '{schema_name}'
        order by 1
        """
    ).collect()
    return [r[0] for r in rows]


def fetch_schema_card_for_schema(session: Session, database_name: str, schema_name: str, max_tables: int = 50, max_cols: int = 30) -> str:
    # Build a compact schema card: per table, a list of columns and types
    rows = session.sql(
        f"""
        select table_name, column_name, data_type
        from {database_name}.information_schema.columns
        where table_schema = '{schema_name}'
        order by table_name, ordinal_position
        """
    ).collect()
    card_lines: List[str] = []
    current_table = None
    cols: List[str] = []
    tables_count = 0
    for r in rows:
        tbl = r[0]
        col = r[1]
        typ = r[2]
        if current_table is None:
            current_table = tbl
        if tbl != current_table:
            card_lines.append(f"- {database_name}.{schema_name}.{current_table}: "+ ", ".join(cols[:max_cols]))
            tables_count += 1
            if tables_count >= max_tables:
                break
            current_table = tbl
            cols = []
        cols.append(f"{col} {typ}")
    # flush last table
    if current_table is not None and tables_count < max_tables:
        card_lines.append(f"- {database_name}.{schema_name}.{current_table}: "+ ", ".join(cols[:max_cols]))
    return "\n".join(card_lines)


def extract_primary_table(sql_text: str) -> str | None:
    # Very simple extractor: find first FROM <identifier> (handles quoted identifiers and dots)
    s = strip_sql_comments(sql_text)
    m = re.search(r"from\s+((?:\"[^\"]+\"|[A-Za-z0-9_]+)(?:\.(?:\"[^\"]+\"|[A-Za-z0-9_]+)){0,2})", s, re.I)
    if m:
        return m.group(1)
    return None

st.set_page_config(page_title="Pipeline Factory", layout="wide")

@st.cache_resource(show_spinner=False)
def _get_session() -> Session:
    return get_session()

session = _get_session()

st.title("Prompt → SQL → Dynamic Table")

with st.expander("Scope & Options", expanded=True):
    st.caption("Select a database and schema. The model will choose applicable tables.")

    # Database selection
    db_list = list_databases(session)
    selected_db = st.selectbox("Database", options=db_list, index=0 if db_list else None)

    # Schema multiselect in selected database
    selected_schemas = []
    if selected_db:
        schema_list = list_schemas_in_db(session, selected_db)
        selected_schemas = st.multiselect("Schemas", options=schema_list, default=[])

    default_wh = st.text_input("Warehouse", value=DEFAULT_WAREHOUSE)
    target_dt_name = st.text_input("Target Dynamic Table name (DB.SCHEMA.NAME)")
    lag_minutes = st.number_input("Lag (minutes)", min_value=1, max_value=1440, value=10)
    pipeline_id = st.text_input("Pipeline ID", placeholder="orders_complete_nlp")

st.subheader("Describe the data")
prompt = st.text_area("Prompt", height=140, placeholder="Show the latest order per customer in the last 30 days")

if st.button("Generate SQL with Cortex", type="primary"):
    if not selected_db or not selected_schemas:
        st.error("Please select a database and at least one schema.")
    elif not prompt.strip():
        st.error("Please enter a prompt.")
    else:
        with st.spinner("Generating and validating SQL..."):
            # Build schema card for selected database + schemas
            schema_cards = []
            for schema in selected_schemas:
                card = fetch_schema_card_for_schema(session, selected_db, schema)
                if card:
                    schema_cards.append(card)
            schema_card = "\n".join(schema_cards)
            system = "\n".join(FEW_SHOTS)
            ok, generated_sql, preview_rows, errs = try_generate_and_preview(
                session=session,
                model=CORTEX_MODEL,
                system=system,
                schema_card=schema_card,
                user_prompt=prompt,
                preview_limit=2,
                max_attempts=MAX_GENERATION_ATTEMPTS,
            )
        if ok:
            st.code(generated_sql, language="sql")
            st.session_state["generated_sql"] = generated_sql
            st.success("SQL validated and returns rows. Preview below.")
            st.dataframe([row.as_dict() for row in preview_rows], use_container_width=True)
        else:
            st.error("Failed to generate executable SQL that returns rows after multiple attempts.")
            with st.expander("Troubleshooting Details"):
                st.write("**Common issues and solutions:**")
                st.write("- **Empty tables**: Your selected schemas may have no data or test data")
                st.write("- **Column names**: Check if your prompt references columns that exist")
                st.write("- **Filters needed**: Try more specific prompts like 'active customers from last 30 days'")
                st.write("- **Schema scope**: Try selecting fewer schemas or different schemas with known data")
                st.write("")
                st.write("**Attempt details:**")
                for e in errs:
                    st.write("- " + e)
                st.write("")
                st.write("**Suggestions:**")
                st.write("- Try a simpler query like 'show me 10 rows from [table name]'")
                st.write("- Be more specific about date ranges or status filters")
                st.write("- Check if the selected schemas contain actual data")

if "generated_sql" in st.session_state:
    sql_text = st.session_state["generated_sql"]

    st.subheader("Validation & Preview")
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

    preview_ok = False
    if is_select and is_ro and explain_ok:
        try:
            rows = preview_query(session, sql_text, limit=2)
            if rows:
                st.dataframe([row.as_dict() for row in rows], use_container_width=True)
                preview_ok = True
            else:
                st.info("Query returned 0 rows.")
        except Exception as e:
            st.error(f"Preview failed: {e}")

    st.subheader("Create Pipeline")
    if not target_dt_name:
        st.info("Enter a target Dynamic Table name above.")
    can_create = is_select and is_ro and explain_ok and preview_ok and bool(target_dt_name) and bool(pipeline_id)

    if st.button("Insert into PIPELINE_CONFIG (PENDING)", disabled=not can_create):
        try:
            # Derive a source hint from the primary table in the generated SQL, else use a placeholder
            source_hint = extract_primary_table(sql_text) or "PUBLIC.DUAL"
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
