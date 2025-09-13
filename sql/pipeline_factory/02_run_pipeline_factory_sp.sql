-- Python Stored Procedure: RUN_PIPELINE_FACTORY
create or replace procedure RUN_PIPELINE_FACTORY()
  returns string
  language python
  runtime_version = '3.11'
  packages = ('snowflake-snowpark-python')
  handler = 'run'
  execute as owner
as
$$
from typing import List
from snowflake.snowpark import Session

def quote_identifier(identifier: str) -> str:
    parts = [p.strip() for p in identifier.split('.')]
    quoted_parts = []
    for p in parts:
        if not p:
            continue
        p_safe = p.replace('"', '""')
        quoted_parts.append('"' + p_safe + '"')
    return '.'.join(quoted_parts)

def quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"

def build_select_sql(snippet: str, quoted_source_table: str) -> str:
    s = (snippet or "").strip()
    has_placeholder = "{SOURCE_TABLE}" in s
    if has_placeholder:
        return s.replace("{SOURCE_TABLE}", quoted_source_table)
    if s[:6].lower() == "select":
        return s
    # Treat as clause appended to select * from source
    return f"select * from {quoted_source_table} {s}"

def run(session: Session) -> str:
    rows: List = session.sql("""
        select
          pipeline_id,
          source_table_name,
          transformation_sql_snippet,
          target_dt_name,
          lag_minutes,
          warehouse
        from PIPELINE_CONFIG
        where status = 'PENDING'
        order by pipeline_id
    """).collect()

    if not rows:
        return "No pending pipelines."

    created = 0
    messages = []

    for r in rows:
        pipeline_id = r['PIPELINE_ID']
        source_table_name = r['SOURCE_TABLE_NAME']
        snippet = r['TRANSFORMATION_SQL_SNIPPET']
        target_dt_name = r['TARGET_DT_NAME']
        lag_minutes = int(r['LAG_MINUTES'])
        warehouse = r['WAREHOUSE']

        q_source = quote_identifier(source_table_name)
        q_target = quote_identifier(target_dt_name)
        q_wh = quote_identifier(warehouse)

        select_sql = build_select_sql(snippet, q_source)

        dt_sql = f"""
create or replace dynamic table {q_target}
warehouse = {q_wh}
lag = '{lag_minutes} minutes'
as
{select_sql}
"""
        session.sql(dt_sql).collect()

        session.sql(
            f"update PIPELINE_CONFIG set status = 'ACTIVE' where pipeline_id = {quote_literal(pipeline_id)}"
        ).collect()

        created += 1
        messages.append(f"{pipeline_id} -> {target_dt_name}")

    return f"Created/updated {created} dynamic table(s): " + ", ".join(messages)
$$;
