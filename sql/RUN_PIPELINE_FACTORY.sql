CREATE OR REPLACE PROCEDURE "RUN_PIPELINE_FACTORY"()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS OWNER
AS '
from typing import List
from snowflake.snowpark import Session

def quote_identifier(identifier: str) -> str:
    parts = [p.strip() for p in identifier.split(''.'')]
    quoted_parts = []
    for p in parts:
        if not p:
            continue
        p_safe = p.replace(''"'', ''""'')
        quoted_parts.append(''"'' + p_safe + ''"'')
    return ''.''.join(quoted_parts)

def quote_literal(value: str) -> str:
    return "''" + value.replace("''", "''''") + "''"

def build_select_sql(snippet: str) -> str:
    s = (snippet or "").strip()
    return s

def run(session: Session) -> str:
    rows: List = session.sql("""
        select
          transformation_sql_snippet,
          target_dt_database,
          target_dt_name,
          lag_minutes,
          warehouse
        from PIPELINE_CONFIG
        where status = ''PENDING''
        order by target_dt_name
    """).collect()

    if not rows:
        return "No pending pipelines."

    created = 0
    messages = []

    for r in rows:
        snippet = r[''TRANSFORMATION_SQL_SNIPPET'']
        target_dt_database = r[''TARGET_DT_DATABASE'']
        target_dt_name = r[''TARGET_DT_NAME'']
        lag_minutes = int(r[''LAG_MINUTES''])
        warehouse = r[''WAREHOUSE'']

        q_db = quote_identifier(target_dt_database)
        q_target = quote_identifier(target_dt_name)
        q_wh = quote_identifier(warehouse)

        select_sql = build_select_sql(snippet)

        dt_sql = f"""
create or replace dynamic table {q_db}..{q_target}
warehouse = {q_wh}
lag = ''{lag_minutes} minutes''
as
{select_sql}
"""
        session.sql(dt_sql).collect()

        session.sql(
            f"update PIPELINE_CONFIG set status = ''ACTIVE'' where target_dt_database = {quote_literal(target_dt_database)} and target_dt_name = {quote_literal(target_dt_name)}"
        ).collect()

        created += 1
        messages.append(f"{target_dt_database}..{target_dt_name}")

    return f"Created/updated {created} dynamic table(s): " + ", ".join(messages)
';