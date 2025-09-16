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

PROHIBITED_TOKENS = (
    ' use ', ' create ', ' alter ', ' drop ', ' grant ', ' revoke ', ' call ',
    ' copy ', ' insert ', ' update ', ' delete ', ' merge ', ' truncate ', ' set '
)
ALLOWED_CLAUSE_PREFIXES = (
    'where', 'qualify', 'group by', 'having', 'order by', 'limit'
)

def validate_snippet(sql_text: str) -> None:
    s = (sql_text or '').strip()
    s_lower = ' ' + s.lower() + ' '
    if ';' in s_lower:
        raise ValueError("Transformation SQL must be a single statement (no semicolons)")
    for token in PROHIBITED_TOKENS:
        if token in s_lower:
            raise ValueError(f"Unsupported token in transformation SQL: {token.strip()}")

def sanitize_snippet(sql_text: str) -> str:
    # Remove any lines starting with USE/SET and strip semicolons
    lines = []
    for raw in (sql_text or '').splitlines():
        l = raw.strip()
        if not l:
            continue
        l_lower = l.lower()
        if l_lower.startswith('use ') or l_lower.startswith('set '):
            continue
        if l.endswith(';'):
            l = l[:-1]
        lines.append(l)
    return '\n'.join(lines).strip()

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
    s = sanitize_snippet(snippet)
    has_placeholder = "{SOURCE_TABLE}" in s
    if has_placeholder:
        validate_snippet(s)
        return s.replace("{SOURCE_TABLE}", quoted_source_table)
    s_lower = s[:6].lower()
    if s_lower.startswith("select") or s[:4].lower() == 'with':
        validate_snippet(s)
        return s
    # Treat as clause appended to select * from source
    validate_snippet(s)
    s_head = s.lower().lstrip()
    if not any(s_head.startswith(p) for p in ALLOWED_CLAUSE_PREFIXES):
        raise ValueError("Snippet must be a SELECT/CTE or a clause starting with WHERE/QUALIFY/GROUP BY/HAVING/ORDER BY/LIMIT")
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
