from __future__ import annotations
from typing import Dict, List, Tuple
import os
import re
import sqlparse
from snowflake.snowpark import Session

READ_ONLY_PATTERN = re.compile(r"\b(insert|update|delete|merge|create|alter|drop|truncate|grant|revoke|call)\b", re.I)


def get_session() -> Session:
    # For local dev, rely on env vars; in Snowflake Streamlit, use default context
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


def list_tables(session: Session, database: str | None = None, schema: str | None = None) -> List[Tuple[str, str, str]]:
    sql = """
        select table_catalog, table_schema, table_name
        from information_schema.tables
        where table_type = 'BASE TABLE'
    """
    if database:
        sql += f" and table_catalog = '{database}'"
    if schema:
        sql += f" and table_schema = '{schema}'"
    sql += " order by 1, 2, 3"
    return [(r[0], r[1], r[2]) for r in session.sql(sql).collect()]


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
    parsed = sqlparse.parse(sql_text)
    if len(parsed) != 1:
        return False
    stmt = parsed[0]
    return stmt.get_type() == "SELECT" and ";" not in sql_text


def enforce_read_only(sql_text: str) -> bool:
    return READ_ONLY_PATTERN.search(sql_text) is None


def preview_query(session: Session, sql_text: str, limit: int = 50):
    limited = f"select * from ( {sql_text} ) limit {limit}"
    return session.sql(limited).collect()


def explain_query(session: Session, sql_text: str) -> str:
    result = session.sql(f"explain using text {sql_text}").collect()
    return "\n".join([r[0] for r in result])


def insert_pipeline_config(session: Session, pipeline_id: str, target_dt_name: str, lag_minutes: int, warehouse: str, sql_select: str, source_hint: str) -> None:
    # Use the full SELECT as transformation, and pass a minimal source table hint
    sql = f"""
        insert into PIPELINE_CONFIG (
          pipeline_id, source_table_name, transformation_sql_snippet, target_dt_name, lag_minutes, warehouse, status
        ) values (
          '{pipeline_id}', '{source_hint}', $$ {sql_select} $$, '{target_dt_name}', {lag_minutes}, '{warehouse}', 'PENDING'
        )
    """
    session.sql(sql).collect()
