"""
Natural Language to SQL Pipeline Generator for Snowflake Streamlit

This application converts natural language business questions into SQL queries 
using Snowflake Cortex AI and integrates with an existing pipeline factory system.

Required Libraries (automatically available in Snowflake Streamlit):
- streamlit
- pandas 
- snowflake-snowpark-python
- json (built-in)
- typing (built-in)

Setup Instructions:
1. Deploy this file to Snowflake Streamlit
2. Ensure Cortex AI is enabled in your Snowflake account
3. Verify access to the PIPELINE_CONFIG table
4. Grant necessary permissions for schema discovery
"""

# Import required libraries for Snowflake Streamlit
import streamlit as st
import pandas as pd
import json
import re
from typing import Dict, List, Optional, Tuple
from snowflake.snowpark.context import get_active_session

# Page configuration
st.set_page_config(
    page_title="Natural Language to SQL Pipeline Generator",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# App version - bump this on each update
VERSION = "1.3"

def render_version_badge() -> None:
    badge_css = """
    <style>
    .version-badge {
        position: fixed;
        top: 8px;
        right: 12px;
        background: #f0f2f6;
        color: #343a40;
        border: 1px solid #e0e0e0;
        border-radius: 12px;
        padding: 2px 10px;
        font-size: 12px;
        font-weight: 600;
        z-index: 1000;
        box-shadow: 0 1px 2px rgba(0,0,0,0.05);
    }
    </style>
    """
    st.markdown(badge_css + f"<div class=\"version-badge\">v {VERSION}</div>", unsafe_allow_html=True)

def get_snowflake_session():
    """Get the active Snowflake session for Snowflake Streamlit apps."""
    try:
        return get_active_session()
    except Exception as e:
        st.error(f"Failed to get Snowflake session: {str(e)}")
        return None

def get_databases(session) -> List[str]:
    """Fetch available databases from Snowflake."""
    try:
        result = session.sql("SHOW DATABASES").collect()
        return sorted([row['name'] for row in result])
    except Exception as e:
        st.error(f"Error fetching databases: {str(e)}")
        return []

def get_schemas(session, database: str) -> List[str]:
    """Fetch available schemas from the selected database."""
    try:
        result = session.sql(f"SHOW SCHEMAS IN DATABASE {database}").collect()
        return sorted([row['name'] for row in result])
    except Exception as e:
        st.error(f"Error fetching schemas: {str(e)}")
        return []

def get_schema_metadata(
    session,
    database: str,
    schema: str,
    max_tables: int = 20,
    max_columns_per_table: int = 12,
    max_chars: int = 6000,
) -> Tuple[str, bool]:
    """Fetch table/column metadata with limits to avoid LLM token overflow.

    Returns: (schema_string, was_truncated)
    """
    try:
        # 1) Pick top N tables by row_count (fall back to alphabetical)
        top_tables_sql = f"""
        with top_tables as (
          select table_name
          from {database}.information_schema.tables
          where table_schema = '{schema}' and table_type in ('BASE TABLE','VIEW')
          order by coalesce(row_count, 0) desc, table_name
          limit {max_tables}
        )
        select c.table_name, c.column_name, c.data_type, c.ordinal_position
        from {database}.information_schema.columns c
        join top_tables t
          on c.table_name = t.table_name
        where c.table_schema = '{schema}'
        order by c.table_name, c.ordinal_position
        """
        rows = session.sql(top_tables_sql).collect()

        # Group columns by table and enforce per-table column limit
        table_to_columns = {}
        for r in rows:
            t = r['TABLE_NAME']
            if t not in table_to_columns:
                table_to_columns[t] = []
            if len(table_to_columns[t]) < max_columns_per_table:
                table_to_columns[t].append(f"{r['COLUMN_NAME']} ({r['DATA_TYPE']})")

        # Build schema string
        parts = []
        for table_name, cols in table_to_columns.items():
            parts.append(f"Table: {table_name}, Columns: {', '.join(cols)}")

        schema_str = "; ".join(parts)

        # Hard cap to avoid model token overflow
        truncated = False
        if len(schema_str) > max_chars:
            schema_str = schema_str[: max_chars].rsplit(' ', 1)[0] + " ... [TRUNCATED]"
            truncated = True

        return schema_str, truncated
    except Exception as e:
        st.error(f"Error fetching schema metadata: {str(e)}")
        return "", False

def generate_sql_with_cortex(session, schema_metadata: str, user_question: str, database: str, schema: str, schema_truncated: bool = False, additional_instructions: str = "", model_name: str = 'snowflake-arctic') -> Optional[str]:
    """Generate SQL using Snowflake Cortex AI."""
    try:
        system_prompt = "You are an expert Snowflake SQL data analyst. Your task is to write a single, clean, and efficient SQL query to answer the user's question based on the provided schema."
        
        truncation_note = "\nNote: The schema context was truncated for brevity. Focus only on referenced tables/columns.\n" if schema_truncated else ""

        user_prompt = f"""
Here is the schema for the database {database}.{schema}:
---
{schema_metadata}
---
Based on this schema, write a SQL query that answers the following question: '{user_question}'

Important guidelines:
- Only return the SQL code itself, with no explanations or introductions
- Use fully qualified table names (database.schema.table_name)
- The query should be ready to run as-is
- Focus on answering the specific business question asked
{truncation_note}
{additional_instructions}
"""

        # Construct the Cortex query - escape single quotes properly
        system_prompt_escaped = system_prompt.replace("'", "''")
        user_prompt_escaped = user_prompt.replace("'", "''")
        
        cortex_query = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            '{model_name}',
            CONCAT(
                '<s>[INST]',
                '{system_prompt_escaped}',
                '{user_prompt_escaped}',
                '[/INST]'
            )
        ) as sql_response
        """
        
        result = session.sql(cortex_query).collect()
        response_data = result[0]['SQL_RESPONSE']
        
        # Clean up the response - remove common AI response patterns
        cleaned_response = str(response_data).strip()
        
        # Remove markdown code blocks if present
        if cleaned_response.startswith('```sql'):
            cleaned_response = cleaned_response.replace('```sql', '').replace('```', '').strip()
        elif cleaned_response.startswith('```'):
            cleaned_response = cleaned_response.replace('```', '').strip()
            
        return cleaned_response
        
    except Exception as e:
        st.error(f"Error generating SQL with Cortex: {str(e)}")
        return None

def validate_sql_with_cortex(
    session,
    schema_metadata: str,
    schema_catalog: Dict[str, set],
    candidate_sql: str,
    database: str,
    schema: str,
    error_context: str = "",
    model_name: str = 'snowflake-arctic'
) -> Optional[str]:
    """Ask Cortex to validate/fix SQL using schema metadata and a table->columns map.
    Returns corrected SQL or None.
    """
    try:
        # Build a compact catalog focused on relevant tables to reduce token usage
        referenced = [t.split('.')[-1].replace('"', '') for t in extract_referenced_tables(candidate_sql)]
        ob_quals = [q.replace('"', '') for q, _c in extract_order_by_qualified_pairs(candidate_sql)]
        relevant = set([t.upper() for t in referenced + ob_quals])
        compact_catalog: Dict[str, List[str]] = {}
        for t, cols in schema_catalog.items():
            if t.upper() in relevant:
                compact_catalog[t] = sorted(list(cols))[:50]
        if not compact_catalog:
            # Fallback: first 10 tables alphabetically
            for t in sorted(schema_catalog.keys())[:10]:
                compact_catalog[t] = sorted(list(schema_catalog[t]))[:30]
        catalog_json = json.dumps(compact_catalog)
        system_prompt = (
            "You are a Snowflake SQL validator and fixer. Given a schema and a candidate SQL, "
            "return a single corrected SQL statement that strictly uses only existing tables/columns, "
            "adds necessary JOINs when referencing columns from other tables (prefer ID-like join keys), "
            "fully-qualifies identifiers as database.schema.table, and avoids DDL/USE/SET."
        )
        user_prompt = f"""
Database: {database}
Schema: {schema}

Schema summary:
---
{schema_metadata}
---

Schema catalog (JSON mapping of table -> columns):
{catalog_json}

Candidate SQL to validate/fix:
---
{candidate_sql}
---

Rules:
- Use only tables and columns present in the schema catalog above.
- If ORDER BY or SELECT references a column from a table not in FROM/JOIN, add the appropriate JOIN with an explicit ON using a reasonable shared key (prefer *_ID/ID/_KEY).
- If a column does not exist, replace with the closest semantically correct existing column.
- Fully qualify all table references as {database}.{schema}.table.
- Return only the corrected SQL (no explanations, no markdown, no code fences).
- The SQL must be a single statement.
{('Error context to fix: ' + error_context) if error_context else ''}
"""

        system_prompt_escaped = system_prompt.replace("'", "''")
        user_prompt_escaped = user_prompt.replace("'", "''")

        cortex_query = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            '{model_name}',
            CONCAT(
                '<s>[INST]',
                '{system_prompt_escaped}',
                '{user_prompt_escaped}',
                '[/INST]'
            )
        ) as sql_response
        """

        result = session.sql(cortex_query).collect()
        response_data = result[0]['SQL_RESPONSE']
        cleaned_response = str(response_data).strip()
        if cleaned_response.startswith('```sql'):
            cleaned_response = cleaned_response.replace('```sql', '').replace('```', '').strip()
        elif cleaned_response.startswith('```'):
            cleaned_response = cleaned_response.replace('```', '').strip()
        return cleaned_response
    except Exception as e:
        st.error(f"Error validating SQL with Cortex: {str(e)}")
        return None

def build_preview_sql(sql_text: str) -> str:
    """Wrap the generated SQL to safely preview the first 3 rows."""
    s = (sql_text or "").strip()
    if s.endswith(";"):
        s = s[:-1]
    return f"""
with __q as (
{s}
)
select * from __q limit 3
"""

def qualify_sql(sql_text: str, database: str, schema: str) -> str:
    """Best-effort qualification of unqualified table references in FROM/JOIN.
    This is heuristic and avoids touching subqueries or already qualified names.
    """
    if not sql_text:
        return sql_text

    q_db = '"' + database.replace('"', '""') + '"'
    q_schema = '"' + schema.replace('"', '""') + '"'

    def repl(match: re.Match) -> str:
        keyword = match.group(1)
        ident = match.group(2)
        tail = match.group(3) or ''
        ident_strip = ident.strip()
        lower = ident_strip.lower()
        # Skip subqueries, functions, stages, CTE refs, or already qualified
        if ident_strip.startswith('(') or ident_strip.startswith('@') or '.' in ident_strip or lower.startswith('table('):
            return f"{keyword} {ident}{tail}"
        return f"{keyword} {q_db}.{q_schema}.{ident}{tail}"

    # FROM and JOIN targets
    pattern = re.compile(r"(?i)\b(FROM|JOIN)\s+(\(?\s*(?:\"[^\"]+\"|[A-Za-z_][\w$]*))(\b)")
    qualified = re.sub(pattern, repl, sql_text)
    return qualified

def use_context(session, database: str, schema: str) -> None:
    # No-op placeholder; USE is not supported in some Streamlit contexts.
    return None

def get_schema_catalog(session, database: str, schema: str) -> Dict[str, set]:
    """Return mapping of table_name -> set(columns) for the selected db.schema."""
    query = f"""
    select table_name, column_name
    from {database}.information_schema.columns
    where table_schema = '{schema}'
    order by table_name, ordinal_position
    """
    rows = session.sql(query).collect()
    catalog: Dict[str, set] = {}
    for r in rows:
        t = r['TABLE_NAME']
        c = r['COLUMN_NAME']
        if t not in catalog:
            catalog[t] = set()
        catalog[t].add(c)
    return catalog

def extract_referenced_tables(sql_text: str) -> List[str]:
    """Heuristically extract table identifiers following FROM/JOIN clauses."""
    if not sql_text:
        return []
    pattern = re.compile(r"(?i)\b(FROM|JOIN)\s+((?:\"[^\"]+\"|[A-Za-z_][\w$]*)(?:\.(?:\"[^\"]+\"|[A-Za-z_][\w$]*)){0,2})")
    tables = []
    for m in pattern.finditer(sql_text):
        ident = m.group(2)
        # Strip trailing aliases
        ident = re.split(r"\s+", ident)[0]
        tables.append(ident)
    return tables

def extract_table_aliases(sql_text: str) -> Dict[str, str]:
    """Extract mapping of alias->base_table (simple name) from FROM/JOIN clauses."""
    aliases: Dict[str, str] = {}
    if not sql_text:
        return aliases
    pattern = re.compile(r"(?is)\b(FROM|JOIN)\s+((?:\"[^\"]+\"|[A-Za-z_][\w$]*)(?:\.(?:\"[^\"]+\"|[A-Za-z_][\w$]*)){0,2})\s*(?:AS\s+)?(?:\"([^\"]+)\"|([A-Za-z_][\w$]*))?")
    for m in pattern.finditer(sql_text):
        ident = (m.group(2) or '').strip()
        alias = (m.group(3) or m.group(4) or '').strip()
        if not ident:
            continue
        # base table simple name (last part of identifier)
        base = ident.split('.')[-1].replace('"', '')
        if alias:
            aliases[alias.replace('"', '')] = base
        else:
            # Map the table name to itself for easy resolution
            aliases[base] = base
    return aliases

def extract_column_references(sql_text: str) -> List[Tuple[str, str]]:
    """Extract (qualifier, column) pairs like alias.column or table.column."""
    refs: List[Tuple[str, str]] = []
    if not sql_text:
        return refs
    pattern = re.compile(r"(?i)(\"[^\"]+\"|[A-Za-z_][\w$]*)\s*\.\s*(\"[^\"]+\"|[A-Za-z_][\w$]*)")
    for m in pattern.finditer(sql_text):
        left = m.group(1)
        right = m.group(2)
        refs.append((left.replace('"', ''), right.replace('"', '')))
    return refs

def find_missing_columns(sql_text: str, schema_catalog: Dict[str, set]) -> List[str]:
    """Return list of 'table.column' that are not present per INFORMATION_SCHEMA."""
    aliases = extract_table_aliases(sql_text)
    refs = extract_column_references(sql_text)
    missing: List[str] = []
    for qual, col in refs:
        base = aliases.get(qual, qual)
        base_u = base.upper()
        col_u = col.upper()
        if base_u not in {t.upper(): None for t in schema_catalog.keys()}:
            missing.append(f"{base}.{col}")
            continue
        # find actual table key case-insensitively
        matched_table = next((t for t in schema_catalog.keys() if t.upper() == base_u), None)
        if matched_table is None:
            missing.append(f"{base}.{col}")
            continue
        # Validate columns on joins and predicates as well (we extract all qualifier.column pairs)
        if col_u not in {c.upper() for c in schema_catalog[matched_table]}:
            missing.append(f"{base}.{col}")
    # de-duplicate
    return sorted(list(dict.fromkeys(missing)))

def parse_invalid_identifier_from_error(err_msg: str) -> List[str]:
    if not err_msg:
        return []
    m = re.search(r"invalid identifier '([^']+)'", err_msg, re.IGNORECASE)
    if not m:
        return []
    ident = m.group(1)
    return [ident]

def extract_order_by_unqualified_columns(sql_text: str) -> List[str]:
    """Extract unqualified column tokens from ORDER BY clause(s)."""
    if not sql_text:
        return []
    cols: List[str] = []
    for m in re.finditer(r"(?is)\border\s+by\s+(.*?)(?:(?:limit|offset|fetch)\b|$)", sql_text):
        clause = m.group(1)
        # split by commas not within parentheses
        parts = re.split(r",(?![^()]*\))", clause)
        for p in parts:
            token = p.strip()
            # remove ASC/DESC and NULLS FIRST/LAST
            token = re.sub(r"(?i)\b(asc|desc)\b", "", token)
            token = re.sub(r"(?i)nulls\s+(first|last)", "", token)
            token = token.strip()
            # skip function calls and qualified refs (handled elsewhere)
            if not token or '(' in token or '.' in token:
                continue
            # simple identifier
            m2 = re.match(r"^[A-Za-z_][\w$]*$|^\"[^\"]+\"$", token)
            if m2:
                cols.append(token.replace('"', ''))
    return cols

def find_missing_order_by_columns(sql_text: str, schema_catalog: Dict[str, set]) -> Tuple[List[str], List[str]]:
    """Return (missing_specs, ambiguous_notes) for unqualified ORDER BY columns.
    If only one table is present in FROM/JOIN aliases, validate against it.
    If multiple tables, attempt to resolve by unique match across catalog; otherwise mark ambiguous.
    """
    aliases = extract_table_aliases(sql_text)
    base_tables = list(set(aliases.values())) if aliases else []
    unq_cols = extract_order_by_unqualified_columns(sql_text)
    missing: List[str] = []
    ambiguous_notes: List[str] = []
    if not unq_cols:
        return missing, ambiguous_notes

    # Precompute reverse index: column -> tables containing it
    col_to_tables: Dict[str, List[str]] = {}
    for t, cols in schema_catalog.items():
        for c in cols:
            col_to_tables.setdefault(c.upper(), []).append(t)

    for col in unq_cols:
        col_u = col.upper()
        candidate_tables = col_to_tables.get(col_u, [])
        if len(base_tables) == 1:
            base = base_tables[0]
            if base not in schema_catalog or col_u not in {c.upper() for c in schema_catalog.get(base, set())}:
                missing.append(f"{base}.{col}")
        else:
            if len(candidate_tables) == 1:
                # Suggest qualification if not already
                base = candidate_tables[0]
                # not strictly missing, but enforce qualification; if base not in aliases, we still require LLM to adjust
                # We'll not add to missing, but provide ambiguous note
                ambiguous_notes.append(f"Qualify ORDER BY {col} with table {base} (unqualified reference).")
            elif len(candidate_tables) == 0:
                missing.append(col)
            else:
                ambiguous_notes.append(
                    f"ORDER BY column {col} exists in multiple tables: {', '.join(candidate_tables)}. Qualify explicitly."
                )
    return sorted(list(dict.fromkeys(missing))), ambiguous_notes

def extract_order_by_qualified_pairs(sql_text: str) -> List[Tuple[str, str]]:
    """Extract qualified pairs like qualifier.column specifically from ORDER BY."""
    pairs: List[Tuple[str, str]] = []
    if not sql_text:
        return pairs
    # pull order by section(s)
    for m in re.finditer(r"(?is)\border\s+by\s+(.*?)(?:(?:limit|offset|fetch)\b|$)", sql_text):
        clause = m.group(1)
        for m2 in re.finditer(r"(?i)(\"[^\"]+\"|[A-Za-z_][\w$]*)\s*\.\s*(\"[^\"]+\"|[A-Za-z_][\w$]*)", clause):
            left = m2.group(1)
            right = m2.group(2)
            pairs.append((left.replace('"', ''), right.replace('"', '')))
    return pairs

def find_order_by_qualifiers_not_in_from(sql_text: str) -> List[str]:
    """Return order-by qualifiers that aren't among FROM/JOIN aliases/base tables."""
    aliases = extract_table_aliases(sql_text)
    alias_keys = set(k.upper() for k in aliases.keys())
    base_values = set(v.upper() for v in aliases.values())
    invalid: List[str] = []
    for qual, _col in extract_order_by_qualified_pairs(sql_text):
        q_u = qual.upper()
        if q_u not in alias_keys and q_u not in base_values:
            invalid.append(qual)
    return sorted(list(dict.fromkeys(invalid)))

def get_present_base_tables(sql_text: str) -> List[str]:
    """Return list of base table simple names present in FROM/JOIN."""
    aliases = extract_table_aliases(sql_text)
    bases = list(dict.fromkeys(aliases.values())) if aliases else []
    return [b.replace('"', '') for b in bases]

def is_id_like(col: str) -> bool:
    cu = col.upper()
    return cu == 'ID' or cu.endswith('_ID') or cu.endswith('ID') or cu.endswith('_KEY')

# Preferred join keys between known table pairs (simple names, case-insensitive)
# Example: DT_DRIVERS <-> DT_DRIVER_PERFORMANCE should join on DriverID
PREFERRED_JOIN_KEYS: Dict[Tuple[str, str], str] = {
    ("DT_DRIVERS", "DT_DRIVER_PERFORMANCE"): "DriverID",
    ("DT_DRIVER_PERFORMANCE", "DT_DRIVERS"): "DriverID",
}

def lookup_preferred_join_key(table_a: str, table_b: str, catalog: Dict[str, set]) -> Optional[str]:
    a_u = table_a.upper()
    b_u = table_b.upper()
    pref = PREFERRED_JOIN_KEYS.get((a_u, b_u))
    if not pref:
        return None
    # Ensure both tables actually have the column (case-insensitive)
    cols_a = {c.upper(): c for c in catalog.get(table_a, set())}
    cols_b = {c.upper(): c for c in catalog.get(table_b, set())}
    pref_u = pref.upper()
    if pref_u in cols_a and pref_u in cols_b:
        # Return the preferred casing (from A or provided)
        return cols_a.get(pref_u, pref)
    return None

def choose_best_join_key(table_a: str, table_b: str, catalog: Dict[str, set]) -> Optional[str]:
    """Pick a reasonable join column name shared by both tables, preferring *_ID/ID/_KEY."""
    # 1) Preferred mapping first
    preferred = lookup_preferred_join_key(table_a, table_b, catalog)
    if preferred:
        return preferred
    # 2) Otherwise find any common columns
    cols_a = {c.upper(): c for c in catalog.get(table_a, set())}
    cols_b = {c.upper(): c for c in catalog.get(table_b, set())}
    common = [u for u in cols_a.keys() if u in cols_b]
    if not common:
        return None
    # Prefer id-like
    id_like = [u for u in common if is_id_like(u)]
    if id_like:
        # Return original casing from table A
        return cols_a[id_like[0]]
    # fallback to first common column
    return cols_a[common[0]]

def suggest_join_instructions(missing_table: str, present_tables: List[str], catalog: Dict[str, set]) -> Optional[str]:
    """Suggest a JOIN instruction string for LLM given missing table and present ones."""
    if not present_tables:
        return None
    # pick best present table by availability of id-like join key
    best = None
    best_key = None
    for pt in present_tables:
        key = choose_best_join_key(pt, missing_table, catalog)
        if key:
            best = pt
            best_key = key
            break
    if not best:
        # Try any common column
        for pt in present_tables:
            key = choose_best_join_key(pt, missing_table, catalog)
            if key:
                best = pt
                best_key = key
                break
    if not best or not best_key:
        return None
    return f"Join {missing_table} to {best} using column {best_key} (e.g., ON {best}.{best_key} = {missing_table}.{best_key})."

def auto_repair_missing_orderby_join(
    sql_text: str,
    database: str,
    schema: str,
    catalog: Dict[str, set]
) -> Optional[str]:
    """If ORDER BY uses qualifier not present in FROM/JOIN, add a JOIN using shared key.
    Heuristic string rewrite intended to fix simple cases like ordering by a column from another table.
    """
    if not sql_text:
        return None
    missing_quals = find_order_by_qualifiers_not_in_from(sql_text)
    if not missing_quals:
        return None
    present_bases = get_present_base_tables(sql_text)
    if not present_bases:
        return None

    # Only handle first missing qualifier for now
    mtable = missing_quals[0]
    # Pick a base table with a viable join key
    chosen_base = None
    chosen_key = None
    for base in present_bases:
        key = choose_best_join_key(base, mtable, catalog)
        if key:
            chosen_base = base
            chosen_key = key
            break
    if not (chosen_base and chosen_key):
        return None

    q_db = '"' + database.replace('"', '""') + '"'
    q_schema = '"' + schema.replace('"', '""') + '"'
    fq_missing = f"{q_db}.{q_schema}.{mtable}"

    # Find the FROM <target> portion to append JOIN
    m = re.search(r"(?is)(\bfrom\s+[^\n;]+?)\s*(\bwhere\b|\bgroup\s+by\b|\border\s+by\b|\blimit\b|$)", sql_text)
    if not m:
        return None
    from_clause = m.group(1)
    tail_start = m.start(2) if m.group(2) else len(sql_text)

    # If join already includes the missing table, skip
    if re.search(rf"(?i)\bjoin\s+\Q{fq_missing}\E\b", sql_text):
        return None

    join_snippet = f" JOIN {fq_missing} ON {chosen_base}.{chosen_key} = {mtable}.{chosen_key}"
    new_from = from_clause + join_snippet
    repaired = sql_text[:m.start(1)] + new_from + sql_text[m.start(2):] if m.group(2) else sql_text[:m.start(1)] + new_from
    return repaired

def insert_into_pipeline_factory(session, pipeline_id: str, source_table: str, sql_snippet: str, 
                                target_dt_name: str, lag_minutes: int, warehouse: str) -> bool:
    """Insert the generated SQL into the PIPELINE_CONFIG table."""
    try:
        insert_query = f"""
        INSERT INTO PIPELINE_CONFIG (
            pipeline_id, 
            source_table_name, 
            transformation_sql_snippet, 
            target_dt_name, 
            lag_minutes, 
            warehouse, 
            status
        ) VALUES (
            '{pipeline_id}',
            '{source_table}',
            $${sql_snippet}$$,
            '{target_dt_name}',
            {lag_minutes},
            '{warehouse}',
            'PENDING'
        )
        """
        session.sql(insert_query).collect()
        return True
    except Exception as e:
        st.error(f"Error inserting into pipeline factory: {str(e)}")
        return False

def main():
    st.title("🔍 Natural Language to SQL Pipeline Generator")
    st.markdown("Transform your business questions into SQL queries and add them to the pipeline factory.")
    render_version_badge()
    
    # Initialize Snowflake session
    session = get_snowflake_session()
    if not session:
        return
    
    # Sidebar for connection settings
    with st.sidebar:
        st.header("🔧 Connection Settings")
        
        # Database selection
        databases = get_databases(session)
        if not databases:
            st.error("No databases found or connection failed.")
            return
            
        selected_database = st.selectbox("Select Database", databases)
        
        # Schema selection
        schemas = get_schemas(session, selected_database)
        if not schemas:
            st.error("No schemas found in the selected database.")
            return
            
        selected_schema = st.selectbox("Select Schema", schemas)
        
        st.success(f"Connected to: {selected_database}.{selected_schema}")

        # Advanced controls for prompt size limits
        with st.expander("Advanced (Prompt Size Limits)", expanded=False):
            max_tables = st.number_input(
                "Max tables to include in schema context",
                min_value=5,
                max_value=200,
                value=20,
                step=1
            )
            max_columns_per_table = st.number_input(
                "Max columns per table",
                min_value=5,
                max_value=200,
                value=12,
                step=1
            )
    
    # Main content area
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.header("📝 Ask Your Question")
        
        # User question input
        user_question = st.text_area(
            "What question do you want to answer from the data?",
            placeholder="e.g., Show me the top 5 selling products in the last quarter",
            height=100
        )
        
        # Generate SQL button
        if st.button("🚀 Generate SQL with Cortex AI", type="primary", use_container_width=True):
            if not user_question.strip():
                st.warning("Please enter a question first.")
                return
                
            with st.spinner("Fetching schema metadata..."):
                schema_metadata, schema_truncated = get_schema_metadata(
                    session, selected_database, selected_schema, max_tables, max_columns_per_table
                )
                # Persist for use after rerun
                st.session_state['schema_metadata'] = schema_metadata
                st.session_state['schema_truncated'] = schema_truncated
                st.session_state['selected_database'] = selected_database
                st.session_state['selected_schema'] = selected_schema
                
            if not schema_metadata:
                st.error("Could not fetch schema metadata. Please check your database and schema selection.")
                return
                
            with st.spinner("Generating SQL with Cortex AI..."):
                generated_sql = generate_sql_with_cortex(
                    session, schema_metadata, user_question, selected_database, selected_schema, schema_truncated
                )
                if generated_sql:
                    # Best-effort fully qualify any unqualified identifiers
                    generated_sql = qualify_sql(generated_sql, selected_database, selected_schema)
            
            if generated_sql:
                st.session_state['generated_sql'] = generated_sql
                st.session_state['user_question'] = user_question
                st.rerun()
    
    with col2:
        st.header("📊 Schema Info")
        if selected_database and selected_schema:
            with st.expander("View Schema Metadata", expanded=False):
                schema_info, _schema_trunc = get_schema_metadata(
                    session, selected_database, selected_schema, max_tables, max_columns_per_table
                )
                if schema_info:
                    # Format for better display
                    tables = schema_info.split("; ")
                    for table_info in tables:
                        if table_info.strip():
                            st.text(table_info)
    
    # Display generated SQL if available
    if 'generated_sql' in st.session_state:
        st.header("🎯 Generated SQL Query")
        st.code(st.session_state['generated_sql'], language='sql')
        
        # Validate and auto preview results with retries and schema checking
        st.header("🔎 Preview Results (Top 3 rows)")
        validation_messages: List[str] = []
        preview_df = None
        max_retries = 5

        # Ensure schema metadata is available across reruns
        schema_metadata = st.session_state.get('schema_metadata')
        schema_truncated = st.session_state.get('schema_truncated', False)
        if not schema_metadata:
            schema_metadata, schema_truncated = get_schema_metadata(
                session, selected_database, selected_schema, max_tables, max_columns_per_table
            )
            st.session_state['schema_metadata'] = schema_metadata
            st.session_state['schema_truncated'] = schema_truncated

        # Precompute schema catalog for basic validation on failure
        schema_catalog = get_schema_catalog(session, selected_database, selected_schema)

        # Try to preview up to N times, validating columns before execution
        last_error = ""
        for attempt in range(1, max_retries + 1):
            try:
                # First ask the model to validate/fix the candidate SQL using schema metadata and catalog
                # Try primary model first
                validated = validate_sql_with_cortex(
                    session,
                    schema_metadata,
                    schema_catalog,
                    st.session_state['generated_sql'],
                    selected_database,
                    selected_schema,
                    "",
                    'snowflake-arctic'
                )
                if validated:
                    validated = qualify_sql(validated, selected_database, selected_schema)
                    st.session_state['generated_sql'] = validated
                else:
                    # Fallback to a lighter model to avoid token limit issues
                    validated_alt = validate_sql_with_cortex(
                        session,
                        schema_metadata,
                        schema_catalog,
                        st.session_state['generated_sql'],
                        selected_database,
                        selected_schema,
                        "",
                        'mistral-large'
                    )
                    if validated_alt:
                        validated_alt = qualify_sql(validated_alt, selected_database, selected_schema)
                        st.session_state['generated_sql'] = validated_alt

                # Attempt heuristic auto-repair for missing ORDER BY qualifier JOINs
                repaired = auto_repair_missing_orderby_join(
                    st.session_state['generated_sql'],
                    selected_database,
                    selected_schema,
                    schema_catalog
                )
                if repaired:
                    st.session_state['generated_sql'] = repaired

                preview_sql = build_preview_sql(st.session_state['generated_sql'])
                with st.spinner(f"Running preview (attempt {attempt}/{max_retries})..."):
                    df = session.sql(preview_sql).to_pandas()
                if df is not None and len(df.index) > 0:
                    preview_df = df
                    break
                else:
                    # No rows; ask LLM to regenerate with instruction to ensure rows
                    validation_messages.append("No rows returned; retrying with stronger constraints.")
                    addl = "Ensure the query returns at least a few rows based on available data. Avoid referencing non-existent columns. Add necessary JOINs if ORDER BY/SELECT columns come from different tables."
                    regenerated = generate_sql_with_cortex(
                        session,
                        schema_metadata,
                        st.session_state['user_question'],
                        selected_database,
                        selected_schema,
                        False,
                        addl,
                        'snowflake-arctic'
                    )
                    if regenerated:
                        regenerated = qualify_sql(regenerated, selected_database, selected_schema)
                        st.session_state['generated_sql'] = regenerated
                    else:
                        # fallback model
                        regenerated_alt = generate_sql_with_cortex(
                            session,
                            schema_metadata,
                            st.session_state['user_question'],
                            selected_database,
                            selected_schema,
                            False,
                            addl,
                            'mistral-large'
                        )
                        if regenerated_alt:
                            regenerated_alt = qualify_sql(regenerated_alt, selected_database, selected_schema)
                            st.session_state['generated_sql'] = regenerated_alt
                        else:
                            last_error = "Regeneration failed"
            except Exception as e:
                last_error = str(e)
                # Attempt to detect invalid identifiers and guide a regeneration
                referenced = extract_referenced_tables(st.session_state['generated_sql'])
                missing_targets = []
                for ident in referenced:
                    # Consider only last part as table when fully qualified
                    table_simple = ident.split('.')[-1].replace('"', '')
                    if table_simple not in schema_catalog:
                        missing_targets.append(ident)
                # If ORDER BY references a qualifier not in FROM/JOIN, suggest JOIN
                invalid_order_qualifiers = find_order_by_qualifiers_not_in_from(
                    st.session_state['generated_sql']
                )
                join_hints: List[str] = []
                if invalid_order_qualifiers:
                    present = get_present_base_tables(st.session_state['generated_sql'])
                    for missing_table in invalid_order_qualifiers:
                        hint = suggest_join_instructions(missing_table, present, schema_catalog)
                        if hint:
                            join_hints.append(hint)
                invalid_from_error = parse_invalid_identifier_from_error(last_error)
                addl_parts = []
                if missing_targets:
                    addl_parts.append(
                        "Do not reference these missing tables: " + ", ".join(missing_targets)
                    )
                if invalid_from_error:
                    addl_parts.append(
                        "Remove or replace this invalid identifier: " + ", ".join(invalid_from_error)
                    )
                if join_hints:
                    addl_parts.append("Add necessary JOINs based on schema relationships:")
                    for j in join_hints:
                        addl_parts.append("- " + j)
                addl_parts.append("Use only tables and columns present in the provided schema context.")
                addl_parts.append("If a column doesn't exist, choose appropriate alternatives to ensure the query runs.")
                addl = "\n".join(addl_parts)
                regenerated = validate_sql_with_cortex(
                    session,
                    schema_metadata,
                    schema_catalog,
                    st.session_state['generated_sql'] + "\n-- Fix per errors above",
                    selected_database,
                    selected_schema,
                    addl,
                    'snowflake-arctic'
                )
                if regenerated:
                    regenerated = qualify_sql(regenerated, selected_database, selected_schema)
                    st.session_state['generated_sql'] = regenerated
                else:
                    # Fallback model for regeneration
                    regenerated_alt = validate_sql_with_cortex(
                        session,
                        schema_metadata,
                        schema_catalog,
                        st.session_state['generated_sql'] + "\n-- Fix per errors above",
                        selected_database,
                        selected_schema,
                        addl,
                        'mistral-large'
                    )
                    if regenerated_alt:
                        regenerated_alt = qualify_sql(regenerated_alt, selected_database, selected_schema)
                        st.session_state['generated_sql'] = regenerated_alt
                    else:
                        validation_messages.append("Regeneration failed after error: " + last_error)

        if preview_df is not None:
            st.caption("Showing up to 3 rows")
            st.dataframe(preview_df, use_container_width=True)
            st.session_state['preview_df'] = preview_df
            if validation_messages:
                with st.expander("Validation notes", expanded=False):
                    for msg in validation_messages:
                        st.write(msg)
        else:
            st.error("Unable to generate a working query that returns rows after multiple attempts.")
            if last_error:
                with st.expander("Last error", expanded=False):
                    st.code(last_error)

        # Pipeline Factory Integration
        st.header("🏭 Add to Pipeline Factory")
        
        col1, col2 = st.columns(2)
        
        with col1:
            pipeline_id = st.text_input(
                "Pipeline ID",
                placeholder="e.g., top_products_q3_pipeline",
                help="Unique identifier for your pipeline"
            )
            
            source_table = st.text_input(
                "Source Table Name",
                placeholder="e.g., RAW.SALES.PRODUCTS",
                help="Fully qualified source table name"
            )
            
            target_dt_name = st.text_input(
                "Target Dynamic Table Name",
                placeholder="e.g., ANALYTICS.STAGE.TOP_PRODUCTS_DT",
                help="Fully qualified target dynamic table name"
            )
        
        with col2:
            lag_minutes = st.number_input(
                "Lag Minutes",
                min_value=1,
                max_value=10080,  # 1 week
                value=5,
                help="Refresh lag in minutes"
            )
            
            warehouse = st.text_input(
                "Warehouse",
                value="PIPELINE_WH",
                help="Warehouse to use for the dynamic table"
            )
        
        if st.button("➕ Add to Pipeline Factory", type="primary", use_container_width=True):
            if not all([pipeline_id, source_table, target_dt_name]):
                st.warning("Please fill in all required fields.")
            else:
                success = insert_into_pipeline_factory(
                    session, 
                    pipeline_id, 
                    source_table, 
                    st.session_state['generated_sql'],
                    target_dt_name,
                    lag_minutes,
                    warehouse
                )
                
                if success:
                    st.success(f"✅ Pipeline '{pipeline_id}' has been successfully added to the factory!")
                    st.balloons()
                    
                    # Clear the session state
                    if 'generated_sql' in st.session_state:
                        del st.session_state['generated_sql']
                    if 'user_question' in st.session_state:
                        del st.session_state['user_question']

if __name__ == "__main__":
    main()
