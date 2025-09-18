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
from typing import Dict, List, Optional, Tuple
from snowflake.snowpark.context import get_active_session

# Page configuration
st.set_page_config(
    page_title="Natural Language to SQL Pipeline Generator",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

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

def generate_sql_with_cortex(session, schema_metadata: str, user_question: str, database: str, schema: str, schema_truncated: bool = False) -> Optional[str]:
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
"""

        # Construct the Cortex query - escape single quotes properly
        system_prompt_escaped = system_prompt.replace("'", "''")
        user_prompt_escaped = user_prompt.replace("'", "''")
        
        cortex_query = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            'snowflake-arctic',
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
                
            if not schema_metadata:
                st.error("Could not fetch schema metadata. Please check your database and schema selection.")
                return
                
            with st.spinner("Generating SQL with Cortex AI..."):
                generated_sql = generate_sql_with_cortex(
                    session, schema_metadata, user_question, selected_database, selected_schema, schema_truncated
                )
            
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
