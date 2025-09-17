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
from typing import Dict, List, Optional
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

def get_schema_metadata(session, database: str, schema: str) -> str:
    """Fetch table and column metadata for the selected database and schema."""
    try:
        query = f"""
        SELECT 
            table_name,
            column_name,
            data_type
        FROM {database}.INFORMATION_SCHEMA.COLUMNS
        WHERE table_schema = '{schema}'
        ORDER BY table_name, ordinal_position
        """
        result = session.sql(query).collect()
        
        # Group columns by table
        tables_info = {}
        for row in result:
            table_name = row['TABLE_NAME']
            if table_name not in tables_info:
                tables_info[table_name] = []
            tables_info[table_name].append(f"{row['COLUMN_NAME']} ({row['DATA_TYPE']})")
        
        # Format as readable string
        schema_info = []
        for table, columns in tables_info.items():
            schema_info.append(f"Table: {table}, Columns: {', '.join(columns)}")
        
        return "; ".join(schema_info)
    except Exception as e:
        st.error(f"Error fetching schema metadata: {str(e)}")
        return ""

def generate_sql_with_cortex(session, schema_metadata: str, user_question: str, database: str, schema: str) -> Optional[str]:
    """Generate SQL using Snowflake Cortex AI."""
    try:
        system_prompt = "You are an expert Snowflake SQL data analyst. Your task is to write a single, clean, and efficient SQL query to answer the user's question based on the provided schema."
        
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
                schema_metadata = get_schema_metadata(session, selected_database, selected_schema)
                
            if not schema_metadata:
                st.error("Could not fetch schema metadata. Please check your database and schema selection.")
                return
                
            with st.spinner("Generating SQL with Cortex AI..."):
                generated_sql = generate_sql_with_cortex(
                    session, schema_metadata, user_question, selected_database, selected_schema
                )
            
            if generated_sql:
                st.session_state['generated_sql'] = generated_sql
                st.session_state['user_question'] = user_question
                st.rerun()
    
    with col2:
        st.header("📊 Schema Info")
        if selected_database and selected_schema:
            with st.expander("View Schema Metadata", expanded=False):
                schema_info = get_schema_metadata(session, selected_database, selected_schema)
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
