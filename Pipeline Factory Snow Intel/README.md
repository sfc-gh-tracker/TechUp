# Natural Language to SQL Pipeline Generator

A Streamlit application that converts natural language business questions into SQL queries using Snowflake Cortex AI, and integrates with an existing pipeline factory system.

## Features

- **Natural Language Processing**: Ask business questions in plain English
- **AI-Powered SQL Generation**: Choose Snowflake Intelligence AISQL or Cortex COMPLETE
- **Dynamic Schema Discovery**: Automatically discovers and uses your database schema
- **Pipeline Factory Integration**: Seamlessly adds generated queries to your existing pipeline system
- **Interactive UI**: Clean, intuitive interface for business users

## Setup Instructions

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Snowflake Connection

Edit `.streamlit/secrets.toml` with your Snowflake connection details:

```toml
[connections.snowflake]
account = "your-account-identifier"
user = "your-username" 
password = "your-password"
role = "your-role"
warehouse = "your-warehouse"
database = "your-database"
schema = "your-schema"
```

### 3. Set Up Pipeline Factory (if not already done)

Run the SQL scripts in the `sql/pipeline_factory/` directory in order:

1. `01_pipeline_config.sql` - Creates the configuration table
2. `02_run_pipeline_factory_sp.sql` - Creates the pipeline execution stored procedure
3. `03_dynamic_tables.sql` - Sets up monitoring and orchestration

### 4. Run the Application

```bash
streamlit run app.py
```

## How It Works

### Page 1: SQL Generation UI

1. **Database Connection**: Uses Streamlit's native Snowflake connector
2. **Schema Selection**: Dynamic dropdowns for database and schema selection
3. **Question Input**: Text area for natural language business questions
4. **AI Processing**: 
   - Fetches schema metadata from `INFORMATION_SCHEMA.COLUMNS`
   - Backend selection:
     - Snowflake Intelligence AISQL: `select snowflake.cortex.aisql(parse_json('<payload>'))`
     - Cortex COMPLETE: `select snowflake.cortex.complete('<model>', '<prompt>')`
5. **Pipeline Integration**: Allows users to add generated SQL to the pipeline factory

### Pipeline Factory Integration

The application integrates with your existing `PIPELINE_CONFIG` table structure:

- `pipeline_id`: Unique identifier for the pipeline
- `source_table_name`: Source table for the transformation
- `transformation_sql_snippet`: The AI-generated SQL query
- `target_dt_name`: Target dynamic table name
- `lag_minutes`: Refresh frequency in minutes
- `warehouse`: Snowflake warehouse to use
- `status`: Set to 'PENDING' for new pipelines

## Example Usage

1. Select your database and schema from the sidebar
2. Ask a business question like: "Show me the top 5 customers by total order value in the last 6 months"
3. Click "Generate SQL with Cortex AI"
4. Review the generated SQL query
5. Fill in the pipeline details and click "Add to Pipeline Factory"

## Security Features

- SQL injection protection through parameterized queries
- Validation of transformation SQL snippets
- Restricted SQL operations (no DDL/DML outside of SELECT)
- Proper identifier quoting and escaping

## Troubleshooting

### Connection Issues
- Verify your Snowflake credentials in `secrets.toml`
- Ensure your role has access to the required databases and schemas
- Check that Cortex AI is enabled in your Snowflake account

### Permission Issues
- Ensure your role has `SELECT` permissions on `INFORMATION_SCHEMA`
- Verify `INSERT` permissions on the `PIPELINE_CONFIG` table
- Check that Cortex functions are available in your account

### AI Generation Issues
- Verify Cortex AI and Snowflake Intelligence are enabled and accessible
- Ensure AISQL is available in your region or enable Cross-region inference
- Ensure your schema metadata is being fetched correctly

## References

- Snowflake Intelligence: [docs](https://docs.snowflake.com/en/user-guide/snowflake-cortex/snowflake-intelligence)
- Cortex AISQL: [docs](https://docs.snowflake.com/en/user-guide/snowflake-cortex/aisql)

## Google Slides

Generate a one-slide deck and write link to `slides_link.txt`:

1. Place `credentials.json` at repo root (Google OAuth for Slides/Drive)
2. Install dependencies:
   - `pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib`
3. Run from repo root:
   - `python3 slides_generator_gdrive.py`
4. Link saved to `Pipeline Factory/slides_link.txt`