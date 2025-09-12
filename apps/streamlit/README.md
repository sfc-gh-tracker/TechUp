# Snowflake Streamlit: SQL Pipeline Factory

This app lets users describe the data they want in natural language. It uses Snowflake Cortex to generate a SQL SELECT, validates and previews it, and inserts a `PENDING` row into `PIPELINE_CONFIG` to create a Dynamic Table via the factory.

## Features
- Prompt-to-SQL via Cortex with schema grounding
- Validation (read-only, allowlist, EXPLAIN + LIMIT 0)
- Data preview (LIMIT 50)
- Insert into `PIPELINE_CONFIG` on approval

## Prereqs
- Snowflake account with Cortex enabled
- Role with privileges:
  - USAGE on desired database/schema(s), SELECT on whitelisted tables
  - INSERT on `PIPELINE_CONFIG`
  - USAGE on warehouse (e.g., `PIPELINE_WH`)
- Streamlit in Snowflake enabled (or run locally with Snowpark connector)

## Run in Snowflake (recommended)
This app is designed to run inside Snowflake Streamlit using the active session (no credentials needed).

1) Create a stage and upload the app files (from your workstation):
```sql
create or replace stage APP_STAGE;
```
Then in Snowsight or SnowSQL:
```sql
-- Upload the local folder to the stage (use Snowsight UI Upload, or SnowSQL PUT):
-- SnowSQL example:
-- !put file://apps/streamlit/* @APP_STAGE/apps/streamlit auto_compress=false overwrite=true;
```

2) Create the Streamlit app referencing the stage path:
```sql
create or replace streamlit PIPELINE_FACTORY_APP
  root_location = '@APP_STAGE/apps/streamlit'
  query_warehouse = PIPELINE_WH
  main_file = 'app.py';
```

3) Open PIPELINE_FACTORY_APP in Snowsight and run it. The app uses the active session via `get_active_session()` and your current role/warehouse.

## Local dev (optional)
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
streamlit run apps/streamlit/app.py
```

Set env vars for Snowflake connection (local):
- SNOWFLAKE_ACCOUNT
- SNOWFLAKE_USER
- SNOWFLAKE_PASSWORD (or use key pair)
- SNOWFLAKE_ROLE
- SNOWFLAKE_WAREHOUSE
- SNOWFLAKE_DATABASE
- SNOWFLAKE_SCHEMA

## Notes
- The app only writes to `PIPELINE_CONFIG`; object creation is delegated to the existing factory SP + DT orchestrator.
- Adjust allowlists and default warehouse as needed in `config.py`.
- In Snowflake deployment, the app uses the active Snowflake session; locally it reads connection info from environment variables.
