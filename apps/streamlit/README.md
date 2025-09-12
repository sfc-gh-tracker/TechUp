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
1. Upload this folder to a stage, then create the app:
```sql
create or replace streamlit PIPELINE_FACTORY_APP
  root_location = '@your_stage/apps/streamlit'
  query_warehouse = PIPELINE_WH
  main_file = 'app.py';
```

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
