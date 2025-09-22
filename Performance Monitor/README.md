# Self-Optimizing Performance Monitor

Artifacts
- sql/00_ingest_metering.sql → TECHUP.AUDIT.WAREHOUSE_METERING_STG + task
- sql/01_warehouse_performance_dt.sql → WAREHOUSE_PERFORMANCE_DT
- sql/02_optimization_recommendations_dt.sql → OPTIMIZATION_RECOMMENDATIONS_DT
- sql/03_pending_ddl_actions_dt.sql → PENDING_DDL_ACTIONS_DT
- sql/04_run_pending_actions.sql → ACTION_LOG, RUN_PENDING_ACTIONS() + RUN_ACTIONS_TASK

Deploy (order)
1) 00_ingest_metering.sql
2) 01_warehouse_performance_dt.sql
3) 02_optimization_recommendations_dt.sql
4) 03_pending_ddl_actions_dt.sql
5) 04_run_pending_actions.sql (creates log, procedure, task; task resumed)

Google Slides
- Generate one-slide deck and save link:
  - Place credentials.json at repo root (Google OAuth for Slides/Drive)
  - pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib
  - python3 ../slides_generator_gdrive.py
  - Link will be written to Performance Monitor/slides_link.txt
