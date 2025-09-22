# Query Pattern Optimizer

Artifacts
- sql/01_usage_aggregation_dt.sql → QPO_USAGE_DT
- sql/02_recommendations_dt.sql → QPO_RECOMMENDATIONS_DT
- sql/03_pending_actions_dt.sql → QPO_PENDING_ACTIONS_DT
- sql/04_executor.sql → QPO_ACTION_LOG, QPO_RUN_ACTIONS() + QPO_RUN_ACTIONS_TASK

Deploy (order)
1) 01_usage_aggregation_dt.sql
2) 02_recommendations_dt.sql
3) 03_pending_actions_dt.sql
4) 04_executor.sql (creates log, procedure, and task; task is resumed)

Notes
- Proposed DDLs are prefixed with /* REVIEW */; remove to auto-apply.
- ROI signal is a heuristic; tune thresholds per account.

Google Slides
- Generate one-slide deck and save link:
  - Place credentials.json at repo root (Google OAuth for Slides/Drive)
  - pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib
  - python3 ../slides_generator_gdrive.py
  - Link will be written to Query Pattern Optimizer/slides_link.txt
