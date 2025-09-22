# Adaptive Warehouse Right-Sizing

Artifacts
- sql/01_right_sizing_policy_dt.sql → RIGHT_SIZING_POLICY_DT
- sql/02_executor.sql → RIGHT_SIZING_LOG, APPLY_RIGHT_SIZING() + APPLY_RIGHT_SIZING_TASK

Deploy (order)
1) 01_right_sizing_policy_dt.sql
2) 02_executor.sql (creates log, procedure, and hourly task; task resumed)

Notes
- Policy chooses SMALL/MEDIUM/LARGE heuristically; tune thresholds.
- Multi-cluster is enabled when queueing is observed.

Google Slides
- Generate one-slide deck and save link:
  - Place credentials.json at repo root (Google OAuth for Slides/Drive)
  - pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib
  - python3 slides_generator_gdrive.py
  - Link will be written to Adaptive Right-Sizing/slides_link.txt
