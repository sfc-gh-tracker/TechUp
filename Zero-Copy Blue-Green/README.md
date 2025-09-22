# Zero-Copy Blue/Green for DT Graphs

Artifacts
- sql/01_blue_green_plan_dt.sql → BG_DEPLOY_PLAN_DT (plans branches)
- sql/02_executor.sql → BG_ACTION_LOG, BG_APPLY() + BG_APPLY_TASK

Deploy (order)
1) 01_blue_green_plan_dt.sql
2) 02_executor.sql (creates log, procedure, and task; task resumed)

Notes
- Proposed commands are prefixed with /* REVIEW */ by default. Remove the marker to auto-apply.
- Extend plan DT to include validation steps for the branch (e.g., count checks, drift checks) before promotion.
