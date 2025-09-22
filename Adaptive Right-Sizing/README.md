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
