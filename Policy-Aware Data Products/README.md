# Policy-Aware Data Products

Artifacts
- sql/01_policy_projector_dt.sql → POLICY_PRODUCT_PROJECTIONS_DT
- sql/02_executor.sql → POLICY_PRODUCT_LOG, APPLY_POLICY_PRODUCTS() + APPLY_POLICY_PRODUCTS_TASK

Deploy (order)
1) 01_policy_projector_dt.sql
2) 02_executor.sql (creates log, procedure, and task; task resumed)

Notes
- Projections generate policy-safe views into a companion *_PRODUCT schema.
- Extend to incorporate masking policy metadata and regionalization tags.
