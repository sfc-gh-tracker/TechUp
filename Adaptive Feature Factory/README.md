# Adaptive Feature Factory

An advanced, zero-ops demo that turns Snowflake Dynamic Tables into an autonomous feature governance plane for AI. It continuously measures feature health, detects drift, and proposes (or applies) fixes — without external schedulers or orchestration.

What this shows beyond typical docs:
- Dynamic Tables as an always-on control loop (monitor → diagnose → remediate), not just materialization.
- Enterprise AI readiness: feature drift detection, quality enforcement, and safe remediation paths.
- Truly zero-ops: 100% inside Snowflake (DTs + 1 lightweight task/proc).

Contents
- sql/01_feature_stats_dt.sql: DT_FEATURE_STATS aggregates rolling stats (mean, stddev, quantiles) across feature sets.
- sql/02_drift_alerts_dt.sql: DT_DRIFT_ALERTS compares live stats vs a managed baseline table and flags drift types.
- sql/03_patch_candidates_dt.sql: DT_PATCH_CANDIDATES generates proposed SQL patches (commented for review by default).
- sql/04_run_feature_patches.sql: FEATURE_PATCH_LOG, APPLY_FEATURE_PATCHES() stored procedure, and scheduled task.

Where this is beneficial
- New AI workloads where feature distributions change as data, business rules, or seasonality evolve.
- Teams that want self-healing pipelines without external orchestration or MLOps frameworks.
- Regulated/enterprise environments needing auditable controls and low-op overhead.

When not to use
- If feature transformations are fully managed by external ML platforms (and Snowflake only stores final artifacts).
- If you require complex remediation policies that must be human-only; keep this in “propose only” mode.

How to deploy (order)
1) Run 01_feature_stats_dt.sql
2) Run 02_drift_alerts_dt.sql
3) Run 03_patch_candidates_dt.sql
4) Run 04_run_feature_patches.sql (creates log, proc, and task)

Operating modes
- Propose-only (default): Patches in DT_PATCH_CANDIDATES are commented with /* REVIEW */. Curate them, remove the marker, and let the task apply.
- Auto-apply (demo): Adjust generation logic to emit vetted SQL without the review marker.

Baselines
- FEATURE_STATS_BASELINE is created empty. Capture a baseline by inserting a snapshot from DT_FEATURE_STATS (e.g., month-end) to lock expected distributions.

20-minute demo flow (suggested)
0–3m: Frame zero-ops control loop (monitor → detect → propose → apply). Value: autonomous data quality for AI.
3–8m: Show DT_FEATURE_STATS (always fresh), explain computations and lag settings.
8–12m: Show DT_DRIFT_ALERTS vs baseline; simulate drift (e.g., backfill or inject outliers) and watch flags flip.
12–16m: Show DT_PATCH_CANDIDATES; discuss safe defaults and governance (commented SQL, code review).
16–19m: Run APPLY_FEATURE_PATCHES task/proc; show FEATURE_PATCH_LOG. Emphasize auditability and minimal ops.
19–20m: Wrap with enterprise implications: less pipeline toil, trustworthy features for AI, all in Snowflake.

Talking points for Solution Engineers
- Dynamic Tables can drive self-governance loops, not just incremental views.
- Zero external schedulers: DTs + one task satisfies SRE/CTO concerns on ownership and failure domains.
- Interop: Outputs (alerts/patches) feed downstream tooling (ITSM, Slack, CI approvals) if desired.
- Safe-by-design: default to propose-only; move to auto-apply via policy/approval gates.

Next steps
- Parameterize feature sources via metadata (table list, entity mapping) instead of hard-coded examples.
- Enrich drift tests (PSI/KL divergence, population stability, anomaly bands).
- Add approval workflow (e.g., tag-based or table-driven allowlist) before APPLY_FEATURE_PATCHES runs.
