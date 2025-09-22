-- DT_PATCH_CANDIDATES: generate patch SQL based on drift type
create or replace dynamic table DT_PATCH_CANDIDATES
warehouse = PIPELINE_WH
lag = '30 minutes'
as
with alerts as (
  select * from DT_DRIFT_ALERTS where drift_type in ('MEAN_SHIFT','TAIL_INFLATION')
)
select
  detected_at,
  entity,
  feature_name,
  drift_type,
  case drift_type
    when 'MEAN_SHIFT' then '/* REVIEW */ /* Winsorize and re-center */'
    when 'TAIL_INFLATION' then '/* REVIEW */ /* Add cap at p99 */'
  end as rationale,
  case drift_type
    when 'MEAN_SHIFT' then '/* REVIEW */ update ANALYTICS.FEATURES.' || entity || '_FEATURES set ' || feature_name || ' = nullif(' || feature_name || ', 0)'
    when 'TAIL_INFLATION' then '/* REVIEW */ update ANALYTICS.FEATURES.' || entity || '_FEATURES set ' || feature_name || ' = iff(' || feature_name || ' > <P99>, <P99>, ' || feature_name || ')'
  end as proposed_sql
from alerts;


