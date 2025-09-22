-- Convert recommendations into proposed DDL (review-first)
create or replace dynamic table QPO_PENDING_ACTIONS_DT
warehouse = PIPELINE_WH
lag = '30 minutes'
as
with recs as (
  select * from QPO_RECOMMENDATIONS_DT
)
select
  generated_at,
  object_name,
  q_count,
  bytes_scanned,
  bytes_spilled,
  recommendation,
  roi_signal,
  case
    when recommendation ilike 'Recommend CLUSTER BY%' then '/* REVIEW */ alter table ' || object_name || ' cluster by (/* TODO: choose columns */)'
    when recommendation ilike 'Recommend CLUSTER BY / Materialized aggregate%' then '/* REVIEW */ /* Consider building an aggregate DT or materialized view for ' || object_name || ' */'
    else null
  end as proposed_ddl
from recs;


