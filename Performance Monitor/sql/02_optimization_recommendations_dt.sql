-- OPTIMIZATION_RECOMMENDATIONS_DT: generate human-readable actions from performance data
create or replace dynamic table OPTIMIZATION_RECOMMENDATIONS_DT
warehouse = PIPELINE_WH
lag = '30 minutes'
as
with perf as (
  select * from WAREHOUSE_PERFORMANCE_DT
),
storage as (
  select
    table_catalog as database_name,
    table_schema as schema_name,
    table_name,
    active_bytes,
    time_travel_bytes,
    failsafe_bytes
  from snowflake.account_usage.table_storage_metrics
)
select
  current_timestamp() as generated_at,
  perf.warehouse_name,
  perf.hour_bucket,
  storage.database_name,
  storage.schema_name,
  storage.table_name,
  case
    when perf.bytes_spilled > 10*1024*1024*1024 then 'High spillage detected'
    when perf.bytes_scanned > 100*1024*1024*1024 then 'High scan volume detected'
    when perf.credits_used > 50 then 'High utilization detected'
    else 'Normal'
  end as issue_category,
  case
    when perf.bytes_spilled > 10*1024*1024*1024 then
      'Recommend defining a cluster key on selective columns to reduce spillage.'
    when perf.bytes_scanned > 100*1024*1024*1024 then
      'Recommend adding a cluster key to improve pruning on large scans.'
    when perf.credits_used > 50 then
      'Recommend increasing warehouse size or enabling multi-cluster during peak hours.'
    else 'No action needed.'
  end as recommendation_text
from perf
left join storage
  on 1=1;


