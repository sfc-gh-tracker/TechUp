-- WAREHOUSE_PERFORMANCE_DT: aggregates key metrics from account usage
create or replace dynamic table WAREHOUSE_PERFORMANCE_DT
warehouse = PIPELINE_WH
lag = '15 minutes'
as
with q as (
  select
    query_id,
    warehouse_name,
    database_name,
    schema_name,
    query_text,
    start_time,
    end_time,
    total_elapsed_time,
    total_bytes_scanned,
    bytes_spilled_to_local_storage,
    bytes_spilled_to_remote_storage,
    rows_produced
  from snowflake.account_usage.query_history
  where start_time >= dateadd('hour', -24, current_timestamp())
),
wm as (
  select
    warehouse_name,
    start_time,
    end_time,
    credits_used,
    avg_running,
    avg_queued_load,
    avg_queued_provisioning
  from snowflake.account_usage.warehouse_metering_history
  where start_time >= dateadd('hour', -24, current_timestamp())
)
select
  coalesce(q.warehouse_name, wm.warehouse_name) as warehouse_name,
  date_trunc('hour', coalesce(q.start_time, wm.start_time)) as hour_bucket,
  count(q.query_id) as total_queries,
  sum(q.total_bytes_scanned) as bytes_scanned,
  sum(q.bytes_spilled_to_local_storage + q.bytes_spilled_to_remote_storage) as bytes_spilled,
  avg(wm.avg_running) as avg_running,
  avg(wm.avg_queued_load + wm.avg_queued_provisioning) as avg_queued,
  sum(wm.credits_used) as credits_used
from q
full outer join wm
  on q.warehouse_name = wm.warehouse_name
 and date_trunc('hour', q.start_time) = date_trunc('hour', wm.start_time)
group by 1,2;


