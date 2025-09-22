-- Computes hour-by-hour right-sizing policy for warehouses
create or replace dynamic table RIGHT_SIZING_POLICY_DT
warehouse = PIPELINE_WH
lag = '30 minutes'
as
with wm as (
  select warehouse_name, date_trunc('hour', start_time) as hour_bucket,
         avg_running, avg_queued_load, avg_queued_provisioning
  from TECHUP.AUDIT.WAREHOUSE_METERING_STG
  where start_time >= dateadd('day', -7, current_timestamp())
), agg as (
  select warehouse_name, hour_bucket,
         avg(avg_running) as avg_running,
         avg(avg_queued_load + avg_queued_provisioning) as avg_queue
  from wm group by 1,2
)
select
  current_timestamp() as generated_at,
  warehouse_name,
  hour_bucket,
  case when avg_queue > 2 then 'LARGE' when avg_running < 0.5 then 'SMALL' else 'MEDIUM' end as recommended_size,
  case when avg_queue > 2 then 2 else 0 end as recommend_multi_cluster
from agg;


