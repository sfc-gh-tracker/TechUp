-- Computes hour-by-hour right-sizing policy for warehouses
create or replace dynamic table RIGHT_SIZING_POLICY_DT
warehouse = PIPELINE_WH
lag = '30 minutes'
as
with wm as (
  select warehouse_name, date_trunc('hour', start_time) as hour_bucket,
         credits_used
  from TECHUP.AUDIT.WAREHOUSE_METERING_STG
  where start_time >= dateadd('day', -7, current_timestamp())
), agg as (
  select warehouse_name, hour_bucket,
         avg(credits_used) as avg_credits
  from wm group by 1,2
)
select
  current_timestamp() as generated_at,
  warehouse_name,
  hour_bucket,
  case when avg_credits > 10 then 'LARGE' else 'MEDIUM' end as recommended_size,
  case when avg_credits > 10 then 2 else 0 end as recommend_multi_cluster
from agg;


