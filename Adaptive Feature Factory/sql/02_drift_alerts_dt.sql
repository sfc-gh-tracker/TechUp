-- DT_DRIFT_ALERTS: compares current stats vs baseline snapshot
-- Baseline managed via a table FEATURE_STATS_BASELINE captured periodically
create or replace table FEATURE_STATS_BASELINE as
select * from DT_FEATURE_STATS where 1=0;

create or replace dynamic table DT_DRIFT_ALERTS
warehouse = PIPELINE_WH
lag = '15 minutes'
as
with curr as (
  select * from DT_FEATURE_STATS
),
base as (
  select * from FEATURE_STATS_BASELINE
)
select
  current_timestamp() as detected_at,
  c.entity,
  c.feature_name,
  c.mean as current_mean,
  b.mean as baseline_mean,
  c.stddev as current_stddev,
  b.stddev as baseline_stddev,
  case
    when b.n is null then 'NO_BASELINE'
    when abs(c.mean - b.mean) > 3 * coalesce(b.stddev,0) then 'MEAN_SHIFT'
    when c.p99 > b.p99 * 1.25 then 'TAIL_INFLATION'
    else 'OK'
  end as drift_type
from curr c
left join base b
  on c.entity = b.entity and c.feature_name = b.feature_name;


