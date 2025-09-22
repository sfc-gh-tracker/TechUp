-- DT_FEATURE_STATS: compute rolling stats for selected feature tables
-- Assumes features live in ANALYTICS.FEATURES.*; adjust as needed
create or replace dynamic table DT_FEATURE_STATS
warehouse = PIPELINE_WH
lag = '15 minutes'
as
with src as (
  -- Union a few exemplar feature tables; in practice, parameterize via metadata
  select 'CUSTOMER' as entity, * from ANALYTICS.FEATURES.CUSTOMER_FEATURES
  union all
  select 'ORDER' as entity, * from ANALYTICS.FEATURES.ORDER_FEATURES
),
typed as (
  select entity, $1 as row_object from src -- if variant based, else select columns explicitly
),
exploded as (
  select entity, k as feature_name, try_to_double(v::string) as feature_value
  from typed,
  lateral flatten(input => object_construct_keep_null(row_object)) f(k, v)
),
numeric as (
  select entity, feature_name, feature_value
  from exploded
  where feature_value is not null and try_cast(feature_value as double) is not null
)
select
  current_timestamp() as computed_at,
  entity,
  feature_name,
  count(*) as n,
  avg(feature_value) as mean,
  stddev_samp(feature_value) as stddev,
  min(feature_value) as min_val,
  max(feature_value) as max_val,
  approx_percentile(feature_value, 0.01) as p01,
  approx_percentile(feature_value, 0.50) as p50,
  approx_percentile(feature_value, 0.99) as p99
from numeric
group by 1,2,3;


