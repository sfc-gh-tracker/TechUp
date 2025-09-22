-- Produces physical design recommendations with ROI estimates
create or replace dynamic table QPO_RECOMMENDATIONS_DT
warehouse = PIPELINE_WH
lag = '30 minutes'
as
with base as (
  select * from QPO_USAGE_DT
),
scans as (
  select
    coalesce(from_obj, join_obj) as obj,
    count(*) as q_count,
    sum(total_bytes_scanned) as bytes_scanned,
    sum(bytes_spilled) as bytes_spilled
  from base
  where coalesce(from_obj, join_obj) is not null
  group by 1
)
select
  current_timestamp() as generated_at,
  obj as object_name,
  q_count,
  bytes_scanned,
  bytes_spilled,
  case
    when bytes_spilled > 10*1024*1024*1024 then 'Recommend CLUSTER BY on most selective predicate'
    when bytes_scanned > 200*1024*1024*1024 then 'Recommend CLUSTER BY / Materialized aggregate'
    else 'OK'
  end as recommendation,
  case
    when bytes_spilled > 0 then round(bytes_spilled/bytes_scanned, 3)
    else 0
  end as roi_signal
from scans;


