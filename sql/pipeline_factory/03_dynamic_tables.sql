-- Dynamic Tables
-- 1) Pending count
create or replace dynamic table PENDING_PIPELINES_DT
warehouse = PIPELINE_WH
lag = '1 minute'
as
select count(*) as pending_count
from PIPELINE_CONFIG
where status = 'PENDING';

-- 2) Orchestrator Task (DTs cannot call procedures/system functions)
create or replace task PIPELINE_ORCHESTRATOR_TASK
warehouse = PIPELINE_WH
schedule = 'USING CRON * * * * * UTC'
as
begin
  if (select count(*) from PIPELINE_CONFIG where status = 'PENDING') > 0 then
    call RUN_PIPELINE_FACTORY();
  end if;
end;

-- Enable the task
alter task PIPELINE_ORCHESTRATOR_TASK resume;

-- 3) Health Monitor DT
create or replace dynamic table PIPELINE_HEALTH_MONITOR_DT
warehouse = PIPELINE_WH
lag = '5 minutes'
as
with active as (
  select target_dt_name
  from PIPELINE_CONFIG
  where status = 'ACTIVE'
),
latest_history as (
  select
    database_name,
    schema_name,
    name as dt_name,
    state as last_refresh_state,
    start_time as last_refresh_start,
    end_time as last_refresh_end,
    rows_inserted,
    rows_updated,
    rows_deleted,
    staleness_seconds,
    row_number() over (
      partition by database_name, schema_name, name
      order by coalesce(end_time, start_time) desc
    ) as rn
  from snowflake.account_usage.dynamic_table_refresh_history
)
select
  lh.database_name,
  lh.schema_name,
  lh.dt_name,
  lh.last_refresh_state,
  lh.last_refresh_start,
  lh.last_refresh_end,
  lh.rows_inserted,
  lh.rows_updated,
  lh.rows_deleted,
  lh.staleness_seconds as data_freshness_seconds
from latest_history lh
join active a
  on upper(lh.dt_name) = upper(split_part(a.target_dt_name, '.', -1))
where lh.rn = 1;
