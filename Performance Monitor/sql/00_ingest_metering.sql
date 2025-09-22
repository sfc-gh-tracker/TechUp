-- Stage ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY into TECHUP.AUDIT for DT-safe consumption
create schema if not exists TECHUP.AUDIT;

create table if not exists TECHUP.AUDIT.WAREHOUSE_METERING_STG as
select
  warehouse_id,
  warehouse_name,
  start_time,
  end_time,
  credits_used,
  credits_used_compute,
  credits_used_cloud_services,
  avg_running,
  avg_queued_load,
  avg_queued_provisioning
from snowflake.account_usage.warehouse_metering_history
where start_time >= dateadd(day, -14, current_timestamp());

alter table TECHUP.AUDIT.WAREHOUSE_METERING_STG set change_tracking = true;

create or replace task TECHUP.AUDIT.INGEST_WAREHOUSE_METERING_TASK
warehouse = PIPELINE_WH
schedule = 'USING CRON */30 * * * * UTC'
as
merge into TECHUP.AUDIT.WAREHOUSE_METERING_STG t
using (
  select warehouse_id, warehouse_name, start_time, end_time, credits_used,
         credits_used_compute, credits_used_cloud_services,
         avg_running, avg_queued_load, avg_queued_provisioning
  from snowflake.account_usage.warehouse_metering_history
  where start_time >= dateadd(day, -2, current_timestamp())
) s
on t.warehouse_id = s.warehouse_id and t.start_time = s.start_time
when matched then update set
  warehouse_name = s.warehouse_name,
  end_time = s.end_time,
  credits_used = s.credits_used,
  credits_used_compute = s.credits_used_compute,
  credits_used_cloud_services = s.credits_used_cloud_services,
  avg_running = s.avg_running,
  avg_queued_load = s.avg_queued_load,
  avg_queued_provisioning = s.avg_queued_provisioning
when not matched then insert (
  warehouse_id, warehouse_name, start_time, end_time, credits_used,
  credits_used_compute, credits_used_cloud_services,
  avg_running, avg_queued_load, avg_queued_provisioning
) values (
  s.warehouse_id, s.warehouse_name, s.start_time, s.end_time, s.credits_used,
  s.credits_used_compute, s.credits_used_cloud_services,
  s.avg_running, s.avg_queued_load, s.avg_queued_provisioning
);

alter task TECHUP.AUDIT.INGEST_WAREHOUSE_METERING_TASK resume;


