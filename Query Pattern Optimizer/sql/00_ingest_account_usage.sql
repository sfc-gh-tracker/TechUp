-- Stage ACCOUNT_USAGE.QUERY_HISTORY into TECHUP.QPO_AUDIT.QUERY_HISTORY_STG (DT-safe)
create schema if not exists TECHUP.QPO_AUDIT;

create table if not exists TECHUP.QPO_AUDIT.QUERY_HISTORY_STG as
select
  query_id,
  warehouse_name,
  database_name,
  schema_name,
  query_text,
  bytes_scanned,
  bytes_spilled_to_local_storage,
  bytes_spilled_to_remote_storage,
  start_time
from snowflake.account_usage.query_history
where start_time >= dateadd('day', -7, current_timestamp());

alter table TECHUP.QPO_AUDIT.QUERY_HISTORY_STG set change_tracking = true;

create or replace task TECHUP.QPO_AUDIT.INGEST_QUERY_HISTORY_TASK
warehouse = PIPELINE_WH
schedule = 'USING CRON */15 * * * * UTC'
as
merge into TECHUP.QPO_AUDIT.QUERY_HISTORY_STG t
using (
  select query_id, warehouse_name, database_name, schema_name, query_text,
         bytes_scanned, bytes_spilled_to_local_storage, bytes_spilled_to_remote_storage, start_time
  from snowflake.account_usage.query_history
  where start_time >= dateadd('day', -2, current_timestamp())
) s
on t.query_id = s.query_id
when matched then update set
  warehouse_name = s.warehouse_name,
  database_name = s.database_name,
  schema_name = s.schema_name,
  query_text = s.query_text,
  bytes_scanned = s.bytes_scanned,
  bytes_spilled_to_local_storage = s.bytes_spilled_to_local_storage,
  bytes_spilled_to_remote_storage = s.bytes_spilled_to_remote_storage,
  start_time = s.start_time
when not matched then insert (
  query_id, warehouse_name, database_name, schema_name, query_text,
  bytes_scanned, bytes_spilled_to_local_storage, bytes_spilled_to_remote_storage, start_time
) values (
  s.query_id, s.warehouse_name, s.database_name, s.schema_name, s.query_text,
  s.bytes_scanned, s.bytes_spilled_to_local_storage, s.bytes_spilled_to_remote_storage, s.start_time
);

alter task TECHUP.QPO_AUDIT.INGEST_QUERY_HISTORY_TASK resume;


