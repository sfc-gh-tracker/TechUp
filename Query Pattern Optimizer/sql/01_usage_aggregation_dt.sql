-- Aggregates query patterns to find hot predicates/joins and large scans
create or replace dynamic table QPO_USAGE_DT
warehouse = PIPELINE_WH
lag = '15 minutes'
as
select
  query_id,
  warehouse_name,
  database_name,
  schema_name,
  query_text,
  bytes_scanned,
  bytes_spilled_to_local_storage + bytes_spilled_to_remote_storage as bytes_spilled,
  start_time,
  regexp_substr(query_text, 'from[[:space:]]+([A-Za-z0-9_\.\"`]+)', 1, 1, 'ie', 1) as from_obj,
  regexp_substr(query_text, 'join[[:space:]]+([A-Za-z0-9_\.\"`]+)', 1, 1, 'ie', 1) as join_obj,
  regexp_substr(query_text, 'where[[:space:]]+.*', 1, 1, 'i') as where_clause
from TECHUP.QPO_AUDIT.QUERY_HISTORY_STG
where start_time >= dateadd('hour', -24, current_timestamp())
  and query_text ilike '%select%';


