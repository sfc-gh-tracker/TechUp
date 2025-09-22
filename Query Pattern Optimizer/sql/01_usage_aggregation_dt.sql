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
  total_bytes_scanned,
  bytes_spilled_to_local_storage + bytes_spilled_to_remote_storage as bytes_spilled,
  start_time,
  regexp_substr(query_text, '(?i)from\s+([\w\."`]+)', 1, 1, 'e', 1) as from_obj,
  regexp_substr(query_text, '(?i)join\s+([\w\."`]+)', 1, 1, 'e', 1) as join_obj,
  regexp_substr(query_text, '(?i)where\s+(.*)', 1, 1) as where_clause
from snowflake.account_usage.query_history
where start_time >= dateadd('hour', -24, current_timestamp())
  and query_text ilike '%select%';


