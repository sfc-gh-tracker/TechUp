-- Plans objects for blue/green deployment using zero-copy branches
create or replace dynamic table BG_DEPLOY_PLAN_DT
warehouse = PIPELINE_WH
lag = '15 minutes'
as
select
  current_timestamp() as planned_at,
  database_name,
  schema_name,
  name as object_name,
  object_type,
  '/* REVIEW */ create branch ' || database_name || '_BG from ' || database_name as proposed_branch_ddl
from snowflake.account_usage.objects
where deleted is null and object_type in (''TABLE'',''VIEW'',''DYNAMIC TABLE'');


