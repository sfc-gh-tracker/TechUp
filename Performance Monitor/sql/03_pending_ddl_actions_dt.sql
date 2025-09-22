-- PENDING_DDL_ACTIONS_DT: convert recommendations to candidate DDL statements
create or replace dynamic table PENDING_DDL_ACTIONS_DT
warehouse = PIPELINE_WH
lag = '30 minutes'
as
with recs as (
  select * from OPTIMIZATION_RECOMMENDATIONS_DT
)
select
  generated_at,
  warehouse_name,
  database_name,
  schema_name,
  table_name,
  issue_category,
  recommendation_text,
  case
    when issue_category like 'High spillage%' and recommendation_text like 'Recommend defining a cluster key%' then
      '/* REVIEW */ ' || 'alter table ' || coalesce(database_name,'') || '.' || coalesce(schema_name,'') || '.' || coalesce(table_name,'') || ' cluster by (/* TODO: choose columns */)'
    when issue_category like 'High scan volume%' and recommendation_text like 'Recommend adding a cluster key%' then
      '/* REVIEW */ ' || 'alter table ' || coalesce(database_name,'') || '.' || coalesce(schema_name,'') || '.' || coalesce(table_name,'') || ' cluster by (/* TODO: choose columns */)'
    when issue_category = 'High queueing detected' then
      '/* REVIEW */ ' || 'alter warehouse ' || warehouse_name || ' set max_cluster_count = greatest(coalesce(current_max_cluster_count,1)+1,2)'
    else null
  end as proposed_ddl
from recs;


