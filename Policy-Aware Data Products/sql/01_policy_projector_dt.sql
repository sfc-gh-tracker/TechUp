-- Builds policy-aware projections from base tables using tags/masking policies
create or replace dynamic table POLICY_PRODUCT_PROJECTIONS_DT
warehouse = PIPELINE_WH
lag = '30 minutes'
as
with cols as (
  select
    c.table_catalog as database_name,
    c.table_schema as schema_name,
    c.table_name,
    c.column_name,
    coalesce(t.tag_value, 'PUBLIC') as sensitivity
  from information_schema.columns c
  left join table(information_schema.tag_references_all_columns(object_domain=>'COLUMN')) t
    on t.object_database = c.table_catalog
   and t.object_schema = c.table_schema
   and t.object_name = c.table_name
   and t.column_name = c.column_name
)
select
  current_timestamp() as generated_at,
  database_name,
  schema_name,
  table_name,
  'create or replace view ' || database_name || '.' || schema_name || '_PRODUCT.' || table_name || '_SAFE as select ' || listagg('"' || column_name || '"', ', ') within group (order by column_name) || ' from ' || database_name || '.' || schema_name || '.' || table_name as proposed_view_ddl
from cols
where sensitivity in ('PUBLIC','INTERNAL')
group by 1,2,3,4;


