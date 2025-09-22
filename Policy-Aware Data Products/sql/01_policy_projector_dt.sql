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
    coalesce(tr.tag_value, 'PUBLIC') as sensitivity
  from TECHUP.information_schema.columns c
  left join TECHUP.information_schema.tag_references tr
    on tr.object_database = c.table_catalog
   and tr.object_schema = c.table_schema
   and tr.object_name = c.table_name
   and tr.column_name = c.column_name
   and upper(tr.tag_name) = 'SENSITIVITY'
   and upper(tr.domain) = 'COLUMN'
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


