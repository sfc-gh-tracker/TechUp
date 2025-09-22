-- Action log
create or replace table POLICY_PRODUCT_LOG (
  action_id string default uuid_string(),
  executed_at timestamp_ltz default current_timestamp(),
  status string,
  ddl text,
  error text
);

create or replace procedure APPLY_POLICY_PRODUCTS()
returns string
language sql
execute as owner
as
$$
declare
  c cursor for
    select proposed_view_ddl from POLICY_PRODUCT_PROJECTIONS_DT where proposed_view_ddl is not null;
  v_sql text; v_count number := 0;
begin
  for rec in c do
    v_sql := rec.proposed_view_ddl;
    begin
      execute immediate v_sql;
      insert into POLICY_PRODUCT_LOG(status, ddl) values('SUCCESS', v_sql);
      v_count := v_count + 1;
    exception when others then
      insert into POLICY_PRODUCT_LOG(status, ddl, error) values('ERROR', v_sql, sqlerrm);
    end;
  end for;
  return 'Created ' || v_count || ' policy-aware view(s).';
end;
$$;

create or replace task APPLY_POLICY_PRODUCTS_TASK
warehouse = PIPELINE_WH
schedule = 'USING CRON */60 * * * * UTC'
as call APPLY_POLICY_PRODUCTS();

alter task APPLY_POLICY_PRODUCTS_TASK resume;


