-- Log table
create or replace table QPO_ACTION_LOG (
  action_id string default uuid_string(),
  executed_at timestamp_ltz default current_timestamp(),
  object_name string,
  status string,
  ddl text,
  error text
);

-- Executor procedure and task
create or replace procedure QPO_RUN_ACTIONS()
returns string
language sql
execute as owner
as
$$
declare
  c cursor for select object_name, proposed_ddl from QPO_PENDING_ACTIONS_DT where proposed_ddl is not null;
  v_obj string; v_sql text; v_count number := 0;
begin
  for rec in c do
    v_obj := rec.object_name; v_sql := rec.proposed_ddl;
    begin
      execute immediate v_sql;
      insert into QPO_ACTION_LOG(object_name, status, ddl) values(v_obj,'SUCCESS',v_sql);
      v_count := v_count + 1;
    exception when other then
      insert into QPO_ACTION_LOG(object_name, status, ddl, error) values(v_obj,'ERROR',v_sql,sqlerrm);
    end;
  end for;
  return 'Executed ' || v_count || ' optimizer action(s).';
end;
$$;

create or replace task QPO_RUN_ACTIONS_TASK
warehouse = PIPELINE_WH
schedule = 'USING CRON */60 * * * * UTC'
as call QPO_RUN_ACTIONS();

alter task QPO_RUN_ACTIONS_TASK resume;


