-- Action log table
create or replace table ACTION_LOG (
  action_id string default uuid_string(),
  executed_at timestamp_ltz default current_timestamp(),
  status string,
  ddl text,
  error text
);

-- Stored procedure to execute actions
create or replace procedure RUN_PENDING_ACTIONS()
returns string
language sql
execute as owner
as
$$
declare
  c cursor for
    select proposed_ddl from PENDING_DDL_ACTIONS_DT where proposed_ddl is not null;
  v_sql text;
  v_count number := 0;
begin
  for rec in c do
    v_sql := rec.proposed_ddl;
    begin
      execute immediate v_sql;
      insert into ACTION_LOG(status, ddl, error) values('SUCCESS', v_sql, null);
      v_count := v_count + 1;
    exception
      when others then
        insert into ACTION_LOG(status, ddl, error) values('ERROR', v_sql, sqlerrm);
    end;
  end for;
  return 'Executed ' || v_count || ' action(s).';
end;
$$;

-- Task to run periodically
create or replace task RUN_ACTIONS_TASK
warehouse = PIPELINE_WH
schedule = 'USING CRON */30 * * * * UTC'
as call RUN_PENDING_ACTIONS();

alter task RUN_ACTIONS_TASK resume;


