-- Execution log
create or replace table BG_ACTION_LOG (
  action_id string default uuid_string(),
  executed_at timestamp_ltz default current_timestamp(),
  status string,
  ddl text,
  error text
);

create or replace procedure BG_APPLY()
returns string
language sql
execute as owner
as
$$
declare
  c cursor for select proposed_branch_ddl from BG_DEPLOY_PLAN_DT where proposed_branch_ddl like '/* REVIEW */%' = false;
  v_sql text; v_count number := 0;
begin
  for rec in c do
    v_sql := rec.proposed_branch_ddl;
    begin
      execute immediate v_sql;
      insert into BG_ACTION_LOG(status, ddl) values('SUCCESS', v_sql);
      v_count := v_count + 1;
    exception when others then
      insert into BG_ACTION_LOG(status, ddl, error) values('ERROR', v_sql, sqlerrm);
    end;
  end for;
  return 'Applied ' || v_count || ' blue/green action(s).';
end;
$$;

create or replace task BG_APPLY_TASK
warehouse = PIPELINE_WH
schedule = 'USING CRON */60 * * * * UTC'
as call BG_APPLY();

alter task BG_APPLY_TASK resume;


