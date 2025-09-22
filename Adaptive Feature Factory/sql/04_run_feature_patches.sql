-- Action log for feature patches
create or replace table FEATURE_PATCH_LOG (
  patch_id string default uuid_string(),
  executed_at timestamp_ltz default current_timestamp(),
  entity string,
  feature_name string,
  drift_type string,
  status string,
  sql text,
  error text
);

-- Stored procedure to apply vetted patches (expects proposed_sql to be manually parameterized)
create or replace procedure APPLY_FEATURE_PATCHES()
returns string
language sql
execute as owner
as
$$
declare
  c cursor for
    select entity, feature_name, drift_type, proposed_sql
    from DT_PATCH_CANDIDATES
    where proposed_sql is not null and startswith(proposed_sql, '/* REVIEW */') = false;
  v_sql text;
  v_entity string;
  v_feature string;
  v_drift string;
  v_count number := 0;
begin
  for rec in c do
    v_entity := rec.entity; v_feature := rec.feature_name; v_drift := rec.drift_type; v_sql := rec.proposed_sql;
    begin
      execute immediate v_sql;
      insert into FEATURE_PATCH_LOG(entity, feature_name, drift_type, status, sql) values(v_entity, v_feature, v_drift, 'SUCCESS', v_sql);
      v_count := v_count + 1;
    exception when others then
      insert into FEATURE_PATCH_LOG(entity, feature_name, drift_type, status, sql, error) values(v_entity, v_feature, v_drift, 'ERROR', v_sql, sqlerrm);
    end;
  end for;
  return 'Applied ' || v_count || ' feature patch(es).';
end;
$$;

-- Scheduled task
create or replace task APPLY_FEATURE_PATCHES_TASK
warehouse = PIPELINE_WH
schedule = 'USING CRON */30 * * * * UTC'
as call APPLY_FEATURE_PATCHES();

alter task APPLY_FEATURE_PATCHES_TASK resume;


