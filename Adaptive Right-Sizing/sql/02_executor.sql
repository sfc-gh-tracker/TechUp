-- Log table
create or replace table RIGHT_SIZING_LOG (
  action_id string default uuid_string(),
  executed_at timestamp_ltz default current_timestamp(),
  warehouse_name string,
  hour_bucket timestamp_ltz,
  recommended_size string,
  recommend_multi_cluster number,
  status string,
  ddl text,
  error text
);

create or replace procedure APPLY_RIGHT_SIZING()
returns string
language sql
execute as owner
as
$$
declare
  c cursor for
    select warehouse_name, hour_bucket, recommended_size, recommend_multi_cluster
    from RIGHT_SIZING_POLICY_DT
    where date_trunc('hour', current_timestamp()) = hour_bucket;
  v_wh string; v_hr timestamp_ltz; v_size string; v_mc number; v_sql text; v_count number := 0;
begin
  for rec in c do
    v_wh := rec.warehouse_name; v_hr := rec.hour_bucket; v_size := rec.recommended_size; v_mc := rec.recommend_multi_cluster;
    v_sql := 'alter warehouse ' || v_wh || ' set warehouse_size = ' || v_size || case when v_mc > 0 then ', max_cluster_count = 2' else '' end;
    begin
      execute immediate :v_sql;
      insert into RIGHT_SIZING_LOG(warehouse_name, hour_bucket, recommended_size, recommend_multi_cluster, status, ddl)
      values(:v_wh, :v_hr, :v_size, :v_mc, 'SUCCESS', :v_sql);
      v_count := v_count + 1;
    exception when other then
      insert into RIGHT_SIZING_LOG(warehouse_name, hour_bucket, recommended_size, recommend_multi_cluster, status, ddl, error)
      values(:v_wh, :v_hr, :v_size, :v_mc, 'ERROR', :v_sql, sqlerrm);
    end;
  end for;
  return 'Applied ' || v_count || ' right-sizing action(s).';
end;
$$;

create or replace task APPLY_RIGHT_SIZING_TASK
warehouse = PIPELINE_WH
schedule = 'USING CRON 0 * * * * UTC'
as call APPLY_RIGHT_SIZING();

alter task APPLY_RIGHT_SIZING_TASK resume;


