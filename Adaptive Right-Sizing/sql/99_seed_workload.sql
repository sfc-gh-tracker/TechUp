-- Seed varying load to exercise right-sizing policy
alter warehouse if exists PIPELINE_WH set warehouse_size = XSMALL;

-- Burst: run expensive joins hourly (manually execute around top of hour)
create or replace schema if not exists TECHUP.RIGHTSIZE;
create or replace table TECHUP.RIGHTSIZE.FACT as
select seq4() as id,
       uniform(1,100000,random()) as k,
       uniform(1,1000,random())::number(10,2) as v,
       to_timestamp_ltz(dateadd('second', uniform(0,86400,random()), current_date())) as ts
from table(generator(rowcount=>3e6));

create or replace table TECHUP.RIGHTSIZE.DIM as
select seq4() as k, randstr(20,random()) as attr
from table(generator(rowcount=>200000));

-- High-load join
select count(*)
from TECHUP.RIGHTSIZE.FACT f
join TECHUP.RIGHTSIZE.DIM d on f.k = d.k
where f.ts >= dateadd('hour', -1, current_timestamp());


-- Procedure to run workload repeatedly with cache disabled
create or replace procedure RIGHTSIZE_SEED_RUN()
returns string
language javascript
execute as owner
as
$$
var cmds = [
  "alter session set use_cached_result = false",
  "select count(*) from TECHUP.RIGHTSIZE.FACT f join TECHUP.RIGHTSIZE.DIM d on f.k = d.k where f.ts >= dateadd('hour', -1, current_timestamp())",
  "select count(*) from TECHUP.RIGHTSIZE.FACT f join TECHUP.RIGHTSIZE.DIM d on f.k = d.k where f.ts >= dateadd('hour', -1, current_timestamp())",
  "select count(*) from TECHUP.RIGHTSIZE.FACT f join TECHUP.RIGHTSIZE.DIM d on f.k = d.k where f.ts >= dateadd('hour', -1, current_timestamp())",
  "select sum(v) from TECHUP.RIGHTSIZE.FACT where ts >= dateadd('hour', -1, current_timestamp())",
  "select d.attr, count(*) from TECHUP.RIGHTSIZE.FACT f join TECHUP.RIGHTSIZE.DIM d on f.k = d.k group by 1 order by 2 desc limit 1000"
];
for (var i = 0; i < cmds.length; i++) {
  snowflake.execute({sqlText: cmds[i]});
}
return 'RIGHTSIZE_SEED_RUN completed';
$$;

-- Hourly task to execute workload
create or replace task RIGHTSIZE_SEED_TASK
warehouse = PIPELINE_WH
schedule = 'USING CRON 0 * * * * UTC'
as call RIGHTSIZE_SEED_RUN();

alter task RIGHTSIZE_SEED_TASK resume;


