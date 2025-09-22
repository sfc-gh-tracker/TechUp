-- Seed varying load to exercise right-sizing policy
alter warehouse if exists PIPELINE_WH set warehouse_size = XSMALL;

-- Burst: run expensive joins hourly (manually execute around top of hour)
create or replace schema if not exists DEMO.RIGHTSIZE;
create or replace table DEMO.RIGHTSIZE.FACT as
select seq4() as id,
       uniform(1,100000,random()) as k,
       uniform(1,1000,random())::number(10,2) as v,
       to_timestamp_ltz(dateadd('second', uniform(0,86400,random()), current_date())) as ts
from table(generator(rowcount=>3e6));

create or replace table DEMO.RIGHTSIZE.DIM as
select seq4() as k, randstr(20,random()) as attr
from table(generator(rowcount=>200000));

-- High-load join
select count(*)
from DEMO.RIGHTSIZE.FACT f
join DEMO.RIGHTSIZE.DIM d on f.k = d.k
where f.ts >= dateadd('hour', -1, current_timestamp());


