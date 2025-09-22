-- Seed workload to trigger Performance Monitor signals
-- 1) Create a synthetic large table
create or replace schema if not exists TECHUP.WORKLOAD;
create or replace table TECHUP.WORKLOAD.BIG_FACT as
select
  seq4() as id,
  uniform(1,1000000,random()) as customer_id,
  to_date('2024-01-01') + uniform(0,365,random()) as trx_date,
  uniform(1,1000,random())::number(10,2) as amount,
  randstr(20,random()) as payload
from table(generator(rowcount => 5e6));

-- 2) Use a small warehouse to induce queueing/spillage under load
alter warehouse if exists PIPELINE_WH set warehouse_size = XSMALL;

-- 3) Run scan-heavy queries repeatedly
-- (execute this block a few times or schedule via task for sustained load)
select count(*) from TECHUP.WORKLOAD.BIG_FACT where amount > 900;
select sum(amount) from TECHUP.WORKLOAD.BIG_FACT where trx_date between to_date('2024-06-01') and to_date('2024-07-01');
select bf.customer_id, sum(amount) from TECHUP.WORKLOAD.BIG_FACT bf group by 1 order by 2 desc limit 1000;

-- 4) Join with itself to amplify bytes scanned and potential spill
select a.customer_id, sum(a.amount) total_a, sum(b.amount) total_b
from TECHUP.WORKLOAD.BIG_FACT a
join TECHUP.WORKLOAD.BIG_FACT b
  on a.customer_id = b.customer_id
where a.trx_date >= to_date('2024-09-01')
group by 1
order by 2 desc limit 500;

-- 5) Optionally bump size to relieve queue and observe metering changes
-- alter warehouse PIPELINE_WH set warehouse_size = MEDIUM;


