-- Seed queries that produce recognizable patterns and large scans
create or replace schema if not exists DEMO.QPO;
create or replace table DEMO.QPO.SALES as
select seq4() as id,
       uniform(1,50000,random()) as cust_id,
       to_date('2024-01-01') + uniform(0,365,random()) as order_date,
       uniform(1,1000,random())::number(10,2) as amount,
       randstr(12,random()) as region
from table(generator(rowcount=>2e6));

-- repeated predicate: region filter; join pattern on cust_id
select sum(amount) from DEMO.QPO.SALES where region in ('NORTH','SOUTH');
select avg(amount) from DEMO.QPO.SALES where region in ('NORTH','SOUTH') and order_date >= to_date('2024-06-01');

create or replace table DEMO.QPO.CUSTOMERS as
select seq4() as cust_id, randstr(10,random()) as name from table(generator(rowcount=>50000));

select c.name, sum(s.amount)
from DEMO.QPO.CUSTOMERS c
join DEMO.QPO.SALES s on s.cust_id = c.cust_id
where s.region in ('NORTH','SOUTH')
group by 1
order by 2 desc limit 1000;


