-- Crappy queries to trigger optimizer recommendations (large scans / poor predicates)

-- 1) Non-sargable predicate on partitionable column -> forces full scan
select count(*)
from TECHUP.QPO.SALES
where to_varchar(order_date) like '2024-%' -- casting kills pruning
   or regexp_like(region, '.*');           -- always true, scans all rows

-- 2) Join with casts on join keys + regex filter -> heavy scan/join
select sum(s1.amount)
from TECHUP.QPO.SALES s1
join TECHUP.QPO.CUSTOMERS c
  on to_varchar(s1.cust_id) = to_varchar(c.cust_id) -- casting prevents efficient join
where regexp_like(s1.region, 'NORTH|SOUTH');

-- 3) Self-join with casts but limited by modulus to avoid explosion; still scans wide
select sum(s1.amount)
from TECHUP.QPO.SALES s1
join TECHUP.QPO.SALES s2
  on to_varchar(s1.cust_id) = to_varchar(s2.cust_id)
where s1.id % 1000 = 0 and s2.id % 1000 = 0;

-- Tip: run these several times to accumulate bytes_scanned in staging and drive recommendations.


