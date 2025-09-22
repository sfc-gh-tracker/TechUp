-- Seed a table with column tags to drive policy-aware projections
create or replace schema if not exists DEMO.POLICY;
create or replace table DEMO.POLICY.CUSTOMERS as
select seq4() as id,
       randstr(10,random()) as name,
        'user' || seq4() as email,
       to_date('2024-01-01') + uniform(0,365,random()) as created_dt,
       randstr(8,random()) as ssn -- pretend PII
from table(generator(rowcount=>1000));

-- Create a tag and apply it to columns (requires privileges)
create tag if not exists SENSITIVITY;
alter table DEMO.POLICY.CUSTOMERS modify column SSN set tag SENSITIVITY = 'CONFIDENTIAL';
alter table DEMO.POLICY.CUSTOMERS modify column EMAIL set tag SENSITIVITY = 'INTERNAL';
alter table DEMO.POLICY.CUSTOMERS modify column NAME set tag SENSITIVITY = 'PUBLIC';


