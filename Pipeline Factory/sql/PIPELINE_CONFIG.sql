-- PIPELINE_CONFIG table DDL (DB + SCHEMA placement for DTs)
create or replace table PIPELINE_CONFIG (
  transformation_sql_snippet varchar(16777216) not null,
  target_dt_database         varchar           not null,
  target_dt_schema           varchar           not null,
  target_dt_name             varchar           not null,
  lag_minutes                number(10,0)      not null,
  warehouse                  varchar           not null,
  status                     varchar           not null,
  created_at                 timestamp_ltz     default current_timestamp()
);
