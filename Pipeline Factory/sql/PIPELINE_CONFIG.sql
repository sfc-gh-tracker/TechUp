-- PIPELINE_CONFIG table DDL (database-level DT placement)
create or replace table PIPELINE_CONFIG (
  transformation_sql_snippet string           not null,
  target_dt_database         varchar          not null,
  target_dt_name             varchar          not null,
  lag_minutes                number(10,0)     not null,
  warehouse                  varchar          not null,
  status                     varchar          not null,
  created_at                 timestamp_ltz    default current_timestamp()
);
create or replace TABLE PIPELINE_CONFIG (
	TRANSFORMATION_SQL_SNIPPET VARCHAR(16777216) NOT NULL,
	TARGET_DT_NAME VARCHAR(16777216) NOT NULL,
	LAG_MINUTES NUMBER(10,0) NOT NULL,
	WAREHOUSE VARCHAR(16777216) NOT NULL,
	STATUS VARCHAR(16777216) NOT NULL,
	CREATED_AT TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP()
);