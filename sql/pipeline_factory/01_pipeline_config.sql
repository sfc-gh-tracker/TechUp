-- PIPELINE_CONFIG DDL and seed rows
create or replace table PIPELINE_CONFIG (
  pipeline_id                varchar          not null,
  source_table_name          varchar          not null,
  transformation_sql_snippet string           not null,
  target_dt_name             varchar          not null,
  lag_minutes                number(10,0)     not null,
  warehouse                  varchar          not null,
  status                     varchar          not null,
  created_at                 timestamp_ltz    default current_timestamp(),
  constraint PIPELINE_CONFIG_PK primary key (pipeline_id)
);

insert into PIPELINE_CONFIG (
  pipeline_id, source_table_name, transformation_sql_snippet, target_dt_name, lag_minutes, warehouse, status
) values
  (
    'orders_complete',
    'RAW.SALES.ORDERS',
    $$select
         order_id, customer_id, total_amount, order_date
      from {SOURCE_TABLE}
      where status = 'COMPLETE'$$,
    'ANALYTICS.STAGE.ORDERS_COMPLETE_DT',
    5,
    'PIPELINE_WH',
    'PENDING'
  ),
  (
    'customers_latest',
    'RAW.CRM.CUSTOMERS',
    $$select *
      from {SOURCE_TABLE}
      qualify row_number() over (partition by customer_id order by updated_at desc) = 1$$,
    'ANALYTICS.STAGE.CUSTOMERS_LATEST_DT',
    10,
    'PIPELINE_WH',
    'PENDING'
  );
