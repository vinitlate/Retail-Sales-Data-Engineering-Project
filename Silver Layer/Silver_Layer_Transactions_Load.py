# Databricks notebook source
spark.sql("use catalog globalretail")
spark.sql("use schema silver_layer")
spark.sql("""
          create table if not exists silver_orders(
              transaction_id string,
              customer_id string,
              product_id string,
              quantity int,
              total_amount double,
              transaction_date date,
              payment_method string,
              store_type string,
              order_status string,
              last_updated timestamp
          )
          using delta
""")

# COMMAND ----------

last_processed_df = spark.sql("select max(last_updated) as last_processed from silver_orders")
last_processed_timestamp = last_processed_df.collect()[0]['last_processed']

if last_processed_timestamp is None:
  last_processed_timestamp = '1900-01-01T00:00:00.000+0000'

# COMMAND ----------

spark.sql(f"""
          create or replace temporary view bronze_incremental_orders as
          select *
          from globalretail.bronze_layer.bronze_transaction_data
          where ingestion_timestamp >= '{last_processed_timestamp}'
          """)

# COMMAND ----------

"""
quantity and total_amount normalization - setting negative values to 0
data casting to ensure consistent date format
order status derivation based on quantity and total_amount

data quality checks - Filter out records with null transaction dates, customer ids or product ids
"""

spark.sql("""
          create or replace temporary view silver_incremental_orders as
          select
            transaction_id,
            customer_id,
            product_id,
            case
              when quantity < 0 then 0
              else quantity
            end as quantity,
            case
              when total_amount < 0 then 0
              else total_amount
            end as total_amount,
            cast(transaction_date as date) as transaction_date,
            payment_method,
            store_type,
            case
              when quantity = 0 and total_amount = 0 then 'Cancelled'
              else 'Completed'
            end as order_status,
            current_timestamp() as last_updated
          from bronze_incremental_orders
          where
            transaction_date is not null
            and customer_id is not null
            and product_id is not null
          """)

# COMMAND ----------

spark.sql("""
          merge into silver_orders t
          using silver_incremental_orders s
          on t.transaction_id = s.transaction_id
          when matched then
            update set *
          when not matched then
            insert *
          """)

# COMMAND ----------

spark.sql("select * from silver_orders").display()