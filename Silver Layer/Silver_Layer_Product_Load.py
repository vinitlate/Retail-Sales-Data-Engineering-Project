# Databricks notebook source
spark.sql("USE CATALOG globalretail")
spark.sql("USE SCHEMA silver_layer")
spark.sql("""
          create table if not exists silver_products(
              product_id string,
              name string,
              category string,
              brand string,
              price double,
              stock_quantity int,
              rating double,
              is_active boolean,
              price_category string,
              stock_status string,
              last_updated timestamp
          )
          using delta
          """)

# COMMAND ----------

last_processed_df = spark.sql("select max(last_updated) as last_processed from silver_products")
last_processed_timestamp = last_processed_df.collect()[0]['last_processed']

if last_processed_timestamp is None:
  last_processed_timestamp = "1900-01-01T00:00:00.000+0000"

# COMMAND ----------

spark.sql(f"""
          create or replace  temporary view bronze_incremental_products as
          select *
          from globalretail.bronze_layer.bronze_product_data
          where ingestion_timestamp >= '{last_processed_timestamp}'
          """)

# COMMAND ----------

"""
Data Transformations:

Price normalization - setting negative prices to 0
stock quantity normalization - setting negative stock quantities to 0
rating normalization - clamping between 0 to 5
price categorization - premium, standard, budget
stock status calculation - out of stock, low stock, moderate stock, sufficient stock
"""

spark.sql("""
          create or replace temporary view silver_incremental_products as
          select
                product_id,
                name,
                category,
                brand,
                case
                    when price < 0 then 0
                     else price
                end as price,
                case
                    when stock_quantity < 0 then 0
                     else stock_quantity
                end as stock_quantity,
                case
                    when rating < 0 then 0
                    when rating > 5 then 5
                     else rating
                end as rating,
                is_active,
                case
                    when price > 100 then 'premium'
                    when price > 50 then 'standard'
                     else 'budget'
                end as price_category,
                case
                    when stock_quantity <= 0 then 'out of stock'
                    when stock_quantity <= 10 then 'low stock'
                    when stock_quantity <= 50 then 'moderate stock'
                     else 'sufficient stock'
                end as stock_status,
                current_timestamp() as last_updated
          from bronze_incremental_products
          where name is not null and category is not null
          """)

# COMMAND ----------

spark.sql("""
          merge into silver_products t
          using silver_incremental_products s
          on s.product_id = t.product_id
          when matched then update set *
          when not matched then insert *
          """)

# COMMAND ----------

spark.sql("select * from silver_products").display()