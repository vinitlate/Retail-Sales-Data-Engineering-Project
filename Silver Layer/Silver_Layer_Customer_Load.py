# Databricks notebook source
spark.sql("USE CATALOG globalretail")
spark.sql("USE SCHEMA silver_layer")
spark.sql("""
          create table if not exists silver_customers
          (
            customer_id string,
            name string,
            email string,
            country string,
            customer_type string,
            registration_date date,
            age int,
            gender string,
            total_purchases int,
            customer_segment string,
            days_since_registration int,
            last_updated timestamp)
          """)

# COMMAND ----------

last_processed_df = spark.sql("select max(last_updated) as last_update from silver_customers")
last_processed_timestamp = last_processed_df.collect()[0]['last_update']

if last_processed_timestamp is None:
    last_processed_timestamp = "1900-01-01T00:00:00.000+0000"

# COMMAND ----------

spark.sql(f"""
          create or replace temporary view bronze_incremental as
          select * 
          from globalretail.bronze_layer.bronze_customer_data c 
          where c.ingestion_timestamp > '{last_processed_timestamp}'
          """)

# COMMAND ----------

spark.sql("select * from bronze_incremental").display()

# COMMAND ----------

"""
validate email addresses(null or not null)
validate age between 18 to 100
create customer_segment as total_purchase > 10000 then 'High Value' if > 5000 then 'Medium Value' else 'Low Value'
days since user is registered in the system
remove any junk records where total_purchase is negative number
"""

spark.sql("""
          create or replace temporary view silver_incremental as
          select 
            customer_id, name, email, country, customer_type, registration_date, age, gender, total_purchases, 
            case 
                when total_purchases > 10000 then 'High Value'
                when total_purchases > 5000 then 'Medium Value'
                else 'Low Value'
            end as customer_segment,
            datediff(current_date(), registration_date) as days_since_registration,
            current_timestamp() as last_updated
          from bronze_incremental
          where
            age between 18 and 100
            and email is not null
            and total_purchases >= 0 
          """)
spark.sql("select * from silver_incremental").display()

# COMMAND ----------

spark.sql("""
          merge into silver_customers t
          using silver_incremental s
          on t.customer_id = s.customer_id
          when matched then update set *
          when not matched then insert *
          """)

# COMMAND ----------

spark.sql("select * from silver_customers").display()