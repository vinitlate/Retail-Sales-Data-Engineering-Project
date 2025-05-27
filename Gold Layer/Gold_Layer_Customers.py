# Databricks notebook source
spark.sql("USE CATALOG globalretail")
spark.sql("USE SCHEMA gold_layer")
spark.sql("""
        CREATE OR REPLACE TABLE gold_customers AS
        SELECT
            customer_id,
            name,
            email,
            country,
            customer_type,
            registration_date,
            age,
            gender,
            total_purchases,
            customer_segment,
            days_since_registration,
            last_updated
        FROM
            globalretail.silver_layer.silver_customers
          """)