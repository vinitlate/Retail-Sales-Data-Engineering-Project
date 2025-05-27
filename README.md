# 🧠 Retail Data Lakehouse Project on Databricks

This project demonstrates a complete data lakehouse architecture using **Databricks**, **PySpark**, and **Delta Lake**, structured across **Bronze**, **Silver**, and **Gold** layers. It processes retail data involving customers, products, and transactions — making it analytics-ready for **Power BI reporting**.

---

## 📁 Project Structure
```
retail-datalake-databricks/
├──bronze/
│ ├── Bronze_Layer_Customer_Load.py
│ ├── Bronze_Layer_Product_Load.py
│ └── Bronze_Layer_Transaction_Load.py
├──silver/
│ ├── Silver_Layer_Customer_Load.py
│ ├── Silver_Layer_Product_Load.py
│ └── Silver_Layer_Transactions_Load.py
├──gold/
│ ├── Gold_Layer_Daily_Sales.py
│ ├── Gold_Layer_Daily_Sales_By_Category.py
│ ├── Gold_Layer_Customer_Summary.py
│ ├── Gold_Layer_Product_Performance.py
│ ├── Gold_Layer_Country_Sales.py
│ ├── Gold_Layer_Sales_Calendar.py
│ ├── Gold_Layer_Customers.py
│ ├── Gold_Layer_Products.py
│ └── Gold_Layer_Orders.py
├── README.md
```

---

## 🔄 Data Pipeline Overview

### 🔹 Bronze Layer
- **Purpose**: Raw ingestion of customer CSV, product JSON, and transaction Parquet files.
- **Actions**:
  - Inferred schema loading
  - Appending ingestion timestamp
  - Archiving processed files

### 🔸 Silver Layer
- **Purpose**: Clean, validate, and enrich data.
- **Actions**:
  - Email, age, and range validation (Customers)
  - Stock, price, rating normalization (Products)
  - Derived order status and null checks (Transactions)
  - Incremental processing logic
  - Merged into Delta tables using `MERGE INTO`

### 🟡 Gold Layer
- **Purpose**: Business aggregates and analytical views.
- **Actions**:
  - Daily, category, and country sales summaries
  - Customer value segmentation
  - Product-level performance
  - Calendar table for time-series visuals
  - Enriched `orders`, `customers`, `products` fact/dim tables for BI

---

## 📊 Gold Layer Tables

| Table Name                   | Description                                              |
|-----------------------------|----------------------------------------------------------|
| `gold_daily_sales`          | Daily total revenue                                      |
| `gold_category_sales`       | Sales grouped by product category                        |
| `gold_country_sales`        | Sales grouped by customer country                        |
| `gold_customer_summary`     | Lifetime value and order metrics per customer            |
| `gold_product_performance`  | Product-wise sales, revenue, and average rating          |
| `gold_sales_calendar`       | Weekday, month, and year breakdown for time-series       |
| `gold_orders`               | Enriched fact table for order transactions               |
| `gold_customers`            | Customer master dimension                                |
| `gold_products`             | Product master dimension                                 |

---

## 📈 Power BI Integration

With Gold Layer tables in place, this dataset is ideal for a **star schema model** in Power BI:
- **Fact Table**: `gold_orders`
- **Dimension Tables**: `gold_customers`, `gold_products`, `gold_sales_calendar`

This enables visualizations like:
- Daily/Monthly sales trends
- Top products by revenue or volume
- Customer segments by lifetime value
- Country-level heat maps
- Brand/category comparisons

---

## 🛠️ Technologies Used

- [Databricks](https://databricks.com/) (Delta Lake, SQL, Job Orchestration)
- **PySpark**
- **SQL / Delta**
- **Power BI** (for dashboarding)
- **GitHub** (for version control & sharing)

---

## 🧠 Future Enhancements

- Add CI/CD workflows for notebook testing (e.g., `pytest`, `nbdev`)
- Implement data quality dashboards (e.g., Great Expectations)
- Schedule daily updates via Databricks Workflows
- Integrate streaming sources for near real-time analytics

---

## 🙋‍♂️ Author

**Vinit Late**  
🎓 Master’s in Business Analytics — Bentley University  
💼 Data & Project Management Intern @ Corteva Agriscience  
🔗 [LinkedIn](https://linkedin.com/in/yourprofile) (replace with your actual link)

---

## 📎 License

MIT License. Feel free to fork, learn, and build upon it.
