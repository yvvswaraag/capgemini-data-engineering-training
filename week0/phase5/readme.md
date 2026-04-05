

## 📂 Dataset

**Olist Brazilian E-commerce Dataset**
A real-world multi-table dataset containing information about customers, orders, products, and transactions.

---

## 🎯 Objective

The objective of this project is to build an end-to-end data engineering pipeline using PySpark in Databricks. This includes data ingestion, transformation, analysis, and generating a final reporting dataset.

---

## 🧠 Key Concepts Covered

* Data ingestion from multiple CSV files
* Data cleaning and validation
* Joining multiple tables
* Aggregations and grouping
* Window functions (ranking, running totals)
* Customer segmentation
* Building final reporting dataset

---

## ⚙️ Workflow

### 🔹 Step 1: Data Ingestion

* Uploaded multiple CSV files into Databricks
* Loaded data into PySpark DataFrames

---

### 🔹 Step 2: Data Preparation

* Verified schema and data types
* Handled joins between:

  * customers
  * orders
  * order_items
  * products

---

### 🔹 Step 3: Analytical Tasks

#### ✅ Task 1: Top 3 Customers per City

* Calculated total spend per customer
* Used window function (`rank()`) to find top customers

---

#### ✅ Task 2: Running Total of Sales

* Calculated daily sales
* Applied cumulative sum using window function

---

#### ✅ Task 3: Top Products per Category

* Aggregated sales per product
* Joined with product categories
* Used `dense_rank()` for ranking

---

#### ✅ Task 4: Customer Lifetime Value (CLV)

* Calculated total spend per customer across all orders

---

#### ✅ Task 5: Customer Segmentation

* Applied business rules to classify customers:

  * Gold (>10000)
  * Silver (5000–10000)
  * Bronze (<5000)
* Analyzed customer distribution

---

#### ✅ Task 6: Final Reporting Table

* Combined insights into a single dataset:

  * customer_id
  * customer_city
  * total_spend
  * segment
  * total_orders

---

## 📊 Output Insights

* Identified top customers and products
* Analyzed sales trends over time
* Segmented customers based on spending behavior
* Built a final dataset ready for reporting and dashboards

---

## 📚 Key Learnings

* Built a complete data pipeline using PySpark
* Gained hands-on experience with joins, aggregations, and window functions
* Understood how to transform raw data into business insights
* Learned how to design a reporting dataset for analytics

---

## 🏁 Conclusion

This project demonstrates how to handle real-world data using PySpark and Databricks, from ingestion to final reporting. It highlights key data engineering concepts required for building scalable and efficient data pipelines.

---
