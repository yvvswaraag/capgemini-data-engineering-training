
##  Key Concepts Learned and Implemented

During this task, I improved my understanding of core PySpark concepts:

- Data loading using PySpark
- Data type casting using `cast()`
- Joining datasets using `join()`
- Aggregations using `groupBy()` and `sum()`
- Column transformations using `withColumn()`
- Conditional logic using `when()`
- Sorting and limiting results
- Building final reporting tables

---

## ⚙️ What I Implemented

### 1.  Daily Sales Analysis
- Calculated total revenue for each day
- Used `groupBy()` with aggregation functions

### 2.  City-wise Revenue
- Joined sales and customer datasets
- Computed total revenue per city

### 3.  Top 5 Customers
- Created full customer names using `concat_ws()`
- Calculated total spending
- Sorted data to identify top 5 customers

### 4.  Repeat Customers
- Counted number of orders per customer
- Filtered customers with more than one order

### 5.  Customer Segmentation
- Categorized customers based on spending:
  - **Gold** → Spend > 10000  
  - **Silver** → Spend between 5000 and 10000  
  - **Bronze** → Spend < 5000  
- Implemented using `when()` conditional logic

### 6.  Final Reporting Table
- Combined:
  - Customer details  
  - Total spending  
  - Order count  
  - Customer segment  
- Created a clean dataset for reporting and analysis

---

## Challenges Faced

- Handling data type conversions for numeric columns  
- Writing correct joins across multiple datasets  
- Applying conditional logic for segmentation  
- Managing multiple transformations step-by-step  

---

##  Key Learnings

- Real-world data processing involves combining multiple datasets  
- PySpark transformations must follow a logical sequence  
- Joins and aggregations are core data engineering operations  
- Simple datasets can generate valuable business insights with proper logic  

---

##  Outcome

By completing this task, I gained hands-on experience in:

- End-to-end data analysis using PySpark  
- Generating business insights like revenue trends and customer segmentation  
- Writing clean, structured, and efficient PySpark code  

This task helped me move from basic operations to solving real-world data problems.
