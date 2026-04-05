## Process Workflow

### 1. Data Loading
- Loaded CSV files using PySpark  
- Used the `header` option to read column names  

---

### 2. Data Cleaning
- Removed null values using `dropna()`  
- Converted `total_amount` to integer type for calculations  

---

### 3. Data Processing & Analysis

Worked on solving multiple analytical queries:

####  Daily Sales
- Calculated total sales for each day  

####  City-wise Revenue
- Joined sales and customer datasets  
- Computed total revenue for each city  

####  Repeat Customers
- Identified customers who made more than 2 purchases  

####  Highest Spending Customer per City
- Calculated total spending per customer  
- Determined the top customer in each city  

####  Final Reporting Table
- Created a summary table including:
  - Customer ID  
  - Name  
  - City  
  - Total Spend  
  - Order Count  

---

##  Key Learnings

Through this process, gained experience in:

- Working with PySpark DataFrames  
- Performing joins and aggregations  
- Handling real-world data cleaning scenarios  
- Writing efficient data transformations  
