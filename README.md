# 🛒 E-Commerce Data Pipeline

An end-to-end ETL project built using **Python, Pandas, SQL, and MySQL** to process real-world Brazilian e-commerce data.

I built this project to practice the complete process of taking raw CSV data, cleaning and combining it, loading it into a database, and then using SQL to answer business questions.

## What This Project Does

The pipeline starts with raw CSV files from the **Brazilian E-Commerce Dataset by Olist**.

The main steps are:

**Raw CSV Files → Python/Pandas → Cleaning & Transformation → Data Joining → MySQL → SQL Analysis**

### Data Processing

* Loaded 4 raw CSV datasets using Python and Pandas
* Converted date columns into proper date formats
* Handled null values
* Removed duplicate records
* Joined data from orders, customers, payments, and order items
* Created an `orders_cleaned` file using a Python script
* Loaded 100K+ records into MySQL

### SQL Analysis

After loading the data into MySQL, I created **12 business-oriented SQL queries** to analyze the e-commerce data.

Some of the queries include:

* Month-over-month revenue growth using `LAG()`
* Running revenue totals using `SUM() OVER()`
* State-wise revenue ranking using `RANK()` and `DENSE_RANK()`
* Customer/market segmentation using `CASE WHEN`
* Duplicate detection using `GROUP BY` and `HAVING`
* Aggregations for revenue and order analysis
* CTE-based analysis
* Subquery-based analysis

## 🛠️ Technologies Used

* **Python**
* **Pandas**
* **MySQL**
* **SQL**
* **mysql-connector**
* **Git**
* **GitHub**

## 📊 Database Work

The cleaned data was loaded into MySQL for further analysis.

I practiced:

* Creating database tables
* Defining columns and data types
* Batch inserting large amounts of data
* Writing analytical SQL queries
* Using indexes for database performance
* Validating the loaded data

## 🧠 SQL Concepts Practiced

This project helped me practice SQL concepts that are commonly used in data engineering and analytics:

* `JOIN`
* `GROUP BY`
* `HAVING`
* `CASE WHEN`
* Subqueries
* CTEs
* Window Functions
* `RANK()`
* `DENSE_RANK()`
* `LAG()`
* `SUM() OVER()`
* Aggregations

## 📁 Project Flow

```text
Olist CSV Dataset
       ↓
Python / Pandas
       ↓
Data Cleaning
       ↓
Data Transformation
       ↓
Join Related Datasets
       ↓
orders_cleaned.csv
       ↓
MySQL
       ↓
SQL Analysis
       ↓
Business Insights
```

## 📦 Dataset

**Brazilian E-Commerce Public Dataset by Olist**

The dataset contains real Brazilian e-commerce information from **2016–2018** and includes more than **100K orders**.

The project uses data related to:

* Orders
* Customers
* Payments
* Order Items

## 🎯 What I Learned

Through this project, I practiced how an ETL workflow works from beginning to end:

**Extract → Transform → Load → Analyze**

I also got hands-on experience working with a larger dataset instead of only practicing SQL with small example tables.

The project gave me practical experience with **Python/Pandas for data processing, MySQL for storage, and SQL for analysis**.
