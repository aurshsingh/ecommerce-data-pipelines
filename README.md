# E-Commerce Data Pipeline

End-to-end ETL pipeline processing 100,000+ real e-commerce orders.

## What This Does
- Loads 4 raw CSV datasets using Python/Pandas
- Cleans data: date conversion, null removal, deduplication
- Joins 4 tables (orders + customers + payments + items)
- Created new orders_cleaned file using python script.
- Loads 100K+ rows into MySQL database
- Runs 12 business analytics SQL queries

## Skills Demonstrated
- Python (Pandas, mysql-connector)
- SQL: Window Functions, CTEs, Subqueries, CASE WHEN, Aggregations
- MySQL: Schema design, batch insertion, indexing
- Git, GitHub

## Key Queries
- Month-over-month revenue growth using LAG window function
- Running totals with SUM OVER cumulative window
- State revenue ranking with RANK and DENSE_RANK
- Market segmentation with CASE WHEN
- Duplicate detection with GROUP BY + HAVING

## Dataset
Brazilian E-Commerce Dataset by Olist (Kaggle)
100K+ orders | 4 tables | Real Brazilian e-commerce data from 2016-2018
