# Import pandas library
# pandas lets us work with data in tables called DataFrames
import pandas as pd

# Load the orders CSV file into a DataFrame
# Think of DataFrame as an Excel sheet inside Python
orders = pd.read_csv("olist_orders_dataset.csv")

# How many rows and columns?
print("Shape:", orders.shape)

# What are the column names?
print("\nColumns:", orders.columns.tolist())

# Show first 5 rows
print("\nFirst 5 rows:")
print(orders.head())

# How many missing values in each column?
print("\nMissing values:")
print(orders.isnull().sum())

# Count unique values in order_status column
print("\nOrder status counts:")
print(orders['order_status'].value_counts())