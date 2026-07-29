import pandas as pd

# ── LOAD ALL FILES ──────────────────────────────────────────
orders = pd.read_csv("olist_orders_dataset.csv")
customers = pd.read_csv("olist_customers_dataset.csv")
payments = pd.read_csv("olist_order_payments_dataset.csv")
items = pd.read_csv("olist_order_items_dataset.csv")

# How many rows and columns?
print("Files loaded:")
print(f"  Orders: {orders.shape}")
print(f"  Customers: {customers.shape}")
print(f"  Payments: {payments.shape}")
print(f"  Items: {items.shape}")

# ── CLEAN ORDERS ─────────────────────────────────────────────
# Convert text dates to real date objects
# Without this, Python treats dates as plain text
orders['order_purchase_timestamp'] = pd.to_datetime(
    orders['order_purchase_timestamp']
)
orders['order_delivered_customer_date'] = pd.to_datetime(
    orders['order_delivered_customer_date']
)

# Extract year, month, day as separate columns
# Useful for grouping and analysis later
orders['year'] = orders['order_purchase_timestamp'].dt.year
orders['month'] = orders['order_purchase_timestamp'].dt.month
orders['day'] = orders['order_purchase_timestamp'].dt.day

print("\nDate columns added. Sample:")
print(orders[['order_id', 'year', 'month', 'day']].head())

# Remove orders that were never approved
# These are incomplete records not useful for analysis
before = len(orders)
orders = orders.dropna(subset=['order_approved_at'])
after = len(orders)
print(f"\nRemoved {before - after} unapproved orders")
print(f"Remaining orders: {after}")

# ── JOIN TABLES ───────────────────────────────────────────────
# Same as SQL JOIN - combining tables on a common column
# left join = keep all orders even if no customer match
orders_customers = orders.merge(
    customers,
    on='customer_id',
    how='left'
)
print(f"\nAfter joining customers: {orders_customers.shape}")

orders_payments = orders_customers.merge(
    payments,
    on='order_id',
    how='left'
)
print(f"After joining payments: {orders_payments.shape}")

# ── SAVE CLEAN FILE ───────────────────────────────────────────
orders_payments.to_csv("orders_cleaned.csv", index=False)
print("\nSaved: orders_cleaned.csv")
print("Final shape:", orders_payments.shape)