import pandas as pd
import mysql.connector
from mysql.connector import Error

# Load the cleaned data we made yesterday
df = pd.read_csv("orders_cleaned.csv")
print(f"Rows to load: {len(df)}")

#To see exactly which columns had the NaNs
print(df.isna().sum())  

# Fill NaN values with None so MySQL accepts them
# MySQL does not understand Python's NaN
# MySQL understands NULL which is Python's None
#df = df.where(pd.notna(df), None)  it doesn't convert nan to none for numeric values
df = df.astype(object).where(pd.notna(df), None)

try:
    # Create connection to MySQL
    # host = where MySQL is running (our own computer = localhost)
    # user = MySQL username
    # password = your MySQL password (change this)
    # database = which database to use
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password = input("Enter MySQL password: "),
        database="ecommerce"
    )
    
    print("Connected to MySQL!")
    cursor = connection.cursor()
    
    # Drop table if it already exists
    # This lets us run this script multiple times safely
    cursor.execute("DROP TABLE IF EXISTS orders")
    print("Dropped existing table")
    
    # Create the table
    # Define each column name and its data type
    cursor.execute("""
        CREATE TABLE orders (
            order_id VARCHAR(50),
            customer_id VARCHAR(50),
            order_status VARCHAR(30),
            year INT,
            month INT,
            day INT,
            customer_city VARCHAR(100),
            customer_state VARCHAR(10),
            payment_type VARCHAR(30),
            payment_value FLOAT,
            payment_installments INT
        )
    """)
    print("Table created")
    
    # Insert data in batches
    # Why batches? Inserting 100,000 rows one at a time is very slow
    # Inserting 1000 rows at a time is much faster
    
    # Only include columns that actually exist in our data
    columns = [
        'order_id', 'customer_id', 'order_status',
        'year', 'month', 'day',
        'customer_city', 'customer_state',
        'payment_type', 'payment_value', 'payment_installments'
    ]
    existing_cols = [c for c in columns if c in df.columns]
    df_insert = df[existing_cols]
    
    batch_size = 1000
    total = 0
    
    for i in range(0, len(df_insert), batch_size):
        batch = df_insert.iloc[i:i + batch_size]
        
        # Convert each row to a tuple for MySQL
        values = [tuple(row) for row in batch.itertuples(index=False)]
        
        # Create the INSERT statement dynamically
        cols_str = ', '.join(existing_cols)
        placeholders = ', '.join(['%s'] * len(existing_cols))
        
        cursor.executemany(
            f"INSERT INTO orders ({cols_str}) VALUES ({placeholders})",
            values
        )
        connection.commit()
        total += len(batch)
        print(f"Loaded {total} / {len(df_insert)} rows...")
    
    print(f"\nSuccess! Total rows loaded: {total}")

except Error as e:
    print(f"MySQL Error: {e}")

finally:
    # Always close connection even if error occurred
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("Connection closed")