import sqlite3
import pandas as pd


# how to check table names in a database
# with sqlite3.connect('sales_Canada_practice.db') as conn:

#     #1 alternative
#     cursor = conn.cursor()
#     cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
#     tables = cursor.fetchall()

#     print('with Cursor: ', tables)

#     #2 alternative
#     result = pd.read_sql('SELECT name FROM sqlite_master WHERE type="table"', conn)

#     print('with Pandas: ', result)


# how to check data in a specific table

with sqlite3.connect('sales_Canada_practice.db') as conn:
    result = pd.read_sql('SELECT * FROM Sales LIMIT 10', conn)
    print(result)
    print(result.columns)