import mysql.connector
import pandas as pd
# Replace these values with your own database connection details
host = "localhost"
user = "root"
password = "root@123"
database = "staging_db"

# Connect to MySQL
conn = mysql.connector.connect(
    host='localhost',
    port='3306',
    user='root',
    password='root@123',
    database='staging_db'
)

# Create a cursor
cursor = conn.cursor()
df = pd.read_csv(r"/Users/anjugupta/Desktop/employee.csv")
print(df)
# # Example: Execute a simple query
# query = "select * from staging_db.employee_staging"
#
# cursor.execute(query)
#
# # Fetch and print the results
# for row in cursor.fetchall():
#     print(row)
#
# # Close the cursor and connection
# cursor.close()
# conn.close()

