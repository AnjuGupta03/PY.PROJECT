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
df = pd.read_csv(r"/Users/anjugupta/Desktop/employee.csv")#creating data frame
l_df = pd.read_csv(r"/Users/anjugupta/Desktop/location.csv")
print(df)
print(l_df)

for col in df.itertuples():
    cursor.execute("""Insert into staging_db.employee_staging(EmployeeID,FirstName,LastName,PhoneNumber,JoinDate,LocationID) VALUES (%s,%s,%s,%s,%s,%s)
        """,
                           (col[1], col[2], col[3], col[4], col[5], col[6]))
for col in l_df.itertuples():
    cursor.execute("""Insert into staging_db.location_staging(LocationID, LocationName,LocationCity) VALUES (%s,%s,%s)
        """,
                           (col[1], col[2], col[3]))

conn.commit()
print("done")