import mysql.connector
import pandas as pd

# Connect to MySQL
conn_staging = mysql.connector.connect(
    host='localhost',
    port='3306',
    user='root',
    password='root@123',
    database='staging_db'
)

conn_conformed = mysql.connector.connect(
    host='localhost',
    port='3306',
    user='root',
    password='root@123',
    database='confromed_db'
)

# Create a cursor
cursor_staging = conn_staging.cursor()
# Create a cursor
cursor_conformed = conn_conformed.cursor()    #cursor is use for execution of sql.
employee_info_query="""SELECT
        EmployeeId,
        FirstName,
        LastName,
        JoinDate,
        LocationName
    from staging_db.employee_staging e
    join staging_db.location_staging l
    on l.LocationID = e.LocationID;
"""

#creating dataframe
df_employee_info = pd.read_sql_query(employee_info_query,conn_staging)
print(df_employee_info)

for col in df_employee_info.itertuples():
    cursor_conformed.execute("""Insert into confromed_db.employee_info(EmployeeID,FirstName,LastName,JoinDate,LocationName) VALUES (%s,%s,%s,%s,%s)
        """,
                           (col[1], col[2], col[3], col[4], col[5]))

location_info_query = """select  LocationID as location,LocationName,LocationCity
  from staging_db.location_staging;
"""
df_location = pd.read_sql_query(location_info_query,conn_staging)
print(df_location)
for col in df_location.itertuples():
    cursor_conformed.execute("""Insert into confromed_db.location_info(Location,LocationName,LocationCity) VALUES (%s,%s,%s)
        """,
                           (col[1], col[2], col[3]))

Employee_diary_query = """SELECT UPPER(CONCAT(FirstName, ' ', LastName)) AS FullName,LastName, PhoneNumber, l.LocationCity
FROM staging_db.employee_staging e
JOIN staging_db.location_staging l ON e.LocationID = l.LocationID;
"""
df_employee_diary = pd.read_sql_query(Employee_diary_query,conn_staging)
print(df_employee_diary)

for col in df_employee_diary.itertuples():
    cursor_conformed.execute("""Insert into confromed_db.employee_diary(FullName,LastName,Clean_PhoneNumber,LocationCity) VALUES (%s,%s,%s,%s)
        """,
                           (col[1], col[2], col[3], col[4]))
conn_conformed.commit()
conn_conformed.close()
conn_staging.commit()
cursor_staging.close()
conn_staging.close()
print("done")