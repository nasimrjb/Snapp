import pyodbc
import pandas as pd

# 1. Define the connection string based on your original setup
# Note: The connection string format for pyodbc might be slightly different,
# but the key components are the same.
conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=NASIMRAJABI;"  # Replace with your actual server instance name if different
    "DATABASE=AdventureWorks2016;"
    "Trusted_Connection=yes;"
)

# 2. Establish the connection (DB-API connection object)
try:
    cnxn = pyodbc.connect(conn_str)

    # 3. Create a cursor object (DB-API cursor object)
    cursor = cnxn.cursor()

    # 4. Execute the SQL query
    sql_query = "SELECT * FROM Sales.SalesOrderDetail"
    cursor.execute(sql_query)

    # 5. Fetch the results and convert to DataFrame (Pandas step)
    # Get column names from the cursor description
    columns = [column[0] for column in cursor.description]

    # Fetch all rows
    rows = cursor.fetchall()

    # Create the DataFrame
    df = pd.DataFrame.from_records(rows, columns=columns)

    print("Data successfully loaded into DataFrame using DB-API pattern.")
    print(df.head())

except pyodbc.Error as ex:
    sqlstate = ex.args[0]
    print(f"Database Error Occurred: {sqlstate}")

finally:
    # 6. Close the connection
    if 'cnxn' in locals() and cnxn:
        cnxn.close()
