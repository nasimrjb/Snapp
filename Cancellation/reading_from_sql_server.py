from sqlalchemy import create_engine
import pandas as pd

# Create SQLAlchemy engine
engine = create_engine('mssql+pyodbc://@NASIMRAJABI/AdventureWorks2016?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes')

# Now use it with pandas
df = pd.read_sql("SELECT * FROM Sales.SalesOrderDetail", engine)


# Now df contains your data
# print(df.head())
# print(df['BusinessEntityID'].head())
# print(df.describe())
print(df.columns)
print(df.head)