import mysql.connector
import pandas as pd
import numpy as np

# MySQL connection details
connection = mysql.connector.connect(
    host="localhost",  # Your MySQL host
    user="root",       # Your MySQL user
    password="root",   # Your MySQL password
    database="big_data" # Your MySQL database
)

cursor = connection.cursor()

# Create table query
create_table_query = """
CREATE TABLE IF NOT EXISTS data_csv (
    Sno INT PRIMARY KEY,
    Date VARCHAR(50),
    Time VARCHAR(50),
    StateUnionTerritory VARCHAR(100),
    ConfirmedIndianNational INT,
    ConfirmedForeignNational INT,
    Cured INT,
    Deaths INT,
    Confirmed INT
);
"""

# Execute the create table query
cursor.execute(create_table_query)

# Path to the CSV file
csv_file = "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\covid_19_india.csv"

# Read the CSV file using pandas
df = pd.read_csv(csv_file)

# Replace '-' or any invalid integer values with NaN (which we will convert to None for SQL NULL)
df.replace('-', np.nan, inplace=True)

# Convert all NaN to None for SQL compatibility (None in Python will be treated as NULL in MySQL)
df = df.where(pd.notnull(df), None)

# Insert data into MySQL
for index, row in df.iterrows():
    insert_query = """
    INSERT INTO data_csv (Sno, Date, Time, StateUnionTerritory, ConfirmedIndianNational, 
                          ConfirmedForeignNational, Cured, Deaths, Confirmed)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.execute(insert_query, tuple(row))

# Commit the transaction
connection.commit()

print("Data inserted successfully")

# Close the cursor and connection
cursor.close()
connection.close()
