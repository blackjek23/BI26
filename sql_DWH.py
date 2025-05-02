# Import necessary libraries
import new_return
import pyodbc                       # For connecting and interacting with SQL Server
import pandas as pd                 # For data manipulation

# Define database connection parameters
SERVER_NAME = r'DESKTOP-M7H6422\MSSQLSERVER01'       # Local SQL Server instance
DWH_NAME = 'BI26-DWH'
DATABASE_NAME = 'BI26'                               # Target database name

# Construct and establish SQL Server connection
connectionString = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER_NAME};DATABASE={DATABASE_NAME};Trusted_Connection=yes'
connection_object: pyodbc.Connection = pyodbc.connect(connectionString)  # Connect to the database
cursor_object: pyodbc.Cursor = connection_object.cursor()                # Create a cursor to execute SQL commands

# Construct and establish SQL Server connection
connectionString_DWH = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER_NAME};DATABASE={DWH_NAME};Trusted_Connection=yes'
connection_object_DWH: pyodbc.Connection = pyodbc.connect(connectionString_DWH)  # Connect to the database
cursor_object_DWH: pyodbc.Cursor = connection_object_DWH.cursor()                # Create a cursor to execute SQL commands

def prosses_df_for_use():
    tickers_list= []
    for row in cursor_object.tables():
        if row[1] == 'dbo':
            tickers_list.append(row[2])
    return tickers_list

# Function to create a table for each ETF listed in ETF_list.csv
def create_DWH_tables():
        for Symbol in prosses_df_for_use():
            create_table_query = f'''
            IF object_id('{Symbol}') IS NULL                   -- Check if table already exists
            CREATE TABLE [BI26-DWH].[dbo].[{Symbol}]               -- Create table with ticker as name
            (
                [Date] DATE NOT NULL,                          -- Columns for historical price data
                [Close] FLOAT NOT NULL,
                [Daily Return] FLOAT NOT NULL,
                [Monthly Return] FLOAT NOT NULL,
                [Yearly Return] FLOAT NOT NULL
            ) 
            '''
            cursor_object_DWH.execute(create_table_query)          # Execute SQL query to create table
            cursor_object_DWH.commit()                             # Commit changes to the database


# Function to download historical data and insert it into each corresponding ETF table
def insert_data_to_DWH():
    for Symbol in prosses_df_for_use():
        select_query= f'SELECT [Date],[Close] FROM [BI26].[dbo].[{Symbol}]'
        df_records= cursor_object.execute(select_query).fetchall()
        new_df= pd.DataFrame.from_records(
            data= df_records,
            columns= ['Date', 'Close']
            )
        processed_df= new_return.calculate_returns_from_csv(new_df)
        sql_insert = f'''
                    INSERT INTO [BI26-DHW].[dbo].[{Symbol}]            -- Insert data into the corresponding table
                    (
                        [Date],
                        [Close],
                        [Daily Return],
                        [Monthly Return],
                        [Yearly Return]
                    )
                    VALUES
                    (
                        ?, ?, ?, ?, ?                         -- Parameterized query for bulk insert
                    )
                    '''
        # print(processed_df)
        df_records = processed_df.values.tolist()                   # Convert DataFrame to list of records
        cursor_object_DWH.executemany(sql_insert, df_records)     # Bulk insert data into SQL Server
        cursor_object_DWH.commit()                                # Commit after each insert

if __name__ == '__main__':
    create_DWH_tables()
    insert_data_to_DWH()