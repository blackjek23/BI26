# Import necessary libraries
import csv                          # For reading the ETF list from a CSV file
import pyodbc                       # For connecting and interacting with SQL Server
import pandas as pd                 # For data manipulation
import yfinance as yf               # For downloading historical ETF data from Yahoo Finance

# Define database connection parameters
SERVER_NAME = r'DESKTOP-M7H6422\MSSQLSERVER01'       # Local SQL Server instance
DATABASE_NAME = 'BI26'                               # Target database name

# Construct and establish SQL Server connection
connectionString = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER_NAME};DATABASE={DATABASE_NAME};Trusted_Connection=yes'
connection_object: pyodbc.Connection = pyodbc.connect(connectionString)  # Connect to the database
cursor_object: pyodbc.Cursor = connection_object.cursor()                # Create a cursor to execute SQL commands

# Function to create a table for each ETF listed in ETF_list.csv
def create_db_tables():
    with open('ETF_list.csv', newline='') as csvfile:          # Open CSV file with ETF tickers
        spamreader = csv.DictReader(csvfile)                   # Read rows as dictionaries
        for row in spamreader:
            Symbol = (row['Ticker'])                           # Get the ticker symbol from the CSV
            create_table_query = f'''
            IF object_id('{Symbol}') IS NULL                   -- Check if table already exists
            CREATE TABLE [BI26].[dbo].[{Symbol}]               -- Create table with ticker as name
            (
                [Date] DATE NOT NULL,                          -- Columns for historical price data
                [Close] FLOAT NOT NULL,
                [High] FLOAT NOT NULL,
                [Low] FLOAT NOT NULL,
                [Open] FLOAT NOT NULL,
                [Volume] INT NOT NULL
            ) 
            '''
            cursor_object.execute(create_table_query)          # Execute SQL query to create table
            cursor_object.commit()                             # Commit changes to the database

# Function to download historical data and insert it into each corresponding ETF table
def insert_data_to_tables():
    with open('ETF_list.csv', newline='') as csvfile:          # Open the same CSV with ETF tickers
        spamreader = csv.DictReader(csvfile)
        for row in spamreader:
            Symbol = (row['Ticker'])                           # Extract the ticker symbol
            end_date = '2024-12-31'                            # Set the end date for historical data
            df = yf.download(Symbol, end=end_date,
                            interval="1d",
                            multi_level_index=False,
                            auto_adjust= True)                 # Download data
            df['Date'] = df.index                              # Move the index into a 'Date' column
            df = df[['Date', 'Close', 'High', 'Low', 'Open', 'Volume']]  # Keep only relevant columns
            sql_insert = f'''
                INSERT INTO [BI26].[dbo].[{Symbol}]            -- Insert data into the corresponding table
                (
                    [Date],
                    [Close],
                    [High],
                    [Low],
                    [Open],
                    [Volume]
                )
                VALUES
                (
                    ?, ?, ?, ?, ?, ?                          -- Parameterized query for bulk insert
                )
                '''
            df_records = df.values.tolist()                   # Convert DataFrame to list of records
            cursor_object.executemany(sql_insert, df_records) # Bulk insert data into SQL Server
            cursor_object.commit()                            # Commit after each insert

# Run both functions: create tables and populate them with data
if __name__ == "__main__":
    create_db_tables()
    insert_data_to_tables()