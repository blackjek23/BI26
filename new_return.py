import pandas as pd

def calculate_returns_from_csv(df):
    df= pd.DataFrame(df)
    df['Date'] = pd.to_datetime(df['Date'])
    # Calculate daily returns
    df['Daily_Return'] = df['Close'].pct_change().round(3)
    
    # Compute Month-to-Date (MTD) returns
    df['Month'] = df['Date'].dt.to_period('M')
    df['First Close of Month'] = df.groupby('Month')['Close'].transform('first')
    df['Monthly_Return'] = ((df['Close'] / df['First Close of Month']) - 1).round(3)
    
    # Compute Year-to-Date (YTD) returns
    df['Year'] = df['Date'].dt.to_period('Y')
    df['First Close of Year'] = df.groupby('Year')['Close'].transform('first')
    df['Yearly_Return'] = ((df['Close'] / df['First Close of Year']) - 1).round(3)
            
    # Drop temporary columns
    df.drop(columns=['Month', 'First Close of Month', 'Year', 'First Close of Year'], inplace=True)
    return df.fillna(0)