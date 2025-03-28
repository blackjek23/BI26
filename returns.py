import pandas as pd
import os

def calculate_returns_from_csv(directory):
    output_directory = os.path.join(directory, 'DWH')
    os.makedirs(output_directory, exist_ok=True)
    
    for file in os.listdir(directory):
        if file.endswith(".csv"):
            file_path = os.path.join(directory, file)
            data = pd.read_csv(file_path, parse_dates=['Date'])
            
            if 'Close' not in data.columns:
                print(f"Skipping {file}, missing 'Close' column.")
                continue

            # Calculate daily returns
            data['Daily_Return'] = data['Close'].pct_change().round(3)
            
            # Compute Month-to-Date (MTD) returns
            data['Month'] = data['Date'].dt.to_period('M')
            data['First Close of Month'] = data.groupby('Month')['Close'].transform('first')
            data['Monthly Return'] = ((data['Close'] / data['First Close of Month']) - 1).round(3)
            
            # Compute Year-to-Date (YTD) returns
            data['Year'] = data['Date'].dt.to_period('Y')
            data['First Close of Year'] = data.groupby('Year')['Close'].transform('first')
            data['Yearly Return'] = ((data['Close'] / data['First Close of Year']) - 1).round(3)
                    
            # Drop temporary columns
            data.drop(columns=['Month', 'First Close of Month', 'Year', 'First Close of Year'], inplace=True)
            
            # Extract ticker name by removing file extension
            ticker = file.replace(".csv", "")
            
            # Save returns summary to DWH folder
            data.to_csv(os.path.join(output_directory, f"{ticker}.csv"), index=False)
            
            print(f"Processed {file}")

if __name__ == "__main__":
    directory = 'datasets'
    calculate_returns_from_csv(directory)