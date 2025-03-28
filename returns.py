import pandas as pd
import os

def calculate_returns_from_csv(directory):
    output_directory = os.path.join(directory, 'DWH')
    os.makedirs(output_directory, exist_ok=True)
    
    for file in os.listdir(directory):
        if file.endswith(".csv"):
            file_path = os.path.join(directory, file)
            data = pd.read_csv(file_path, parse_dates=['Date'], index_col='Date')
            
            if 'Close' not in data.columns:
                print(f"Skipping {file}, missing 'Close' column.")
                continue
            
            # Extract ticker name by removing file extension
            ticker = file.replace(".csv", "")

            # Calculate daily returns
            data['Return'] = data['Close'].pct_change()
            
            # Compute yearly returns
            yearly_returns = data['Return'].resample('YE').sum() * 100  # Convert to percentage
            yearly_returns = yearly_returns.round(3)  # Round to 3 decimal places
            
            # Compute monthly returns
            monthly_returns = data['Return'].resample('ME').sum() * 100  # Convert to percentage
            monthly_returns = monthly_returns.round(3)  # Round to 3 decimal places
            
            # Save results to DWH folder
            yearly_returns.to_csv(os.path.join(output_directory, f"{ticker}_yearly_returns.csv"))
            monthly_returns.to_csv(os.path.join(output_directory, f"{ticker}_monthly_returns.csv"))
            
            print(f"Processed {file}")

if __name__ == "__main__":
    directory = 'datasets'
    calculate_returns_from_csv(directory)
