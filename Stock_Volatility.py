import os
import pandas as pd
import numpy as np

# Define the directory where CSV files are stored
DATA_DIR = 'datasets'

def calculate_volatility(file_path):
    """Calculate the annualized volatility for a given stock CSV file."""
    df = pd.read_csv(file_path)
    if 'Close' not in df.columns or 'Date' not in df.columns:
        print(f"Skipping {file_path}, 'Close' or 'Date' column not found.")
        return None
    
    df['Date'] = pd.to_datetime(df['Date'])
    df.sort_values(by='Date', ascending=True, inplace=True)
    df['Returns'] = df['Close'].pct_change()
    
    volatility = np.std(df['Returns'].dropna()) * 100
    return volatility

def main():
    volatility_scores = {}
    
    for file in os.listdir(DATA_DIR):
        if file.endswith(".csv"):
            file_path = os.path.join(DATA_DIR, file)
            stock_symbol = file.replace(".csv", "")
            
            vol = calculate_volatility(file_path)
            if vol is not None:
                volatility_scores[stock_symbol] = vol
    
    # Convert to DataFrame and save results
    vol_df = pd.DataFrame(list(volatility_scores.items()), columns=['Stock', 'Volatility'])
    vol_df.sort_values(by='Volatility', ascending=False, inplace=True)  # Sort by volatility
    vol_df.to_csv("volatility_scores.csv", index=False)
    print("Volatility scores saved to volatility_scores.csv")

if __name__ == "__main__":
    main()
