import yfinance as yf
import pandas as pd

def download_stocks_data():
    end_date = '2024-12-31'

    with open('stocks.txt', 'r') as f:
        for ticker in f:
            Symbol = ticker.strip()
            df = yf.download(Symbol, end=end_date, interval="1d")
            df.columns = [col[0] for col in df.columns]
            df.to_csv(f'./datasets/{Symbol}.csv')

if __name__ == "__main__":
    download_stocks_data()