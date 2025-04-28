import yfinance as yf
import pandas as pd
import csv

def download_stocks_data():
    end_date = '2024-12-31'

    with open('ETF_list.csv', newline='') as csvfile:
        spamreader = csv.DictReader(csvfile)
        for row in spamreader:
            Symbol= (row['Ticker'])
            df = yf.download(Symbol, end=end_date, interval="1d")
            df.columns = [col[0] for col in df.columns]
            df.to_csv(f'./datasets/{Symbol}.csv')

if __name__ == "__main__":
    download_stocks_data()