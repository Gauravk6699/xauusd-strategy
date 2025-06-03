import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

# Define the ticker symbol for Gold
ticker_symbol = "XAUUSD=X"

# Use period="60d" for fetching data
print(f"Fetching data for {ticker_symbol} for the last 60 days with 5-minute interval.")

# Attempt to download the data
try:
    data = yf.download(ticker_symbol, period="60d", interval="5m")

    # Check if the DataFrame is empty
    if data.empty:
        print(f"No data returned for {ticker_symbol}. The ticker might be incorrect or data unavailable for the period/interval.")
    else:
        # Print the head and tail of the DataFrame
        print("\nData head:")
        print(data.head())
        print("\nData tail:")
        print(data.tail())

        # Ensure the DataFrame contains required columns
        required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            print(f"\nMissing columns: {', '.join(missing_columns)}")
        else:
            print("\nAll required columns ('Open', 'High', 'Low', 'Close', 'Volume') are present.")

        # Check and print the timezone information of the DataFrame's index
        if data.index.tz is not None:
            print(f"\nDataFrame index timezone: {data.index.tz}")
        else:
            print("\nDataFrame index is timezone-naive.")

except Exception as e:
    print(f"\nAn error occurred during data download: {e}")

print("\nScript execution finished.")
