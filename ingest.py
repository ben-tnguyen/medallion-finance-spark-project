import yfinance as yf
import pandas as pd
import os
from datetime import datetime


TICKERS = ['AAPL', 'MSFT', 'GOOGL', 'CADUSD=X'] 
START_DATE = '2023-01-01'
END_DATE = datetime.now().strftime('%Y-%m-%d')

def setup_bronze_layer():
    """Creates a 'Bronze' landing zone partitioned by current date."""
    load_date = datetime.now().strftime('%Y-%m-%d')
    path = f"data/bronze/{load_date}"
    os.makedirs(path, exist_ok=True)
    return path

def run_ingestion():
    print(f"🚀 Starting data pull for: {TICKERS}")
    save_path = setup_bronze_layer()
    
    # Download adjusted prices to account for stock splits
    data = yf.download(TICKERS, start=START_DATE, end=END_DATE, group_by='ticker', auto_adjust=True)
    
    for ticker in TICKERS:
        # Extract data and add audit metadata (Data Engineering best practice)
        df = data[ticker].copy().reset_index()
        df['ingested_at'] = datetime.now()
        df['source_system'] = 'Yahoo Finance'
        
        # Save to local 'Data Lake' structure
        file_name = f"{save_path}/{ticker}_raw.csv"
        df.to_csv(file_name, index=False)
        print(f"Created: {file_name}")

if __name__ == "__main__":
    run_ingestion()
    print("\n Phase 1 Complete. Bronze Layer populated!")