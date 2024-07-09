import pandas as pd
import requests 
import json
from datetime import datetime
from tbl_paths import  LIST_COINS
import yfinance as yf

def get_coins_from_api():
    columns = ['symbol', 'name', 'supply', 'maxSupply',"volume24h"]
    # Base URL with the ids parameter
    base_url = "https://api.coincap.io/v2/assets"

    res = requests.get(base_url)
    data = json.loads(res.text)
    # Assuming 'data' is a dictionary containing a 'data' key with a list of rows
    try:
        rows = data['data']  # Access the list of rows from the 'data' key
    except KeyError:
        print("Error: 'data' key not found in the provided dictionary.")
        raise  # Re-raise the exception for further handling if necessary

    df = pd.DataFrame(columns = columns)
    for row in rows:
        symbol = row.get('symbol', None)  # Handle potential missing 'symbol' key
        name = row.get('id', None)          # Handle potential missing 'id' key (use original 'id' here)
        supply = row.get('supply', None)   # Handle potential missing 'supply' key
        max_supply = row.get('maxSupply', 9999999.999999)  # Replace nulls with 9999999
        volume24h = row.get('volumeUsd24Hr',0)
        if max_supply is None: 
            max_supply = 99999999999
        df.loc[len(df)] = [symbol, name, supply, max_supply, volume24h]
    return df

def get_dailycoin_from_api(ticker, start_date):
    # get data to current date
    df = yf.download(ticker, start_date, end=datetime.today().strftime('%Y-%m-%d'))
    df.reset_index(inplace=True)  
    # Convert the Date column to datetime format
    df['Date'] = pd.to_datetime(df['Date'])
    # Rename the columns Adj Close to AdjClose
    df.rename(columns={'Adj Close': 'AdjClose'}, inplace=True)
    return df