# PYTHON-BINANCE IS A THIRD PARTY LIBRARY THAT WRAPS ALL THESE CALLS
from binance.client import Client
import os
import pandas as pd
import datetime
import matplotlib.pyplot as plt
import pdb

def pause():
	return pdb.set_trace()

# Connecting to endpoint thru client
api_key = 'yrvi5vopzCZOmhfdIVD2Fbsn3jsnqXWwXHrr3XKVSsiSwtQPX3RpUtoxj4dWdJj4'
api_secret = 'eRNU7akEhgsNJY0ikRamDL7pLqQxbqI2o3tUbpMmd3ll8KXguq6kAy0Jt1tIkx01'
client = Client(api_key, api_secret, testnet=True)

# Get all tickers of the exchange
coin_info = client.get_all_tickers()
df_all_coins = pd.DataFrame.from_records(coin_info)
print(df_all_coins)
# Getting Binance Server Time
res = client.get_server_time()                              # Prints as a dictionary
timestamp = res['serverTime']/1000                          # Convert unix timestamp [ms] to [s]

dt = datetime.datetime.fromtimestamp(timestamp)             # fromtimestamp converts unix timestamp [s] to datetime object representing the date and time. Python's default string representation is in the format YYYY-MM-DD HH:MM:SS, so no need for strftime
dt.strftime("%Y-%m-%d %H:%M:%S")                            # converts datetime object to a string, includes microseconds

# Getting detailed exchange info
exchange_info = client.get_exchange_info()                  # Returns list of product dictionaries for trade limits, symbols, etc.
df = pd.DataFrame(exchange_info['symbols'])                 # exchange_info returns dictionaries inside a dictionary. Must choose the dictionary to convert to a DF

# Getting exchange details on symbol
symbol_info = client.get_symbol_info('BTCUSDT')             # returns dictionary of dictionaries on symbol's info. Same as above
df2 = pd.DataFrame(symbol_info['filters'])

# Getting market depth
market_depth = client.get_order_book(symbol='BTCUSDT')      # returns nested dictionaries again
bids = pd.DataFrame(market_depth['bids'])
bids.columns = ['price','bids']
asks = pd.DataFrame(market_depth['asks'])
asks.columns = ['price','asks']

bids['price'] = pd.to_numeric(bids['price'])
bids['bids'] = pd.to_numeric(bids['bids'])
asks['price'] = pd.to_numeric(asks['price'])
asks['asks'] = pd.to_numeric(asks['asks'])

df_orderbook = pd.concat([bids,asks], axis=0, join='outer', ignore_index=True).fillna(0)                # Concatenates the two dfs vertically, then must group. Ignore_index preserves the index after concat. Here, we don't need that.
df_orderbook = df_orderbook.groupby('price').agg({'bids': 'first', 'asks': 'first'}).reset_index()
df_orderbook.sort_values(by=['price'], ascending=True)
#print(df_orderbook)

mid_index = df_orderbook['bids'].idxmin()
mid_price = (df_orderbook.iloc[mid_index - 1,0] + df_orderbook.iloc[mid_index,0] )/2















