import requests
import json
import pandas as pd

api_key = 'yrvi5vopzCZOmhfdIVD2Fbsn3jsnqXWwXHrr3XKVSsiSwtQPX3RpUtoxj4dWdJj4'
api_secret = 'eRNU7akEhgsNJY0ikRamDL7pLqQxbqI2o3tUbpMmd3ll8KXguq6kAy0Jt1tIkx01'

# Creating the endppoint URL for getting price
url = 'https://api1.binance.com'
api_call = '/api/v3/ticker/price'       # choosing the endpoint
headers = {'content-type': 'application/json', 'X-MBX-APIKEY': api_key}     # specify that i want a response to my request to be in JSON format

# Making the get request
response = requests.get(url + api_call, headers=headers)       # .text method reads request body and returns it as a string
response = json.loads(response.text)                # response.text usually contains the content of the response (payload) which is usually json data, html content, or text
                                                    # json.loads from json module in python's std library. parses a JSON-formatted data from a string (the response.text, which is an extra step to extract the json formatted data from the response) and converts it to a python data structure (usually dictionary or list, depending on the json data)
                                                    # Doesn't handle unicode encoding issues, whereas response.json() does
df = pd.DataFrame.from_records(response)            # converts a sequence of records (dictionaries, tuples) such as database query results or JSON objects to pandas dataframe object
print(df.head())                                    # returns first 5 rows


# ----------------------------------------------- USING REQUESTS LIBRARY IN PYTHON --------------------------------------------- #
"""
# Getting past 24h of data
api_call = '/api/v3/klines'       # choosing the endpoint
headers = {'content-type': 'application/json', 'X-MBX-APIKEY': api_key}

interval = '1m'
start_time = '0'                                    # UNIX timestamp for start of the day (midnight UTC)
end_time = '9999999999999'                          # UNIX timestamp for the end of the day (11:59:59 PM UTC)

params = {
    'symbol': 'BTCUSDT',
    'interval': interval,
    'startTime': start_time,
    'endTime': end_time,
}

endpoint = url+api_call
response = requests.get(endpoint, headers=headers, params=params)

data = response.json()                              # Requests library, in the response object. Parses response content as JSON and returns a python datastructure (dict, list). Automatically handles content-type header
                                                    # Produces a list of lists
# for price in data:
    # print(price)

column_names = ['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close Time', 'Quote Asset Volume', 'Number of Trades', 'Taker Buy Base Asset Volume', 'Taker Buy Quote Asset Volume']
# open and close indicate open and close for the candlestick of requested time interval (in this case 1min)

df = pd.DataFrame.from_records(data)
df.drop(df.columns[-1], axis=1, inplace=True)          # inplace=True returns none if assigned to a new variable. Either do inplace or assign to df again. MUST explicitly state axis
                                                     # even if you are using df.index[-1] or df.columns[-1]
df.columns = column_names"""

# ------------------------ ORDERBOOK WITH REQUESTS ------------------------- #

# request to API
api_call = '/api/v3/depth'
headers = {'content-type': 'application/json', 'X-MBX-APIKEY': api_key}

params = {
    'symbol': 'BTCUSDT',
}

endpoint = url+api_call
response = requests.get(endpoint, headers=headers, params=params)

# response to json
order_book = response.json()
df = pd.json_normalize(order_book)
df.to_csv('depth.csv')


# json to bids, asks, orderbook dataframes
bids = pd.DataFrame.from_records(order_book['bids'])
bids.columns = ['price','bids']
bids['price'] = pd.to_numeric(bids['price'])
bids['bids'] = pd.to_numeric(bids['bids'])

asks = pd.DataFrame.from_records(order_book['asks'])
asks.columns = ['price','asks']
asks['price'] = pd.to_numeric(asks['price'])
asks['asks'] = pd.to_numeric(asks['asks'])

df_orderbook = pd.concat([bids,asks], axis=0, join='outer', ignore_index=True).fillna(0)                # Concatenates the two dfs vertically, then must group. Ignore_index preserves the index after concat. Here, we don't need that.
df_orderbook = df_orderbook.groupby('price').agg({'bids': 'first', 'asks': 'first'}).reset_index()
df_orderbook.sort_values(by=['price'], ascending=True)






