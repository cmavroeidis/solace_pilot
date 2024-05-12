import pandas as pd
import websocket
import json
import requests
import numpy as np
import timeit
import os
"""
| response.text  |  reads response's body (payload, usually json data) and returns as string 
| json.loads(json string from msg response OR response.text)  |  parses valid json string to a python dictionary (JSON format object). From json module im python's std library  
| response.json()  |  from requests library. Parses response content from the response object's body as JSON and returns a python datastructure (dict, list). Automatically handles content-type header
| pd.DataFrame.from_records(response)  |  Converts a sequence of records (dicts, tuples) such as a list of JSON objects to a pandas df object
| pd.json_normalize(dict/ list of dicts)  |  normalize a semi-structured JSON string (or dictionary) into a flat table

| df.index = [100,200,300]  |  Sets the index of the dataframe (row index) to these values 
"""

api_key = 'yrvi5vopzCZOmhfdIVD2Fbsn3jsnqXWwXHrr3XKVSsiSwtQPX3RpUtoxj4dWdJj4'
api_secret = 'eRNU7akEhgsNJY0ikRamDL7pLqQxbqI2o3tUbpMmd3ll8KXguq6kAy0Jt1tIkx01'

data_filepath = 'data_3h\\'

# ------------- RESTful Depth snapshot ------------- #
url_rest = 'https://api1.binance.com'
api_call = '/api/v3/depth'
headers = {'content-type': 'application/json', 'X-MBX-APIKEY': api_key}
params = {
    'symbol': 'BTCUSDT',
	'limit': 5000
}
endpoint_rest = url_rest + api_call


#----------- WEBSOCKET connection ------------#
symbol = 'btcusdt'
stream = '@depth'
endpoint_ws = 'wss://stream.binance.com:9443'          # can also use f'....' to use {symbol} in a dynamic f-string
raw_stream = '/ws/'
url_socket = endpoint_ws + raw_stream + symbol + stream
sample_time_s = 60 * 60 * 4

def on_message(ws, message):                        # invoked when message is received from the server. ws is WebSocketApp instance representing the connection. message is the data
	global datastream, rows_received
	msg = json.loads(message)
	datastream.append(msg)
	rows_received +=1
	#print(rows_received)
	if rows_received  > sample_time_s:
		ws.close()

def on_error(ws, error): # invoked when an error occurs during the connection. error is the error message or exception object. Can define custom handling like logging the error, retrying the connection, etc.
	print(error)

def on_close(ws, close_status_code, close_msg):     # invoked when websocket connection is closed, either intentionally or due to error. statuscode indicates the reason for closure.
	print("### Connection Closed ###")

def on_open(ws):                                    # invoked when connection is successfully opened and ready to send/ receive msges
	global last_update_id_REST, df_snapshot, df_snapshot_RAW
	response = requests.get(endpoint_rest, headers=headers, params=params)
	df_snapshot_RAW = pd.json_normalize(response.json())
	df_snapshot = df_snapshot_RAW
	last_update_id_REST = df_snapshot['lastUpdateId'].iloc[0]
	print("Opened connection, data streaming.")


# ---------------- MAIN ------------------#
df_snapshot = pd.DataFrame()
df_snapshot_RAW = pd.DataFrame()
datastream = []
rows_received = 0
last_update_id_REST = 0

ws = websocket.WebSocketApp(url_socket, on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)
ws.run_forever()

# -------------- Building snapshot df -------------#
# Building updates df
df_updates_RAW = pd.DataFrame(datastream)
df_updates_complete = df_updates_RAW.copy()
df_updates_complete.columns = ['event_type', 'event_time', 'symbol', 'first_update_id', 'final_update_id', 'bids_to_updated', 'asks_to_updated']
df_updates_complete = df_updates_complete[df_updates_complete['final_update_id'] > last_update_id_REST]

# Unwrapping the orderbook snapshot
bids_snapshot = pd.DataFrame(df_snapshot['bids'].iloc[0], columns=['price', 'bids'])
asks_snapshot = pd.DataFrame(df_snapshot['asks'].iloc[0], columns=['price', 'asks'])

#bids_snapshot['price'] = pd.to_numeric(bids_snapshot['price'])
#bids_snapshot['bids'] = pd.to_numeric(bids_snapshot['bids'])
#asks_snapshot['price'] = pd.to_numeric(asks_snapshot['price'])
#asks_snapshot['asks'] = pd.to_numeric(asks_snapshot['asks'])

# Merge the asks and bids into one orderbook
df_snapshot_complete = pd.concat([bids_snapshot, asks_snapshot], axis=0, join='outer', ignore_index=True).fillna(0)                # Concatenates the two dfs vertically, then must group. Ignore_index preserves the index after concat. Here, we don't need that.
df_snapshot_complete = df_snapshot_complete.groupby('price').agg({'bids': 'first', 'asks': 'first'}).reset_index()
df_snapshot_complete.sort_values(by=['price'], ascending=True)

# -------------- Building concise updates df: [ timestamp[s], bids, asks ]-------------#
df_updates_nested = df_updates_complete[['event_time', 'bids_to_updated', 'asks_to_updated']]
first_timestamp = df_updates_nested['event_time'].iloc[0]
df_updates_nested.loc[:, 'event_time'] = df_updates_nested['event_time'] - first_timestamp       # modifying the original dataframe, not a copy or view of it

# -------------- Matching updates ----------#
df_snapshot_complete = df_snapshot_complete.apply(pd.to_numeric, errors='coerce')

def get_updated_order_book(updates, snapshot, time_elapsed):
	snapshot_copy = snapshot.copy()

	best_dict = {}

	for i, timestamp in enumerate(updates['event_time'], start=0):       # iterates row index AND timestamp at that row
		if timestamp > time_elapsed:
			break

		snapshot_copy.sort_values(by='price', inplace=True, ascending=True)
		snapshot_copy.reset_index(drop=True, inplace=True)

		best_bid_price, best_bid_vol = get_best_bid(snapshot_copy)
		best_ask_price, best_ask_vol = get_best_ask(snapshot_copy)

		best_dict[round(timestamp, -3)] = [best_bid_price, best_bid_vol,  best_ask_price, best_ask_vol]

		for update in updates['bids_to_updated'].iloc[i]:
			snapshot_copy = snapshot_copy.sort_values(by='price', ascending=True)
			snapshot_copy.reset_index(drop=True, inplace=True)
			if (np.float64(update[0]) < snapshot_copy['price'].iloc[0]) or (np.float64(update[0]) > snapshot_copy['price'].iloc[-1]):
				continue

			# get index in snapshot where price level on current update corresponds
			row_indices = snapshot_copy.index[snapshot_copy['price'] == np.float64(update[0])].tolist()         # uses boolean mask to iterate through all price levels in df_orderbook and match the price level from bids_to_updated

			# if update price isn't found
			if len(row_indices) == 0:
				# if update has 0 volume
				if np.float64(update[1]) == 0:
					continue

				# If finite volume, insert price level into snapshot_copy
				new_price = {'price': np.float64(update[0]), 'bids': np.float64(update[1]), 'asks': 0}
				new_price_to_add = pd.DataFrame([new_price])
				new_price_to_add = new_price_to_add.apply(pd.to_numeric, errors='coerce')
				snapshot_copy = pd.concat([snapshot_copy, new_price_to_add], axis=0, join='outer', ignore_index=True).fillna(0)
				continue

			# Update the new bid
			for index in row_indices:
				if np.float64(update[1]) == 0:
					snapshot_copy = snapshot_copy.drop(index)
				else:
					snapshot_copy.loc[index, 'bids'] = np.float64(update[1])

		for update in updates['asks_to_updated'].iloc[i]:
			snapshot_copy = snapshot_copy.sort_values(by='price', ascending=True)
			snapshot_copy.reset_index(drop=True, inplace=True)
			if (np.float64(update[0]) < snapshot_copy['price'].iloc[0]) or (np.float64(update[0]) > snapshot_copy['price'].iloc[-1]):
				continue
			# get index in snapshot where price level on current update corresponds
			row_indices = snapshot_copy.index[snapshot_copy['price'] == np.float64(update[0])].tolist()  # uses boolean mask to iterate through all price levels in df_orderbook and match the price level from bids_to_updated

			# if update price isn't found in orderbook
			if len(row_indices) == 0:
				# Insert price level into snapshot_copy
				if np.float64(update[1]) == 0:
					continue
				new_price = {'price': np.float64(update[0]), 'bids': 0, 'asks': np.float64(update[1])}
				new_price_to_add = pd.DataFrame([new_price])
				new_price_to_add = new_price_to_add.apply(pd.to_numeric, errors='coerce')
				snapshot_copy = pd.concat([snapshot_copy, new_price_to_add], axis=0, join='outer', ignore_index=True).fillna(0)
				continue

			# Update the new ask
			for index in row_indices:
				if np.float64(update[1]) == 0:
					snapshot_copy = snapshot_copy.drop(index)
				else:
					snapshot_copy.loc[index, 'asks'] = np.float64(update[1])


	# Remove all empty price levels at the end
	snapshot_copy = snapshot_copy[((snapshot_copy['bids'] != 0) | (snapshot_copy['asks'] != 0))]
	snapshot_copy = snapshot_copy.sort_values(by='price', ascending=True)
	snapshot_copy.reset_index(drop=True, inplace=True)                # use drop to avoid adding the old index as an additional column
	#snapshot_copy = snapshot_copy[(snapshot_copy == 0).sum(1) < 2]


	# Pull best_prices from loop
	df_best_prices = pd.DataFrame.from_dict(best_dict, orient='index', columns=['best_bid_price', 'best_bid_vol', 'best_ask_price', 'best_ask_vol'])
	df_best_prices.reset_index(inplace=True)
	df_best_prices.rename(columns={'index': 'Timestamp'}, inplace=True)
	df_best_prices['mid_price'] = (df_best_prices['best_bid_price'] + df_best_prices['best_ask_price']) / 2

	return snapshot_copy, df_best_prices


def get_best_ask(df_order_book):
	df_order_book = df_order_book.apply(pd.to_numeric, errors='coerce')
	df_order_book.sort_values(by='price', inplace=True, ascending=True)
	df_order_book.reset_index(drop=True, inplace=True)
	best_ask_index = df_order_book['bids'].idxmin()
	best_ask_price = df_order_book.iloc[best_ask_index, 0]
	best_ask_vol = df_order_book.iloc[best_ask_index, 2]

	return best_ask_price, best_ask_vol


def get_best_bid(df_order_book):
	df_order_book = df_order_book.apply(pd.to_numeric, errors='coerce')
	df_order_book.sort_values(by='price', inplace=True, ascending=True)
	df_order_book.reset_index(drop=True, inplace=True)
	best_bid_index = df_order_book['bids'].idxmin() - 1
	best_bid_price = df_order_book.iloc[best_bid_index, 0]
	best_bid_vol = df_order_book.iloc[best_bid_index, 1]

	return best_bid_price, best_bid_vol


def add_e(df_bbo):
	
	e = [0] * len(df_bbo)

	for n in range(1, len(df_bbo)):
		e[n] = (df_bbo['best_bid_price'][n] >= df_bbo['best_bid_price'][n - 1]) * df_bbo['best_bid_vol'][n] - \
		       (df_bbo['best_bid_price'][n] <= df_bbo['best_bid_price'][n - 1]) * df_bbo['best_bid_vol'][n - 1] - \
		       (df_bbo['best_ask_price'][n] <= df_bbo['best_ask_price'][n - 1]) * df_bbo['best_ask_vol'][n] + \
		       (df_bbo['best_ask_price'][n] >= df_bbo['best_ask_price'][n - 1]) * df_bbo['best_ask_vol'][n - 1]

	df_bbo['e'] = e


def main():
	start_time = timeit.default_timer()
	df_snapshot_updated, df_best_prices = get_updated_order_book(df_updates_nested, df_snapshot_complete, (sample_time_s * 1000) + 1000)
	elapsed_time = timeit.default_timer() - start_time
	print("get_updated_order_book: ", elapsed_time, ' sec')

	start_time = timeit.default_timer()
	add_e(df_best_prices)
	elapsed_time = timeit.default_timer() - start_time
	print("add_e: ", elapsed_time, ' sec')

	df_snapshot_complete.to_csv(data_filepath + 'orderbook.csv')
	df_snapshot_updated.to_csv(data_filepath + 'orderbook_updated.csv')
	df_best_prices.to_csv(data_filepath + 'best_prices.csv')
	df_updates_complete.to_csv(data_filepath + 'updates.csv')

main()
