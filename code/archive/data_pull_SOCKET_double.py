import pandas as pd
import websocket
import json
# -------------- USEFUL COMMANDS -------------- #
"""
| response.text  |  reads response's body (payload, usually json data) and returns as string 
| json.loads(json string from msg response OR response.text)  |  parses valid json string to a python dictionary (JSON format object). From json module im python's std library  
| response.json()  |  from requests library. Parses response content from the response object's body as JSON and returns a python datastructure (dict, list). Automatically handles content-type header
| pd.DataFrame.from_records(response)  |  Converts a sequence of records (dicts, tuples) such as a list of JSON objects to a pandas df object
| pd.json_normalize(dict/ list of dicts)  |  normalize a semi-structured JSON string (or dictionary) into a flat table
"""

# We can create one socket per symbol/ stream type
symbol1 = 'btcusdt'
symbol2 = 'ethbtc'
stream1 = '@trade'
stream2 = '@depth@100ms'
endpoint = 'wss://stream.binance.com:9443'          # can also use f'....' to use {symbol} in a dynamic f-string
raw_stream = '/ws/'
combined_stream = '/stream?streams='

# socket_url = endpoint + raw_stream + symbol1 + '@trade'
socket_url = endpoint + combined_stream + symbol1 + '/' + symbol2

# We are now making a connection, not just a single response. Need to handle messages, errors, returns when open/close, etc.
subscription_request = {
	'method': 'SUBSCRIBE',
	'params':
	[
		symbol1 + stream1,
		symbol1 + stream2
	],
	'id': 1
}

datastream1 = []
datastream2 = []
def on_message(ws, message):                        # invoked when message is received from the server. ws is WebSocketApp instance representing the connection. message is the data
	msg = json.loads(message)
	if msg['stream'] == symbol1 + stream1:
		datastream1.append(msg['data'])
	elif msg['stream'] == symbol1 + stream2:
		datastream2.append(msg['data'])
def on_error(ws, error): # invoked when an error occurs during the connection. error is the error message or exception object. Can define custom handling like logging the error, retrying the connection, etc.
	print(type(error))
def on_close(ws, close_status_code, close_msg):     # invoked when websocket connection is closed, either intentionally or due to error. statuscode indicates the reason for closure.
	df1 = pd.DataFrame.from_records(datastream1)
	df1.columns = ['event_type', 'event_time', 'symbol', 'trade_id', 'price', 'qty', 'buyer_order_id', 'seller_order_id', 'trade_time', 'is_buyer_mm', 'ignore']
	df1.drop(['ignore'], axis=1, inplace=True)

	df2 = pd.DataFrame.from_records(datastream2)
	df2.columns = ['event_type', 'event_time', 'symbol', 'first_update_id', 'final_update_id', 'bids_to_updated', 'asks_to_updated']

	df1.to_csv('trades.csv')
	df2.to_csv('depth_updates.csv')

	print("### Connection Closed ###")
def on_open(ws):                                    # invoked when connection is successfully opened and ready to send/ receive msges
	print("Opened connection, data streaming.")
	ws.send(json.dumps(subscription_request))       # json.dumps converts request dictionary to string


ws = websocket.WebSocketApp(socket_url, on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)
ws.run_forever()