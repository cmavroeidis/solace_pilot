import requests
import json
import pandas as pd
import time
import os

def fetch_order_book(symbol):
	# setx BINANCE_API_KEY 'keycode' in cmd for virtual env
	api_key = 'yrvi5vopzCZOmhfdIVD2Fbsn3jsnqXWwXHrr3XKVSsiSwtQPX3RpUtoxj4dWdJj4'
	api_secret = 'eRNU7akEhgsNJY0ikRamDL7pLqQxbqI2o3tUbpMmd3ll8KXguq6kAy0Jt1tIkx01'

	url = 'https://api1.binance.com'
	api_call = '/api/v3/depth'
	endpoint = url + api_call

	headers = {'content-type': 'application/json', 'X-MBX-APIKEY': api_key}
	params = {
	    'symbol': symbol,
	}

	response = requests.get(endpoint, headers=headers, params=params)

	if response.status_code == 200:             # successful request, data accessed
		return response.json()

	else:
		print("Error fetching order book data, see error code: ", response.status_code)
		return None

def fetch_server_time():
	api_key = 'yrvi5vopzCZOmhfdIVD2Fbsn3jsnqXWwXHrr3XKVSsiSwtQPX3RpUtoxj4dWdJj4'
	api_secret = 'eRNU7akEhgsNJY0ikRamDL7pLqQxbqI2o3tUbpMmd3ll8KXguq6kAy0Jt1tIkx01'

	url = 'https://api1.binance.com'
	api_call = '/api/v3/time'
	endpoint = url + api_call

	headers = {'content-type': 'application/json', 'X-MBX-APIKEY': api_key}
	response = requests.get(endpoint, headers=headers)

	timestamp = response.json()["serverTime"]                  # access value of key 'serverTime' from response dictionary. can also use my_dict.get("key", "value if key doesnt exist")
	return timestamp


def build_order_book(symbol):
	interval = 2
	sec_counter = 0
	order_book_list = []

	while sec_counter < 10:                           # 30 loops/min, 60min, 10h
		timestamp = fetch_server_time()
		response = fetch_order_book(symbol)
		if response :                                           # always true if there is a response from server
			order_book_list.append({'timestamp': timestamp, 'data': response})

		time.sleep(interval)
		sec_counter = sec_counter + interval

	df_order_book_nested = pd.DataFrame(order_book_list)
	#df_order_book_nested.set_index('timestamp', inplace=True)

	# ------------ Unwrapping the orderbook --------------- #
	data = pd.json_normalize(df_order_book_nested['data'])
	df_order_book = pd.concat([df_order_book_nested['timestamp'], data], axis=1)

	file_name = 'ob_' + symbol + '_' + str('{:.2f}'.format(sec_counter/3600)) + 'h_' + str(interval) + 's.json'
	df_order_book.to_json(file_name)
	return df_order_book_nested


# ------------------------ request limits ------------------------- #
# response.headers.get('X-MBX-USED-WEIGHT'))                        Checks weight of total requests
def get_request_weight():

	api_key = 'yrvi5vopzCZOmhfdIVD2Fbsn3jsnqXWwXHrr3XKVSsiSwtQPX3RpUtoxj4dWdJj4'
	api_secret = 'eRNU7akEhgsNJY0ikRamDL7pLqQxbqI2o3tUbpMmd3ll8KXguq6kAy0Jt1tIkx01'

	url = 'https://api1.binance.com'
	api_call = '/api/v3/exchangeInfo'
	headers = {'content-type': 'application/json', 'X-MBX-APIKEY': api_key}

	endpoint = url+api_call
	response = requests.get(endpoint, headers=headers)

	print("Weight of requests: ", response.headers.get('X-MBX-USED-WEIGHT'))

start_time = time.time()
build_order_book('ETHUSDT')
get_request_weight()
end_time = time.time()
print('Start: ', start_time)
print('End: ',end_time)
print("Time elapsed: {:.2f} seconds".format(end_time - start_time))