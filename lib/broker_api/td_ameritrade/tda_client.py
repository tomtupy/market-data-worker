import time
import logging
import requests
import json
from lib.broker_api.broker_api import BrokerAPI

class TDAClient(BrokerAPI):
    def __init__(self, params):
        if "api_key" not in params:
            raise KeyError("api_key must be provided for TDAClient")
        self.api_key = params["api_key"]

    def get_price_history(self, symbol, start_date, end_date, minute_frequency):
        s = time.perf_counter()
        query_url = f"https://api.tdameritrade.com/v1/marketdata/{symbol}/pricehistory?apikey={self.api_key}&periodType=day&frequencyType=minute&frequency={minute_frequency}&endDate={end_date}000&startDate={start_date}000"
        logging.info(query_url)
        r = requests.get(query_url)
        #print(r.text)
        candles = []
        for stock_record in r.json()['candles']:
            # sample stock_record
            # {'open': 223.44, 'high': 223.5, 'low': 223.4, 'close': 223.5, 'volume': 4867, 'datetime': 1570661040000}
            stock_record['stock'] = symbol.lower()
            stock_record['stimestamp'] = stock_record.pop('datetime')

            stock_record['volume'] = int(stock_record['volume'])
            stock_record['stimestamp'] = int(stock_record['stimestamp'])

            #print(stock_record)
            candles.append(stock_record)

        elapsed = time.perf_counter() - s
        logging.info(f"Got {len(candles)} price history data points for {symbol} at {minute_frequency} minute interval {__file__} in {elapsed:0.2f} seconds.")
        return candles

    def get_market_hours(self, date):
        query_url = f"https://api.tdameritrade.com/v1/marketdata/hours?apikey={self.api_key}&markets=EQUITY,OPTION,BOND&date={date}"
        logging.info(query_url)

        r = requests.get(query_url)
        return r.json()
