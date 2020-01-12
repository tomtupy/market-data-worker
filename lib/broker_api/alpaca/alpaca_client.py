import time
import logging
import requests
import json
from lib.broker_api.broker_api import BrokerAPI
import alpaca_trade_api as tradeapi
from datetime import datetime

class AlpacaClient(BrokerAPI):
    def __init__(self, params):
        # key and url are set through environment variables
        pass

    def get_price_history(self, symbol, start_date, end_date, minute_frequency):
        # not yet implemented
        candles = []
        return candles

    def get_market_hours(self, date):
        result = {"open": None, "close": None, "date": date, "is_open": False}
        api = tradeapi.REST(api_version='v2')

        # Check when the market was open
        calendar = api.get_calendar(start=date, end=date)
        
        for day in calendar:
            if datetime.fromisoformat(str(day.date)).date() == datetime.fromisoformat(date).date():
                result["open"] = day.open.strftime("%H:%M")
                result["close"] = day.close.strftime("%H:%M")
                result["is_open"] = True
                break
        return result
