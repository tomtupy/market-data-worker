from config.constants import BROKER_TDA
from config.constants import BROKER_ALPACA
from lib.broker_api.broker_api import BrokerAPI
from lib.broker_api.td_ameritrade.tda_client import TDAClient
from lib.broker_api.alpaca.alpaca_client import AlpacaClient

class BrokerAPIFactory:
	@staticmethod
	def create(broker, params) -> BrokerAPI:
		if broker == BROKER_TDA:
			return TDAClient(params)
		elif broker == BROKER_ALPACA:
			return AlpacaClient(params)
		raise NotImplementedError(f"{broker} class is not implemented")
