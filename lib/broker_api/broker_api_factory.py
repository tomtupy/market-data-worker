from config.constants import BROKER_TDA
from lib.broker_api.broker_api import BrokerAPI
from lib.broker_api.td_ameritrade.tda_client import TDAClient

class BrokerAPIFactory:
	@staticmethod
	def create(broker, params) -> BrokerAPI:
		if broker == BROKER_TDA:
			return TDAClient(params)
		raise NotImplementedError(f"{broker} class is not implemented")
