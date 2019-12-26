from config.constants import BROKER_TDA
from lib.broker_api.broker_api import BrokerAPI
from lib.broker_api.broker_api_factory import BrokerAPIFactory

def get(broker_api_key, symbol, start_timestamp, end_timestamp, min_frequency)-> 'list':
    broker_api_client: BrokerAPI = BrokerAPIFactory.create(BROKER_TDA, {"api_key": broker_api_key}) 
    return broker_api_client.get_price_history(symbol, start_timestamp, end_timestamp, min_frequency)

if __name__ == "__main__":
    from optparse import OptionParser
    import yaml
    import json
    import logging
    from datetime import datetime

    logging.basicConfig(level=logging.DEBUG)
    parser = OptionParser()
    #parser.add_option("-k", "--kafka-config", type="string", dest="kafka_config")
    parser.add_option("-a", "--api_key", type="string", dest="api_key")
    parser.add_option("-n", "--symbol", type="string", dest="symbol")
    parser.add_option("-s", "--start-timestamp", type="string", dest="start_timestamp")
    parser.add_option("-e", "--end-timestamp", type="string", dest="end_timestamp")
    parser.add_option("-f", "--min-frequency", type="string", dest="min_frequency", default=1)
    (options, args) = parser.parse_args()

    # check required options
    if not options.api_key:
        parser.error('Broker API key not specified')
    if not options.symbol:
        parser.error('symbol not specified')

    # retrieve data
    price_data = get(broker_api_key=options.api_key,
                        symbol=options.symbol,
                        start_timestamp=options.start_timestamp,
                        end_timestamp=options.end_timestamp,
                        min_frequency=options.min_frequency)
    logging.info(price_data)
