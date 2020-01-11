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
    from time import sleep

    logging.basicConfig(level=logging.DEBUG)
    parser = OptionParser()
    #parser.add_option("-k", "--kafka-config", type="string", dest="kafka_config")
    parser.add_option("-a", "--api_key", type="string", dest="api_key")
    parser.add_option("-n", "--symbol", type="string", dest="symbol")
    parser.add_option("-s", "--start-timestamp", type="string", dest="start_timestamp")
    parser.add_option("-e", "--end-timestamp", type="string", dest="end_timestamp")
    parser.add_option("-f", "--min-frequency", type="string", dest="min_frequency", default=1)
    parser.add_option("-k", "--kafka-config", type="string", dest="kafka_config")
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
    #logging.info(price_data)


    # publish to kafka if config is specified
    if options.kafka_config is not None:
        from confluent_kafka import avro
        from confluent_kafka.avro import AvroProducer

        config = None
        with open(options.kafka_config) as f:
            try:
                config = yaml.safe_load(f)
            except yaml.YAMLError as exc:
                logging.error(exc)
                exit(1)

        value_schema = avro.loads(json.dumps(config['value-schema']))

        avroProducer = AvroProducer({
              'bootstrap.servers': f"{config['connection']['kafka-host']}:{config['connection']['kafka-port']}",
              'schema.registry.url': f"http://{config['connection']['schema-registry-host']}:{config['connection']['schema-registry-port']}"
              }, default_value_schema=value_schema)

        for stock_record in price_data:
            # sample stock_record
            # {'open': 223.44, 'high': 223.5, 'low': 223.4, 'close': 223.5, 'volume': 4867, 'datetime': 1570661040000}
            stock_record['stock'] = options.symbol.lower()
            stock_record['volume'] = int(stock_record['volume'])
            stock_record['stimestamp'] = int(stock_record['stimestamp'])

            logging.info(stock_record)

            avroProducer.produce(topic=config['topic'], value=stock_record)

            # debug - flish after every record
            avroProducer.flush()
            print("sent")
            sleep(1)

        avroProducer.flush()
