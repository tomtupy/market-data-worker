import boto3
import os, sys, errno
import json
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from optparse import OptionParser
import yaml
import logging
from datetime import datetime, date, timedelta, time
from time import sleep
from config.constants import BROKER_TDA
from config.constants import BROKER_ALPACA
from lib.broker_api.broker_api import BrokerAPI
from lib.broker_api.broker_api_factory import BrokerAPIFactory
import requests

# This service is mainly used to simplify access
# to market data stored across various datastores

logging.basicConfig(level = logging.INFO)

# TODO move to config server
BROKER_API_KEY = os.getenv('BROKER_API_KEY')
AWS_REGION = os.getenv('AWS_REGION')
AWS_KEY = os.getenv('AWS_KEY')
AWS_SECRET = os.getenv('AWS_SECRET')
PRICE_HISTORY_QUEUE_NAME = "price_history_requests"
KAFKA_CONFIG = os.getenv('KAFKA_CONFIG')
INFLUXDB_URL = os.getenv('INFLUXDB_URL')
INFLUXDB_DBNAME = "stock_price_data"

SQS_DEFAULT_VISIBILITY_TIMEOUT = 300
SQS_DEFAULT_RECEIVE_WAIT_TIME = 20
PRICE_HISTORY_QUEUE_URL = ""

def reset_msg_visibility_timeout(last_reset, handle, sqs_client):
    time_since_reset = (datetime.now()-last_reset).total_seconds()
    if (SQS_DEFAULT_VISIBILITY_TIMEOUT * 0.5 < time_since_reset):
        logging.info(f"Visibility timeout reset is necessary: {time_since_reset}")
        sqs_client.change_message_visibility(
            QueueUrl=PRICE_HISTORY_QUEUE_URL,
            ReceiptHandle=handle,
            VisibilityTimeout=SQS_DEFAULT_VISIBILITY_TIMEOUT
        )
        return datetime.now()
    return last_reset

if __name__ == '__main__':
    # Check environment
    env_list = [BROKER_API_KEY, AWS_REGION, AWS_KEY, AWS_SECRET, KAFKA_CONFIG, INFLUXDB_URL]
    if None in env_list or '' in env_list:
        sys.exit(errno.EINVAL)
 
    # Set up kafka
    config = None
    with open(KAFKA_CONFIG) as f:
        try:
            config = yaml.safe_load(f)
        except yaml.YAMLError as exc:
            logging.error(exc)
            exit(errno.EIO)
    value_schema = avro.loads(json.dumps(config['value-schema']))

    avroProducer = AvroProducer({
          'bootstrap.servers': f"{config['connection']['kafka-host']}:{config['connection']['kafka-port']}",
          'schema.registry.url': f"http://{config['connection']['schema-registry-host']}:{config['connection']['schema-registry-port']}"
          }, default_value_schema=value_schema)


    # Test TDA Broker connection
    broker_api_client_tda: BrokerAPI = BrokerAPIFactory.create(BROKER_TDA, {"api_key": BROKER_API_KEY}) 
    market_hours = broker_api_client_tda.get_market_hours(datetime.now().strftime("%Y-%m-%d"))
    if 'error' in market_hours:
        logging.error("Error while testing TDA broker connection")
        exit(errno.EIO)

    logging.info(market_hours)

    # Test Alpaca Broker connection
    broker_api_client_alpaca: BrokerAPI = BrokerAPIFactory.create(BROKER_ALPACA, {}) 
    market_hours = broker_api_client_alpaca.get_market_hours(datetime.now().strftime("%Y-%m-%d"))
    if 'error' in market_hours:
        logging.error("Error while testing Alpaca broker connection")
        exit(errno.EIO)

    logging.info(market_hours)

    # Test InfluxDB connection
    influxdb_tables = []
    influxdb_query_url = f"{INFLUXDB_URL}/query?pretty=false"
    logging.info(influxdb_query_url)
    query_data = {"db": INFLUXDB_DBNAME,
                    "q": "show measurements;"}
    r = requests.get(influxdb_query_url, params=query_data).json()['results']
    logging.info(r)
    if len(r) <= 0:
        logging.error("Error while testing InfluxDB connection")
        exit(errno.EIO)
    influxdb_tables = list(map(lambda x: x[0], r[0]['series'][0]['values']))
    logging.info(f"InfluxDB tables: {influxdb_tables}")

    # Connect to SQS
    aws_sqs_client = boto3.client('sqs', 
                                aws_access_key_id=AWS_KEY, 
                                aws_secret_access_key=AWS_SECRET, 
                                region_name=AWS_REGION)
    PRICE_HISTORY_QUEUE_URL = aws_sqs_client.get_queue_url(QueueName=PRICE_HISTORY_QUEUE_NAME)['QueueUrl']


    process_messages = True
    while process_messages == True:
        # Long poll for message on provided SQS queue
        messages = aws_sqs_client.receive_message(
            QueueUrl=PRICE_HISTORY_QUEUE_URL,
            MaxNumberOfMessages=1,
            MessageAttributeNames=[
                'All'
            ],
            WaitTimeSeconds=SQS_DEFAULT_RECEIVE_WAIT_TIME,
            VisibilityTimeout=SQS_DEFAULT_VISIBILITY_TIMEOUT
        )

        if len(messages) == 0:
            logging.info(f"No messages on the queue!")
            continue

        message_body = json.loads(messages['Messages'][0]['Body'])
        message_handle = messages['Messages'][0]['ReceiptHandle']
        logging.info(f"Received message: {message_handle} {message_body}")
        symbol = message_body["symbol"]
        msg_visibility_last_reset = datetime.now()

        day_count = 0
        data_available = True
        ok_to_delete_message = True
        current_day = date.today()

        # iterate backwards from today
        while data_available:
            logging.info(f"Processing {symbol} {current_day.strftime('%Y-%m-%d')}")

            # Check visibility timeout
            msg_visibility_last_reset = reset_msg_visibility_timeout(msg_visibility_last_reset, message_handle, aws_sqs_client)

            try:
                # check if market is open on this day
                market_hours = broker_api_client_alpaca.get_market_hours(current_day.strftime('%Y-%m-%d'))
                logging.info(f"Market Hours: {market_hours}")

                # Check visibility timeout reset after API call
                msg_visibility_last_reset = reset_msg_visibility_timeout(msg_visibility_last_reset, message_handle, aws_sqs_client)

                if market_hours["is_open"] == True:
                    # Trading Day in UTC 14:30 - 21:00
                    # Get UTC timestamps for InfluxDB query
                    day_open = time.fromisoformat(market_hours["open"])
                    day_close = time.fromisoformat(market_hours["close"])
                    trading_day_start_ts = int(datetime(current_day.year, current_day.month, current_day.day, day_open.hour + 5, day_open.minute, 0).timestamp())
                    trading_day_end_ts = int(datetime(current_day.year, current_day.month, current_day.day, day_close.hour + 5, day_close.minute, 0).timestamp())

                    # Query InfluxDB for datapoint count
                    trading_day_minutes = int((trading_day_end_ts - trading_day_start_ts) / 60.0)
                    logging.info(f"trading_day_minutes {trading_day_minutes}")
                    query_data = {"db": INFLUXDB_DBNAME,
                                "q": f"select count(close) from \"{symbol}\" where time >= {trading_day_start_ts}000000000 and time <= {trading_day_end_ts}000000000;"}
                    logging.info(f"InfluxDB Query: {query_data}")
                    db_res = requests.get(influxdb_query_url, params=query_data).json()['results'][0]
                    logging.info(f"InfluxDB Query Results: {db_res}")

                    # Check visibility timeout reset after API call
                    msg_visibility_last_reset = reset_msg_visibility_timeout(msg_visibility_last_reset, message_handle, aws_sqs_client)

                    data_point_count = None
                    if 'series' in db_res and len(db_res['series']) > 0 and 'values' in db_res['series'][0]:
                        data_point_count = db_res['series'][0]['values'][0][1]
                    logging.info(f"data_point_count: {data_point_count}")

                    data_download_required = False
                    logging.info(f"test: {0.9 * trading_day_minutes} < {data_point_count}")
                    if data_point_count is not None and (0.9 * trading_day_minutes < data_point_count):
                        logging.info("Datapoint count satisfactory")
                    else:
                        logging.info("Not enough datapoints available")
                        data_download_required = True
                        
                    if data_download_required == True:
                        query_start_ts = int(datetime(current_day.year, current_day.month, current_day.day, 0, 0, 0).timestamp())
                        query_end_ts = int(datetime(current_day.year, current_day.month, current_day.day, 23, 55, 0).timestamp())

                        logging.info(f"Download data for {current_day.strftime('%Y-%m-%d')} {query_start_ts} {query_end_ts}")

                        candles = broker_api_client_tda.get_price_history(symbol, query_start_ts, query_end_ts, 1)
                        
                        # Check visibility timeout reset after API call
                        msg_visibility_last_reset = reset_msg_visibility_timeout(msg_visibility_last_reset, message_handle, aws_sqs_client)
                        if len(candles) > 0:
                            logging.info(candles[0])
                            for stock_record in candles:
                                # sample stock_record
                                # {'open': 223.44, 'high': 223.5, 'low': 223.4, 'close': 223.5, 'volume': 4867, 'datetime': 1570661040000}
                                stock_record['stock'] = symbol.lower()
                                stock_record['volume'] = int(stock_record['volume'])
                                stock_record['stimestamp'] = int(stock_record['stimestamp'])

                                logging.info(stock_record)

                                avroProducer.produce(topic=config['topic'], value=stock_record)

                                msg_visibility_last_reset = reset_msg_visibility_timeout(msg_visibility_last_reset, message_handle, aws_sqs_client)

                                # debug - flush after every record
                                #avroProducer.flush()
                                #print("sent")
                                #sleep(1)
                            avroProducer.flush()
                        else:
                            logging.info("No more data available!")
                            data_available = False

                current_day = current_day - timedelta(days=1)
                day_count += 1

            except Exception as error:
                ok_to_delete_message = False
                data_available = False
                logging.info(f"Message processing error {str(error)}")

        if ok_to_delete_message == True:
            logging.info(f"Deleting message: {message_handle}")
            aws_sqs_client.delete_message(
                QueueUrl=PRICE_HISTORY_QUEUE_URL,
                ReceiptHandle=message_handle
            )
        