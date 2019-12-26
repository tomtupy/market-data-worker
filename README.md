# market-data-collector
Collect stock and option data through a broker's API. This project is currently being used under an Airflow DockerOperator.

Supported brokers:
* TD Ameritrade

Supported data:
* per-minute price history

## Build
```
docker build -t market-data-collector -f Dockerfile.arm64 .
```

## Run
### Price History
```
docker run -t market-data-collector:latest pipenv run python price_history.py -a <api key> -n AAPL
```
