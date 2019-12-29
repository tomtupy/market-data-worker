FROM python:3.7-slim-stretch

# Setup environment
RUN apt-get update -yqq \
    && apt-get install -yqq --no-install-recommends \
    	wget \
        python3-dev \
        gcc \
        libz-dev \
        curl \
        build-essential

# Build librdkafka
RUN curl -L https://github.com/edenhill/librdkafka/archive/v1.2.2.tar.gz | tar xzf -
WORKDIR /librdkafka-1.2.2
RUN ./configure --prefix=/usr
RUN make -j
RUN make install
WORKDIR /

RUN pip install pipenv

ADD Pipfile /Pipfile
RUN pipenv install
 
ADD config /config
ADD lib /lib

ADD kafka-config.yml /kafka-config.yml
ADD price_history.py /price_history.py
