FROM ubuntu:latest

WORKDIR /main

ADD . .


RUN apt-get update && \
  apt-get install -y --no-install-recommends gcc git libssl-dev g++ make && \
  cd /tmp && git clone https://github.com/edenhill/librdkafka.git && \
  cd librdkafka && git checkout tags/v1.9.0 && \
  ./configure && make && make install && \
  cd ../ && rm -rf librdkafka

RUN pip install confluent-kafka==1.9.2

ENTRYPOINT [ "python", "main.py" ]