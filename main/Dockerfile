FROM python:3.10

WORKDIR /main

ADD . .

RUN pip install confluent-kafka

ENTRYPOINT [ "python", "main.py" ]