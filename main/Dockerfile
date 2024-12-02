FROM python:3.10-bullseye
ENV PYTHONUNBUFFERED=1

WORKDIR /main

ADD . /main/


RUN python -m pip install --upgrade pip

RUN pip install confluent-kafka==1.9.2

ENTRYPOINT [ "python", "main.py" ]