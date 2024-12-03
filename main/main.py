import sys
import argparse
from random import choice
from confluent_kafka import Producer
from confluent_kafka import Consumer, KafkaException, KafkaError


producer_config = {
    'bootstrap.servers': 'kafka:29092',
    'client.id': 'python-producer'
}

consumer_config = {
    'bootstrap.servers': 'kafka:29092',  # Список серверов Kafka
    'group.id': 'mygroup',                  # Идентификатор группы потребителей
    'auto.offset.reset': 'earliest'       # Начальная точка чтения ('earliest' или 'latest')
    
}


def create_producer(config):
    producer = Producer(config)

    def delivery_report(err, msg):
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


    def send_message(topic, message):
        producer.produce(topic, value=message, callback=delivery_report)
        producer.flush()
    
    return send_message


def main_producer(config, topic, message):
    send_message = create_producer(config)
    send_message(topic, message)


    
def basic_consume_loop(consumer, topics):
    try:
        # подписываемся на топик
        consumer.subscribe(topics)
        
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    sys.stderr.write(f' {msg.topic()} [{msg.partition()}] reached end at offset {msg.offset()}\n' )
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                print(f"Received message: {msg.value().decode('utf-8')}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

    

def main_consumer(config, topics):
    consumer = Consumer(config)
    basic_consume_loop(consumer, topics)


if __name__ == '__main__':

    # produce --message 'Hello World!' --topic 'hello_topic' --kafka 'kafka:29092'
    # consume --topic 'hello_topic' --kafka 'kafka:29092'

    # Initialize parser
    parser = argparse.ArgumentParser()

    # Adding optional argument
    parser.add_argument('command', choices=['produce', 'consume'])
    parser.add_argument("--message")
    parser.add_argument("--topic")
    parser.add_argument("--kafka")


    # Read arguments from command line
    args = parser.parse_args()
    

    if (args.kafka):
        producer_config['bootstrap.servers'] = args.kafka
        consumer_config['bootstrap.servers'] = args.kafka

    if args.command == 'produce':
        main_producer(producer_config, args.topic, args.message)
    elif args.command == 'consume':
        main_consumer(consumer_config,[args.topic])
        # print(f'args.command: {args.command}, args.topic: {args.topic}')
