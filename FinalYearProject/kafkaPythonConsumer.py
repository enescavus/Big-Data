import threading
import logging
import time
import json
from kafka import KafkaConsumer, KafkaProducer

def consumer():
    consumer = KafkaConsumer(bootstrap_servers='localhost:9092',value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    consumer.subscribe(['topictweet'])

    for message in consumer:
        #this is simple line, data can be use for more manipulations
        print (message.value["text"])
consumer()