from kafka import KafkaProducer
from datetime import datetime
import sys
import json
import re
import time
from random import randint
import unicodedata

# for real time streaming connection - use these codes below in the "git-streaming-code.py" file !

def streaming():
    print("Reading the data from datasource you select!")
    with open('yourRealTimeDataOrLocalDataSource.json') as json_file: 
        data = json.load(json_file)
    # use the same broker for consumer connection
    producer = KafkaProducer(bootstrap_servers='localhost:9092',
                                  value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    print("Your stream loop is started now!")
    while True:
        for item in data:
            print("tweet sent...")
            producer.send('topictweet', item)
            # put a timer if you need to see things in a slower mode
            time.sleep(5)
streaming()