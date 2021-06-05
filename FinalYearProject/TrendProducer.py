###################################################################################################################
#  author : ENES ÇAVUŞ
#  subject : Getting Real Time trend data from Twitter via Tweepy - 
###################################################################################################################


from kafka import KafkaProducer
from datetime import datetime
import sys
import json
import re
import time
from random import randint
import unicodedata
import tweepy
import csv
from tweepy import Stream, OAuthHandler
from tweepy.streaming import StreamListener
import datetime
now = datetime.datetime.now()
print(now.strftime("%A"))
today = now.strftime("%A")
import os
import geocoder
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer


# kafka producer on port 9092 and encoding the data for better data transfer
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                                  value_serializer=lambda v: json.dumps(v).encode('utf-8'))

#REAL TIME ------------------------ 
# your API keys and tokens 
# consumer_key="#"
# consumer_secret="#"
# access_token="#"
# access_token_secret="#"

# tweepy authentication
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)
trends_available = api.trends_available()

for i in range(2):
    # this condition gets global trend data and sending usable datas via kafka producer on topic for global transfer
    if i == 0:
        print("\n\n GLOBAL \n\n")
        trends = api.trends_place(1) # for global 
        globalTrends = []
        for trend in trends[0]['trends']: 
            if trend['tweet_volume'] is not None and trend['tweet_volume'] > 10000: 
                globalTrends.append((trend['name'], trend['tweet_volume']))

        globalTrends.sort(key=lambda x:-x[1])
        for  i in range(10):
                producer.send('trends1',[globalTrends[i][0],globalTrends[i][1],])
                print("trend global sent!", i)
                time.sleep(0.5)
    # this condition gets local trend data and sending usable datas via kafka producer on topic for local transfer
    elif i == 1:
        print("\n\n LOCAL \n\n")
        loc = "England"     
        g = geocoder.osm(loc)           # for local
        closest_loc = api.trends_closest(g.lat, g.lng)
        trends = api.trends_place(closest_loc[0]['woeid'])
        localTrends = []
        for trend in trends[0]['trends']: 
            if trend['tweet_volume'] is not None and trend['tweet_volume'] > 10000: 
                localTrends.append((trend['name'], trend['tweet_volume']))

        for  i in range(10):
            producer.send('trends2',[localTrends[i][0],localTrends[i][1],])
            print("trend local sent!", i)
            time.sleep(0.5)

# NOT real Time - from local - only for test
# trends = [["deneme1",123],["deneme2",1232],["deneme3",1213],["deneme4",1233],["deneme5",4123]]
# trendsLocal = [["local1",123],["local1",1232],["local1",1213],["local1",1233],["local1",4123]]
# for i in range(5):
#     producer.send('trends1',[trends[i][0],trends[i][1]])
#     print("trend global sent!")
#     producer.send('trends2',[trendsLocal[i][0],trendsLocal[i][1]])
#     print("trend local sent!")
#     time.sleep(1)

print("Exiting")
sys.exit()

# END - ENES ÇAVUŞ - Btirme Projesi - SAU - Bahar 2021
