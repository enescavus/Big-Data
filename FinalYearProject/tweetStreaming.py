import tweepy
import csv
from tweepy import Stream, OAuthHandler
from tweepy.streaming import StreamListener
import time
timestr = time.strftime("%Y.%m.%d-%H:%M:%S") # to name the json file saved
# print(timestr)


# consumer_key=""
# consumer_secret=""
# access_token=""
# access_token_secret=""

# filename = 'twitter_data_analysis.csv'
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

class MyStreamListener(tweepy.StreamListener):
    def __init__(self, time_limit=5,num_tweets=0):
        self.start_time = time.time()
        self.limit = time_limit
        self.num_tweets = num_tweets
        self.saveFile = open(timestr +'.json', 'a')
        self.saveFile.write('[\n\n')
        super(MyStreamListener, self).__init__()

    def on_data(self, data):
        #print(data.text + "    " + data.location)
        # if (time.time() - self.start_time) < self.limit:
        if self.num_tweets < 250:
            if(self.num_tweets != 0):
                self.saveFile.write(',')
            self.saveFile.write(data)

            self.num_tweets += 1
            print("this is tweet {} text:   ".format(int(self.num_tweets)), type(data))
            return True
        else:
            print("reach the limit - closing the file")
            self.saveFile.write('\n]')
            self.saveFile.close()
            print("file closed - stopping the stream!!!")
            return False

    # def on_status(self, status):
    #     print(status.text)

    def on_error(self, status_code):
        if (time.time() - self.start_time) >= self.limit:
            print ('time is over')
            return False
        elif status_code == 420:
            print('Got an error with status code: ' + str(status_code))
            #returning False in on_error disconnects the stream
            return False
        else:
            print("This is error status code : " , str(status_code))
            return False
# Start Streming
myStream = tweepy.Stream(auth=api.auth, listener=MyStreamListener(num_tweets=0,time_limit=5))
myStream.filter(track=['covid19'])


