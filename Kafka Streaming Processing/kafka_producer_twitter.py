"""API ACCESS KEYS"""

access_token = "QDk7eRBDILPTAULzdmV3bhn4v"
access_token_secret = "8HA2ORooDQ4yXKWPrvdXrhnl0Yrpfpe6aCdmIi11BHIRzl778U"
consumer_key = "404763090-S5WvEyRcZRafwCIGF3eS9pKv06iH963TZbtl5Sxp"
consumer_secret = "kIoHsNL81tW0nBEvKTSwxgSQumfbNaMAGvp9lhbgVxnHA"


# from tweepy.streaming import StreamListener
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='localhost:9092') #Same port as your Kafka server


topic_name = "YOUR KAFKA TOPIC"


class twitterAuth():
    """SET UP TWITTER AUTHENTICATION"""

    def authenticateTwitterApp(self):
        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)

        return auth



class TwitterStreamer():

    """SET UP STREAMER"""
    def __init__(self):
        self.twitterAuth = twitterAuth()

    def stream_tweets(self):
        while True:
            listener = ListenerTS() 
            auth = self.twitterAuth.authenticateTwitterApp()
            stream = Stream(auth, listener)
            stream.filter(track=["Apple"], stall_warnings=True, languages= ["en"])


class ListenerTS(tweepy.Stream):

    def on_data(self, raw_data):
            producer.send(topic_name, str.encode(raw_data))
            return True


if __name__ == "__main__":
    TS = TwitterStreamer()
    TS.stream_tweets()