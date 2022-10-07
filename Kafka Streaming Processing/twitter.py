import tweepy
import time
from confluent_kafka import Producer
from faker import Faker
import json
import time
import logging
import random



api_key ="QDk7eRBDILPTAULzdmV3bhn4v"
api_secret ="8HA2ORooDQ4yXKWPrvdXrhnl0Yrpfpe6aCdmIi11BHIRzl778U"
bearer_token =r"AAAAAAAAAAAAAAAAAAAAAJU%2FhwEAAAAA2PUqx9VDUVXp7uRL6g1OZ0RTGOQ%3Dg5riQ7zYERsWusY2BfFRMAxeI0oxQTTmNsJFoLX4og0ehLHcF0"
access_token ="404763090-S5WvEyRcZRafwCIGF3eS9pKv06iH963TZbtl5Sxp"
access_token_secret="kIoHsNL81tW0nBEvKTSwxgSQumfbNaMAGvp9lhbgVxnHA"

client = tweepy.Client(bearer_token, api_key, api_secret, access_token, access_token_secret)

auth = tweepy.OAuth1UserHandler(api_key, api_secret, access_token, access_token_secret)

api = tweepy.API(auth)

search_terms =["python", "programming", "coding"]

class MyStream(tweepy.StreamingClient):
    def on_connect(self):
        print('Connected')

    def on_tweet(self, tweet):
        if tweet.referenced_tweets == None:
            print(tweet.text)

            time.sleep(0.2)

stream = MyStream(bearer_token= bearer_token)

for term in search_terms:
    stream.add_rules(tweepy.StreamRule(term))

stream.filter(tweet_fields=["referenced_tweets"])