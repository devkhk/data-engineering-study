from twitter_api import (
    CONSUMER_KEY,
    CONSUMER_SECRET,
    ACCESS_TOKEN,
    ACCESS_TOKEN_SECRET,
    BEARER_TOKEN
) 

import json
import tweepy

query = "covid has:images -is:retweet"

client = tweepy.Client(bearer_token=BEARER_TOKEN)
response = client.search_recent_tweets(query=query, max_results=10)

for tweet in response.data:
    print(tweet)