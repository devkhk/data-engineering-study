from twitter_api import (
    CONSUMER_KEY,
    CONSUMER_SECRET,
    ACCESS_TOKEN,
    ACCESS_TOKEN_SECRET,
    BEARER_TOKEN
) 

import json
import tweepy

class ProcessStream(tweepy.StreamingClient):
    def on_data(self, raw_data):
        print(raw_data)


twitter_stream = ProcessStream(bearer_token=BEARER_TOKEN)

# filter() 스트림을 연다는 뜻의 함수 
twitter_stream.filter()

