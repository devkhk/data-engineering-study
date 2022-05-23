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

def delete_all_rules(rules):
    # rules[0] # StreamRules Data
    # Stream Data : value, tag, id 순
    ids = list(map(lambda rule: rule[2], rules[0]))
    client.delete_rules(ids=ids)
    # print("clear")


client = ProcessStream(BEARER_TOKEN)
rules = client.get_rules()
delete_all_rules(rules)


# {"value": "BTS has:images -is:retweet", "tag": "BTS"},

# stream.add_rules(tweepy.StreamRule("Twitter"))
# stream.add_rules(tweepy.StreamRule(value="BTS has:images -is:retweet", tag="BTS"))
# stream.filter()

