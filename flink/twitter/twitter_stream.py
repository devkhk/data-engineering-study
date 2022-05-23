from twitter_api import (
    BEARER_TOKEN
) 

import json
import tweepy

class ProcessStream(tweepy.StreamingClient):
    def on_data(self, raw_data):
        json_data = json.loads(raw_data)
        
        if json_data["data"]["lang"] and json_data["data"]["lang"] == "ko":
            print(json_data)


def delete_all_rules(rules):
    # rules[0] : StreamRules Data
    if rules is None or rules[0] is None:
        return None
    # Stream Data : value, tag, id 순서
    ids = list(map(lambda rule: rule[2], rules[0]))
    client.delete_rules(ids=ids)

client = ProcessStream(BEARER_TOKEN) # 클라이언트 초기화
rules = client.get_rules()

# 기존 룰 삭제
delete_all_rules(rules)

# 새로운 룰 추가
client.add_rules(tweepy.StreamRule(value="Twitter"))
rules = client.get_rules()
print(rules)

# 스트림 실행
client.filter(tweet_fields=["lang"])
# client.filter(expansions=["author_id"], user_fields=["location"], tweet_fields=["lang"])
