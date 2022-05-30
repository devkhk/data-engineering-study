import requests
import json
from slack_api import SLACK_BOT_TOKEN


def post_slack_message(channel, text):
    # 본인이 발급받은 토큰값으로 대체
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + SLACK_BOT_TOKEN
    }
    payload = {
        'channel': channel,
        'text': text
    }
    r = requests.post('https://slack.com/api/chat.postMessage',
                      headers=headers,
                      data=json.dumps(payload)
                      )

# if __name__ == '__main__':
#     #첫번째 param : 채널명 / 두번째 param : 메시지
#     post_slack_message("#이상거래-알림봇", "Hello World!")