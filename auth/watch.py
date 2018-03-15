import requests
import os
import json

url = "https://www.googleapis.com/gmail/v1/users/me/watch"


def watch(token, topic):
    requests.post(url, params=dict(access_token=token),
                  data=json.dumps(dict(topicName=topic, labelIds=["INBOX"])),
                  headers={'Content-type': 'application/json'})


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--token", type=str, default="", help="token")
    parser.add_argument("--topic", type=str, default="projects/wix-wision/topics/oleksii-gmail", help="topic")
    args = parser.parse_args()

    watch(args.token, args.topic)
