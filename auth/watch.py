import requests
import os
import json

token = "ya29.GltyBVvfb7ORaXIVAKwyeaCgLnpVEYEbUV0b7JZPHnoiGnIDOFtqC7evfo2mmcaReTYGnT83-mUCJwufJt-GNp5iltyS9HwLBYer4QvNkPzomiONiK8mQlm1d1_e"
token = "ya29.Glx7BVnKNCRAySXwNByZhaiu-WCTiACsKaD44Wkt82H84iqM4Tj5w4ath12Km2LMpcU2OnYs1zDTaE3TNNYZ64Eh9IwWb7AgNl0NN6c0dqcgj5jbLFcEY-ARTfUhyg"
topic = 'projects/wix-wision/topics/oleksii-gmail'
url = "https://www.googleapis.com/gmail/v1/users/me/watch"

requests.post(url, params=dict(access_token=token), data=json.dumps(dict(topicName=topic, labelIds=["INBOX"])), headers={'Content-type': 'application/json'})
