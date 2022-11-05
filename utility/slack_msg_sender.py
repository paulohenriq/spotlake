import requests

url = ""


def send_slack_message(msg):

    slack_data = {
        "text": msg
    }
    requests.post(url, json=slack_data)
