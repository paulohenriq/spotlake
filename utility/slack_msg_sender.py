import requests
import inspect

url = "https://hooks.slack.com/services/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"


def send_slack_message(msg):
    module_name = inspect.stack()[1][1]
    line_no = inspect.stack()[1][2]
    function_name = inspect.stack()[1][3]

    message = f"File \"{module_name}\", line {line_no}, in {function_name} :\n{msg}"

    slack_data = {
        "text": message
    }
    requests.post(url, json=slack_data)
