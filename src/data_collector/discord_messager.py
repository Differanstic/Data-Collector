import requests

channel_id = {
    'bot': 1394777438516547634
}


def send_channel_message(message:str):
    return requests.post(
    "http://103.194.228.194:8000/send_channel",
    json={
        "channel_id": channel_id["bot"],
        "message": message
    }
    ).status_code