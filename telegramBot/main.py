import functions_framework
import requests
import os

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"

@functions_framework.http
def telegramWebhook(request):
    req_json = request.get_json(silent=True)
    if not req_json:
        return "No data", 400

    if "message" in req_json:
        chat_id = req_json["message"]["chat"]["id"]
        text = req_json["message"].get("text", "")

        send_message(chat_id, f"You said: {text}")

    return "OK", 200

def send_message(chat_id, text):
    url = f"{TELEGRAM_API_URL}/sendMessage"
    requests.post(url, json={"chat_id": chat_id, "text": text})
