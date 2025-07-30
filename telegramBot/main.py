import functions_framework
import requests
import os
from google.cloud import storage
from google.cloud import firestore
from typing import Dict, Any
from datetime import datetime

# Initialize Firestore and Storage clients
db = firestore.Client()
storage_client = storage.Client()
bucket = storage_client.bucket('arems-user-upload')

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
        
        # Store user message in Firestore
        store_message(chat_id, text)
        
        # Handle file attachments if present
        if "document" in req_json["message"]:
            handle_document(req_json["message"]["document"], chat_id)
        elif "photo" in req_json["message"]:
            handle_photo(req_json["message"]["photo"], chat_id)

        send_message(chat_id, f"You said: {text}")

    return "OK", 200

def get_file_path(file_id):
    """Get file path from Telegram"""
    url = f"{TELEGRAM_API_URL}/getFile"
    response = requests.get(url, params={"file_id": file_id})
    if response.status_code == 200:
        return response.json()["result"]["file_path"]
    return None

def send_message(chat_id, text):
    url = f"{TELEGRAM_API_URL}/sendMessage"
    requests.post(url, json={"chat_id": chat_id, "text": text})

def handle_document(document, chat_id):
    """Handle document uploads"""
    file_id = document.get("file_id")
    file_name = document.get("file_name", "unnamed_file")
    
    # Get file path from Telegram
    file_path = get_file_path(file_id)
    if file_path:
        # Download and upload to Cloud Storage
        file_url = f"https://api.telegram.org/file/bot{TELEGRAM_TOKEN}/{file_path}"
        response = requests.get(file_url)
        
        blob = bucket.blob(f"{chat_id}/{file_name}")
        blob.upload_from_string(response.content)
        
        send_message(chat_id, "File received and stored successfully!")

def handle_photo(photos, chat_id):
    """Handle photo uploads"""
    # Get the largest photo version
    photo = photos[-1]
    file_id = photo.get("file_id")
    
    file_path = get_file_path(file_id)
    if file_path:
        file_url = f"https://api.telegram.org/file/bot{TELEGRAM_TOKEN}/{file_path}"
        response = requests.get(file_url)
        
        blob = bucket.blob(f"{chat_id}/photo_{file_id}.jpg")
        blob.upload_from_string(response.content)
        
        send_message(chat_id, "Photo received and stored successfully!")

def update_user_profile(chat_id: str, updates: Dict[Any, Any]) -> None:
    """
    Dynamically update user profile based on new data/use cases
    Args:
        chat_id: User's telegram chat ID
        updates: Dictionary of fields and values to update
    """
    user_ref = db.collection('arems-profiles').document('users').collection('profiles').document(str(chat_id))
    user_doc = user_ref.get()
    
    if not user_doc.exists:
        # New user - create profile with base fields
        base_profile = {
            'chat_id': chat_id,
            'first_interaction': firestore.SERVER_TIMESTAMP,
            'last_active': firestore.SERVER_TIMESTAMP,
            'total_messages': 1,
            'profile_created': datetime.now(),
            'profile_status': 'new'
        }
        # Merge any additional updates
        base_profile.update(updates)
        user_ref.set(base_profile)
    else:
        # Existing user - update only specified fields
        updates['last_active'] = firestore.SERVER_TIMESTAMP
        if 'total_messages' in updates:
            updates['total_messages'] = firestore.Increment(updates['total_messages'])
        user_ref.update(updates)

def store_message(chat_id, text):
    """Store message in Firestore"""
    # Store message in messages collection
    messages_ref = db.collection('arems-profiles').document('messages').collection(str(chat_id))
    messages_ref.add({
        'text': text,
        'timestamp': firestore.SERVER_TIMESTAMP,
        'type': 'user_message'
    })

    # Update user profile with message count
    update_user_profile(chat_id, {
        'total_messages': 1,
        'last_message': text
    })
