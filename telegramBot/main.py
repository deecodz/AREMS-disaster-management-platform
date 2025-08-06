import functions_framework
import requests
import os
from google.cloud import storage
from google.cloud import firestore
from typing import Dict, Any
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Firestore and Storage clients with explicit project and database
try:
    db = firestore.Client(
        project='arems-project',
        database='arems-platform-core-db'
    )
    storage_client = storage.Client(project='arems-project')
    bucket = storage_client.bucket('arems-user-upload')
    logger.info("Successfully initialized Firebase and Storage clients")
except Exception as e:
    logger.error(f"Failed to initialize clients: {str(e)}")
    # Initialize with specified database as fallback
    try:
        db = firestore.Client(
            project='arems-project',
            database='arems-platform-core-db'
        )
        logger.info("Successfully initialized Firestore client with fallback")
    except Exception as e:
        logger.error(f"Failed to initialize Firestore client with fallback: {str(e)}")
        raise

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"

@functions_framework.http
def telegramWebhook(request):
    # Add request logging
    logger.info(f"Received webhook request: {request.method}")
    
    req_json = request.get_json(silent=True)
    if not req_json:
        logger.error("No JSON data in request")
        return "No data", 400

    logger.info(f"Webhook payload: {req_json}")  # Log the full payload

    if "message" in req_json:
        chat_id = req_json["message"]["chat"]["id"]
        text = req_json["message"].get("text", "")
        username = req_json["message"]["from"].get("username", "unknown")
        
        # Update user profile with username
        update_user_profile(chat_id, {
            'username': username,
            'last_active': firestore.SERVER_TIMESTAMP
        })
        
        logger.info(f"Processing message from chat_id: {chat_id}")
        
        # Store user message in Firestore
        store_message(chat_id, text)
        
        # Handle file attachments if present
        if "document" in req_json["message"]:
            logger.info("Processing document attachment")
            handle_document(req_json["message"]["document"], chat_id)
        elif "photo" in req_json["message"]:
            logger.info("Processing photo attachment")
            handle_photo(req_json["message"]["photo"], chat_id)

        send_message(chat_id, f"You said: {text}")
        logger.info("Message processing completed")

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
    try:
        # Get user data first to include username
        user_ref = db.collection('arems-profiles').document('users').collection('profiles').document(str(chat_id))
        user_doc = user_ref.get()
        username = user_doc.get('username') if user_doc.exists else 'unknown'

        # Get the largest photo version
        photo = photos[-1]
        file_id = photo.get("file_id")
        
        file_path = get_file_path(file_id)
        if file_path:
            file_url = f"https://api.telegram.org/file/bot{TELEGRAM_TOKEN}/{file_path}"
            response = requests.get(file_url)
            
            if response.status_code == 200:
                # Format timestamp for better readability
                timestamp = datetime.now().strftime('%I-%M-%p')  # e.g., 09-48-AM
                date = datetime.now().strftime('%B_%d_%Y')      # e.g., August_06_2025
                
                # Updated storage path to include username
                storage_path = f"users/{chat_id}_{username}/{date}/photos/photo_{timestamp}.jpg"
                blob = bucket.blob(storage_path)
                blob.upload_from_string(response.content)
                
                logger.info(f"Successfully uploaded photo from {username} ({chat_id})")
                send_message(chat_id, "Photo received and stored successfully!")
            else:
                logger.error(f"Failed to download photo: {response.status_code}")
                send_message(chat_id, "Sorry, couldn't process your photo. Please try again.")
    except Exception as e:
        logger.error(f"Error handling photo: {str(e)}")
        send_message(chat_id, "Sorry, there was an error processing your photo.")

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
    """Store message in Firestore with error handling"""
    try:
        # Get user data first
        user_ref = db.collection('arems-profiles').document('users').collection('profiles').document(str(chat_id))
        user_doc = user_ref.get()
        username = user_doc.get('username') if user_doc.exists else 'unknown'

        # Store message with timestamp as document ID
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        message_id = f"{timestamp}_{username}"
        
        # Define message storage path
        date_path = datetime.now().strftime('%B_%d_%Y')
        message_path = f"arems-profiles/messages/{chat_id}_{username}/{date_path}/daily_messages/{message_id}"
        
        # Store the message
        db.document(message_path).set({
            'text': text,
            'timestamp': firestore.SERVER_TIMESTAMP,
            'type': 'user_message',
            'username': username
        })
        logger.info(f"Successfully stored message for {username} ({chat_id})")

        # Update user profile
        update_user_profile(chat_id, {
            'total_messages': 1,
            'last_message': text
        })
    except Exception as e:
        logger.error(f"Error storing message: {str(e)}")
        return True
