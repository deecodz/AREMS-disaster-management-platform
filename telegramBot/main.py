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

# Initialize Firestore and Storage clients
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
    raise

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"

@functions_framework.http
def main_handler(request):
    """Route between Telegram and Dialogflow CX webhooks"""
    
    req_json = request.get_json(silent=True)
    logger.info(f"=== INCOMING REQUEST ===")
    logger.info(f"Headers: {dict(request.headers)}")
    logger.info(f"Payload: {req_json}")
    
    # Check if this is a Dialogflow CX webhook
    if req_json and ('fulfillmentInfo' in req_json or 'sessionInfo' in req_json):
        logger.info("🤖 DIALOGFLOW CX REQUEST DETECTED")
        return handle_dialogflow_cx_webhook(request)
    
    # Otherwise handle as Telegram webhook
    logger.info("📱 TELEGRAM REQUEST DETECTED")
    return telegramWebhook(request)

def handle_dialogflow_cx_webhook(request):
    """Handle Dialogflow CX webhook requests"""
    
    try:
        req_json = request.get_json(silent=True)
        session_info = req_json.get("sessionInfo", {})
        fulfillment_info = req_json.get("fulfillmentInfo", {})
        webhook_tag = fulfillment_info.get("tag", "")
        
        logger.info(f"=== DIALOGFLOW CX WEBHOOK ===")
        logger.info(f"Webhook Tag: {webhook_tag}")
        logger.info(f"Session Info: {session_info}")
        logger.info(f"Parameters: {session_info.get('parameters', {})}")
        
        # Route based on webhook tag
        if webhook_tag == "emergency-submission":
            logger.info("🚨 Processing EMERGENCY REPORT")
            return handle_emergency_report(session_info)
            
        elif webhook_tag == "risk-assessment":
            logger.info("📊 Processing RISK ASSESSMENT")
            return handle_risk_assessment(session_info)
            
        else:
            logger.warning(f"Unknown webhook tag: {webhook_tag}")
            return {
                "fulfillmentResponse": {
                    "messages": [{"text": {"text": ["Request processed successfully."]}}]
                }
            }, 200
            
    except Exception as e:
        logger.error(f"Error in Dialogflow CX webhook: {str(e)}")
        return {
            "fulfillmentResponse": {
                "messages": [{"text": {"text": ["System error occurred. Please try again."]}}]
            }
        }, 500

def handle_emergency_report(session_info):
    """Handle emergency report webhook"""
    
    try:
        parameters = session_info.get('parameters', {})
        logger.info(f"🚨 EMERGENCY REPORT PARAMETERS: {parameters}")
        
        # Generate incident ID
        incident_id = f"INC-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        
        # Structure incident data
        incident_data = {
            'incident_id': incident_id,
            'incident_type': parameters.get('incident_type', ''),
            'location': parameters.get('location', ''),
            'severity_level': parameters.get('severity_level', ''),
            'contact_info': parameters.get('contact_info', ''),
            'timestamp': firestore.SERVER_TIMESTAMP,
            'source': 'dialogflow_cx'
        }
        
        logger.info(f"Saving emergency incident: {incident_data}")
        
        # Save to Firestore
        incident_ref = (db.collection('arems-profiles')
                       .document('emergency-reports')
                       .collection('incidents')
                       .document(incident_id))
        
        incident_ref.set(incident_data)
        logger.info(f"✅ SUCCESSFULLY SAVED EMERGENCY REPORT: {incident_id}")
        
        # Return response
        return {
            "fulfillmentResponse": {
                "messages": [{"text": {"text": [f"Emergency report saved with ID: {incident_id}"]}}]
            },
            "sessionInfo": {
                "parameters": {
                    "incident_id": incident_id
                }
            }
        }, 200
        
    except Exception as e:
        logger.error(f"❌ ERROR IN EMERGENCY REPORT: {str(e)}")
        return {
            "fulfillmentResponse": {
                "messages": [{"text": {"text": ["Error processing emergency report. Please try again."]}}]
            }
        }, 500

def handle_risk_assessment(session_info):
    """Handle risk assessment webhook - THIS WAS MISSING"""
    
    try:
        parameters = session_info.get('parameters', {})
        logger.info(f"📊 RISK ASSESSMENT PARAMETERS: {parameters}")
        
        # Generate assessment ID
        assessment_id = f"RISK-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        
        # Extract parameters
        hazard_type = parameters.get('hazard_type', '')
        affected_area = parameters.get('affected_area', '')
        population_at_risk = parameters.get('population_at_risk', '')
        
        # Calculate risk score and level
        risk_score = calculate_risk_score(hazard_type, population_at_risk)
        risk_level = get_risk_level(risk_score)
        
        # Structure assessment data
        assessment_data = {
            'assessment_id': assessment_id,
            'hazard_type': hazard_type,
            'affected_area': affected_area,
            'population_at_risk': population_at_risk,
            'risk_score': risk_score,
            'risk_level': risk_level,
            'timestamp': firestore.SERVER_TIMESTAMP,
            'source': 'dialogflow_cx'
        }
        
        logger.info(f"Saving risk assessment: {assessment_data}")
        logger.info(f"Risk calculation: Score={risk_score}, Level={risk_level}")
        
        # Save to Firestore
        assessment_ref = (db.collection('arems-profiles')
                         .document('risk-assessments')
                         .collection('assessments')
                         .document(assessment_id))
        
        assessment_ref.set(assessment_data)
        logger.info(f"✅ SUCCESSFULLY SAVED RISK ASSESSMENT: {assessment_id}")
        
        # THIS IS CRITICAL: Return risk_level parameter for routing
        return {
            "fulfillmentResponse": {
                "messages": [{"text": {"text": [f"Risk assessment completed. ID: {assessment_id}"]}}]
            },
            "sessionInfo": {
                "parameters": {
                    "assessment_id": assessment_id,
                    "risk_level": risk_level,  # ⭐ THIS IS WHAT YOUR ROUTES NEED
                    "risk_score": risk_score
                }
            }
        }, 200
        
    except Exception as e:
        logger.error(f"❌ ERROR IN RISK ASSESSMENT: {str(e)}")
        return {
            "fulfillmentResponse": {
                "messages": [{"text": {"text": ["Error processing risk assessment. Please try again."]}}]
            }
        }, 500

def calculate_risk_score(hazard_type, population_risk):
    """Calculate risk score based on hazard and population"""
    
    base_scores = {
        'natural_disaster': 40,
        'technological_hazard': 30,
        'biological_hazard': 35,
        'security_threat': 25
    }
    
    population_multipliers = {
        'vulnerable_groups': 1.5,
        'general_population': 1.0,
        'emergency_workers': 1.2,
        'tourists': 1.3
    }
    
    base_score = base_scores.get(hazard_type, 20)
    multiplier = population_multipliers.get(population_risk, 1.0)
    
    final_score = int(base_score * multiplier)
    logger.info(f"Risk calculation: {hazard_type} ({base_score}) × {population_risk} ({multiplier}) = {final_score}")
    
    return min(final_score, 100)

def get_risk_level(score):
    """Convert risk score to risk level"""
    if score >= 80:
        return 'CRITICAL'
    elif score >= 60:
        return 'HIGH'
    elif score >= 40:
        return 'MEDIUM'
    else:
        return 'LOW'

# Keep all your existing Telegram functions below this line...
def telegramWebhook(request):
    # Your existing telegram code stays the same
    logger.info(f"Received telegram webhook request: {request.method}")
    req_json = request.get_json(silent=True)
    
    if not req_json:
        logger.error("No JSON data in request")
        return "No data", 400
    
    # ... rest of your existing telegram code ...
    if "message" in req_json:
        chat_id = req_json["message"]["chat"]["id"]
        text = req_json["message"].get("text", "")
        username = req_json["message"]["from"].get("username", "unknown")
        
        update_user_profile(chat_id, {
            'username': username,
            'last_active': firestore.SERVER_TIMESTAMP
        })
        
        store_message(chat_id, text)
        
        if "document" in req_json["message"]:
            handle_document(req_json["message"]["document"], chat_id)
        elif "photo" in req_json["message"]:
            handle_photo(req_json["message"]["photo"], chat_id)
            
        send_message(chat_id, f"You said: {text}")
        
    return "OK", 200

# Keep all your other existing functions...
def get_file_path(file_id):
    url = f"{TELEGRAM_API_URL}/getFile"
    response = requests.get(url, params={"file_id": file_id})
    if response.status_code == 200:
        return response.json()["result"]["file_path"]
    return None

def send_message(chat_id, text):
    url = f"{TELEGRAM_API_URL}/sendMessage"
    requests.post(url, json={"chat_id": chat_id, "text": text})

def handle_document(document, chat_id):
    file_id = document.get("file_id")
    file_name = document.get("file_name", "unnamed_file")
    file_path = get_file_path(file_id)
    if file_path:
        file_url = f"https://api.telegram.org/file/bot{TELEGRAM_TOKEN}/{file_path}"
        response = requests.get(file_url)
        blob = bucket.blob(f"{chat_id}/{file_name}")
        blob.upload_from_string(response.content)
        send_message(chat_id, "File received and stored successfully!")

def handle_photo(photos, chat_id):
    try:
        user_ref = db.collection('arems-profiles').document('users').collection('profiles').document(str(chat_id))
        user_doc = user_ref.get()
        username = user_doc.get('username') if user_doc.exists else 'unknown'
        
        photo = photos[-1]
        file_id = photo.get("file_id")
        file_path = get_file_path(file_id)
        
        if file_path:
            file_url = f"https://api.telegram.org/file/bot{TELEGRAM_TOKEN}/{file_path}"
            response = requests.get(file_url)
            
            if response.status_code == 200:
                timestamp = datetime.now().strftime('%I-%M-%p')
                date = datetime.now().strftime('%B_%d_%Y')
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
    user_ref = db.collection('arems-profiles').document('users').collection('profiles').document(str(chat_id))
    user_doc = user_ref.get()
    
    if not user_doc.exists:
        base_profile = {
            'chat_id': chat_id,
            'first_interaction': firestore.SERVER_TIMESTAMP,
            'last_active': firestore.SERVER_TIMESTAMP,
            'total_messages': 1,
            'profile_created': datetime.now(),
            'profile_status': 'new'
        }
        base_profile.update(updates)
        user_ref.set(base_profile)
    else:
        updates['last_active'] = firestore.SERVER_TIMESTAMP
        if 'total_messages' in updates:
            updates['total_messages'] = firestore.Increment(updates['total_messages'])
        user_ref.update(updates)

def store_message(chat_id, text):
    try:
        user_ref = db.collection('arems-profiles').document('users').collection('profiles').document(str(chat_id))
        user_doc = user_ref.get()
        username = user_doc.get('username') if user_doc.exists else 'unknown'
        
        chat_id_with_username = f"{chat_id}_{username}"
        current_date = datetime.now()
        date_str = current_date.strftime('%B_%d_%Y')
        time_str = current_date.strftime('%I:%M %p')
        
        messages_ref = (db.collection('arems-profiles')
                       .document('messages')
                       .collection(chat_id_with_username)
                       .document(date_str)
                       .collection('daily_messages'))
        
        messages_ref.document(f"{time_str}_message").set({
            'text': text,
            'timestamp': firestore.SERVER_TIMESTAMP,
            'type': 'user_message',
            'username': username
        })
        
        daily_summary_ref = (db.collection('arems-profiles')
                           .document('messages')
                           .collection(chat_id_with_username)
                           .document(date_str))
        
        daily_summary_ref.set({
            'date': current_date,
            'message_count': firestore.Increment(1),
            'last_message_time': firestore.SERVER_TIMESTAMP,
            'username': username
        }, merge=True)
        
        update_user_profile(chat_id, {
            'total_messages': 1,
            'last_message': text,
            'last_message_time': f"{date_str} at {time_str}"
        })
        
    except Exception as e:
        logger.error(f"Error storing message: {str(e)}")
    
    return True
