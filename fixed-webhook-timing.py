import functions_framework
import requests
import os
from google.cloud import storage
from google.cloud import firestore
from typing import Dict, Any
from datetime import datetime
import logging
import sys

# Setup enhanced logging for Cloud Functions
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

# Add immediate logging to verify deployment
print("🚀 FULL-FEATURED AREMS SYSTEM STARTING - Fixed webhook timing version!")
logger.info("🚀 FULL-FEATURED AREMS SYSTEM STARTING - Fixed webhook timing version!")

# Initialize Firestore and Storage clients
try:
    db = firestore.Client(
        project='arems-project',
        database='arems-platform-core-db'
    )
    storage_client = storage.Client(project='arems-project')
    bucket = storage_client.bucket('arems-user-upload')
    print("✅ Successfully initialized Firebase and Storage clients")
    logger.info("✅ Successfully initialized Firebase and Storage clients")
except Exception as e:
    print(f"❌ Failed to initialize clients: {str(e)}")
    logger.error(f"❌ Failed to initialize clients: {str(e)}")
    raise

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"

@functions_framework.http
def telegramWebhook(request):
    """CORRECTED: Main webhook handler - routes between Telegram and Dialogflow CX"""
    
    print("📥 NEW REQUEST RECEIVED!")
    logger.info("📥 NEW REQUEST RECEIVED!")
    
    try:
        req_json = request.get_json(silent=True)
        
        # Enhanced logging for debugging
        print(f"=== INCOMING REQUEST DEBUG ===")
        print(f"Request Method: {request.method}")
        print(f"User-Agent: {request.headers.get('User-Agent', 'No User-Agent')}")
        print(f"Content-Type: {request.content_type}")
        print(f"Parsed JSON payload: {req_json}")
        
        logger.info(f"=== INCOMING REQUEST DEBUG ===")
        logger.info(f"Request Method: {request.method}")
        logger.info(f"User-Agent: {request.headers.get('User-Agent', 'No User-Agent')}")
        logger.info(f"Content-Type: {request.content_type}")
        logger.info(f"Parsed JSON payload: {req_json}")
        
        # Enhanced Dialogflow CX detection
        is_dialogflow_cx = False
        user_agent = request.headers.get('User-Agent', '')
        
        print(f"🔍 Analyzing request type...")
        logger.info(f"🔍 Analyzing request type...")
        
        # Check for Dialogflow CX specific fields
        if req_json:
            cx_indicators = ['fulfillmentInfo', 'sessionInfo', 'pageInfo', 'intentInfo']
            found_indicators = [field for field in cx_indicators if field in req_json]
            
            print(f"Found Dialogflow CX indicators: {found_indicators}")
            logger.info(f"Found Dialogflow CX indicators: {found_indicators}")
            
            if found_indicators:
                is_dialogflow_cx = True
                print("🤖 DIALOGFLOW CX REQUEST DETECTED - Routing to CX handler")
                logger.info("🤖 DIALOGFLOW CX REQUEST DETECTED - Routing to CX handler")
                return handle_dialogflow_cx_webhook(request)
        
        # Check User-Agent as backup
        if 'Google-Dialogflow' in user_agent:
            print("🤖 DIALOGFLOW CX DETECTED BY USER-AGENT - Routing to CX handler")
            logger.info("🤖 DIALOGFLOW CX DETECTED BY USER-AGENT - Routing to CX handler")
            is_dialogflow_cx = True
            return handle_dialogflow_cx_webhook(request)
            
        if not is_dialogflow_cx:
            print("📱 TELEGRAM REQUEST DETECTED - Routing to Telegram handler")
            logger.info("📱 TELEGRAM REQUEST DETECTED - Routing to Telegram handler")
            return handle_telegram_webhook(request)
            
    except Exception as e:
        error_msg = f"❌ CRITICAL ERROR in main webhook handler: {str(e)}"
        print(error_msg)
        logger.error(error_msg)
        import traceback
        traceback_msg = f"Full traceback: {traceback.format_exc()}"
        print(traceback_msg)
        logger.error(traceback_msg)
        
        # Return appropriate response format based on request type
        user_agent = request.headers.get('User-Agent', '')
        if 'Google-Dialogflow' in user_agent:
            return {
                "fulfillmentResponse": {
                    "messages": [{"text": {"text": ["System error occurred. Please try again."]}}]
                }
            }
        else:
            return {"status": "error", "message": "Internal server error"}, 500

def handle_dialogflow_cx_webhook(request):
    """Handle Dialogflow CX webhook requests with enhanced debugging"""
    
    print("=== ENTERING DIALOGFLOW CX HANDLER ===")
    logger.info("=== ENTERING DIALOGFLOW CX HANDLER ===")
    
    try:
        req_json = request.get_json(silent=True)
        
        print(f"📝 Processing Dialogflow CX request: {req_json}")
        logger.info(f"📝 Processing Dialogflow CX request: {req_json}")
        
        session_info = req_json.get("sessionInfo", {})
        fulfillment_info = req_json.get("fulfillmentInfo", {})
        page_info = req_json.get("pageInfo", {})  # ⭐ CRITICAL: Add page info
        webhook_tag = fulfillment_info.get("tag", "")
        
        print(f"🏷️ Webhook Tag: '{webhook_tag}'")
        print(f"📋 Session Info: {session_info}")
        print(f"🔧 Fulfillment Info: {fulfillment_info}")
        print(f"📄 Page Info: {page_info}")  # ⭐ Log page info
        print(f"⚙️ Parameters: {session_info.get('parameters', {})}")
        
        logger.info(f"🏷️ Webhook Tag: '{webhook_tag}'")
        logger.info(f"📋 Session Info: {session_info}")
        logger.info(f"🔧 Fulfillment Info: {fulfillment_info}")
        logger.info(f"📄 Page Info: {page_info}")  # ⭐ Log page info
        logger.info(f"⚙️ Parameters: {session_info.get('parameters', {})}")

        # Route based on webhook tag with FORM COMPLETION CHECK
        if webhook_tag == "emergency-submission":
            print("🚨 Routing to EMERGENCY REPORT handler")
            logger.info("🚨 Routing to EMERGENCY REPORT handler")
            return handle_emergency_report(session_info, page_info, req_json)  # ⭐ Pass all data
        elif webhook_tag == "risk-assessment":
            print("📊 Routing to RISK ASSESSMENT handler")
            logger.info("📊 Routing to RISK ASSESSMENT handler")  
            return handle_risk_assessment(session_info, page_info, req_json)  # ⭐ Pass all data
        else:
            warning_msg = f"⚠️ Unknown webhook tag: '{webhook_tag}' - Expected 'emergency-submission' or 'risk-assessment'"
            print(warning_msg)
            logger.warning(warning_msg)
            
            print("Returning generic success response")
            logger.info("Returning generic success response")
            return {
                "fulfillmentResponse": {
                    "messages": [{"text": {"text": ["Request processed successfully."]}}]
                }
            }
            
    except Exception as e:
        error_msg = f"❌ ERROR in Dialogflow CX webhook: {str(e)}"
        print(error_msg)
        logger.error(error_msg)
        import traceback
        traceback_msg = f"Full traceback: {traceback.format_exc()}"
        print(traceback_msg)
        logger.error(traceback_msg)
        return {
            "fulfillmentResponse": {
                "messages": [{"text": {"text": ["System error occurred. Please try again."]}}]
            }
        }

def handle_emergency_report(session_info, page_info, full_request):
    """Handle emergency report webhook - ONLY save when form is complete"""
    
    print("🚨 === PROCESSING EMERGENCY REPORT ===")
    logger.info("🚨 === PROCESSING EMERGENCY REPORT ===")
    
    try:
        parameters = session_info.get('parameters', {})
        
        print(f"📝 Emergency report parameters received: {parameters}")
        logger.info(f"📝 Emergency report parameters received: {parameters}")
        
        # ⭐ CRITICAL FIX: Check if this is the final form submission
        # Look for form completion indicators
        page_name = page_info.get("displayName", "")
        form_info = page_info.get("formInfo", {})
        
        print(f"🔍 FORM COMPLETION CHECK:")
        print(f"   - Page Name: {page_name}")
        print(f"   - Form Info: {form_info}")
        print(f"   - Has incident_type: {'incident_type' in parameters}")
        print(f"   - Has location: {'location' in parameters}")
        print(f"   - Has severity_level: {'severity_level' in parameters}")
        print(f"   - Has contact_info: {'contact_info' in parameters}")
        
        logger.info(f"🔍 FORM COMPLETION CHECK:")
        logger.info(f"   - Page Name: {page_name}")
        logger.info(f"   - Form Info: {form_info}")
        logger.info(f"   - Has incident_type: {'incident_type' in parameters}")
        logger.info(f"   - Has location: {'location' in parameters}")
        logger.info(f"   - Has severity_level: {'severity_level' in parameters}")
        logger.info(f"   - Has contact_info: {'contact_info' in parameters}")
        
        # ⭐ ONLY SAVE IF ALL REQUIRED PARAMETERS ARE PRESENT
        required_params = ['incident_type', 'location', 'severity_level', 'contact_info']
        missing_params = [param for param in required_params if not parameters.get(param)]
        
        if missing_params:
            print(f"⏳ FORM NOT COMPLETE - Missing parameters: {missing_params}")
            logger.info(f"⏳ FORM NOT COMPLETE - Missing parameters: {missing_params}")
            
            # Return success response WITHOUT saving data
            return {
                "fulfillmentResponse": {
                    "messages": [{"text": {"text": ["Please continue filling out the form."]}}]
                }
            }

        # ⭐ ALL PARAMETERS PRESENT - PROCEED WITH SAVING
        print("✅ ALL PARAMETERS PRESENT - PROCEEDING WITH SAVE")
        logger.info("✅ ALL PARAMETERS PRESENT - PROCEEDING WITH SAVE")

        # Generate incident ID
        incident_id = f"INC-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        
        print(f"🆔 Generated incident ID: {incident_id}")
        logger.info(f"🆔 Generated incident ID: {incident_id}")

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
        
        print(f"📊 Structured incident data: {incident_data}")
        logger.info(f"📊 Structured incident data: {incident_data}")
        
        print("💾 Attempting to save to Firestore...")
        logger.info("💾 Attempting to save to Firestore...")

        # Save to Firestore with detailed logging
        firestore_path = f"arems-profiles/emergency-reports/incidents/{incident_id}"
        print(f"📁 Firestore path: {firestore_path}")
        logger.info(f"📁 Firestore path: {firestore_path}")
        
        incident_ref = (db.collection('arems-profiles')
                       .document('emergency-reports')
                       .collection('incidents')
                       .document(incident_id))
        
        # Try the save operation
        print("🔄 Executing Firestore set operation...")
        logger.info("🔄 Executing Firestore set operation...")
        
        result = incident_ref.set(incident_data)
        
        print(f"✅ Firestore set operation completed: {result}")
        logger.info(f"✅ Firestore set operation completed: {result}")
        
        success_msg = f"✅ SUCCESSFULLY SAVED EMERGENCY REPORT: {incident_id}"
        print(success_msg)
        logger.info(success_msg)

        # Return response
        response = {
            "fulfillmentResponse": {
                "messages": [{"text": {"text": [f"Emergency report saved with ID: {incident_id}"]}}]
            },
            "sessionInfo": {
                "parameters": {
                    "incident_id": incident_id
                }
            }
        }
        
        print(f"📤 Returning response: {response}")
        logger.info(f"📤 Returning response: {response}")
        
        return response
        
    except Exception as e:
        error_msg = f"❌ ERROR IN EMERGENCY REPORT: {str(e)}"
        print(error_msg)
        logger.error(error_msg)
        import traceback
        traceback_msg = f"Full traceback: {traceback.format_exc()}"
        print(traceback_msg)
        logger.error(traceback_msg)
        return {
            "fulfillmentResponse": {
                "messages": [{"text": {"text": ["Error processing emergency report. Please try again."]}}]
            }
        }

def handle_risk_assessment(session_info, page_info, full_request):
    """Handle risk assessment webhook - ONLY save when form is complete"""
    
    print("📊 === PROCESSING RISK ASSESSMENT ===")
    logger.info("📊 === PROCESSING RISK ASSESSMENT ===")
    
    try:
        parameters = session_info.get('parameters', {})
        
        print(f"📝 Risk assessment parameters received: {parameters}")
        logger.info(f"📝 Risk assessment parameters received: {parameters}")
        
        # ⭐ CRITICAL FIX: Check if this is the final form submission
        page_name = page_info.get("displayName", "")
        form_info = page_info.get("formInfo", {})
        
        print(f"🔍 RISK FORM COMPLETION CHECK:")
        print(f"   - Page Name: {page_name}")
        print(f"   - Form Info: {form_info}")
        print(f"   - Has hazard_type: {'hazard_type' in parameters}")
        print(f"   - Has affected_area: {'affected_area' in parameters}")
        print(f"   - Has population_at_risk: {'population_at_risk' in parameters}")
        
        logger.info(f"🔍 RISK FORM COMPLETION CHECK:")
        logger.info(f"   - Page Name: {page_name}")
        logger.info(f"   - Form Info: {form_info}")
        logger.info(f"   - Has hazard_type: {'hazard_type' in parameters}")
        logger.info(f"   - Has affected_area: {'affected_area' in parameters}")
        logger.info(f"   - Has population_at_risk: {'population_at_risk' in parameters}")

        # ⭐ ONLY SAVE IF ALL REQUIRED PARAMETERS ARE PRESENT
        required_params = ['hazard_type', 'affected_area', 'population_at_risk']
        missing_params = [param for param in required_params if not parameters.get(param)]
        
        if missing_params:
            print(f"⏳ RISK FORM NOT COMPLETE - Missing parameters: {missing_params}")
            logger.info(f"⏳ RISK FORM NOT COMPLETE - Missing parameters: {missing_params}")
            
            # Return success response WITHOUT saving data
            return {
                "fulfillmentResponse": {
                    "messages": [{"text": {"text": ["Please continue with the risk assessment."]}}]
                }
            }

        # ⭐ ALL PARAMETERS PRESENT - PROCEED WITH SAVING
        print("✅ ALL RISK PARAMETERS PRESENT - PROCEEDING WITH SAVE")
        logger.info("✅ ALL RISK PARAMETERS PRESENT - PROCEEDING WITH SAVE")

        # Generate assessment ID
        assessment_id = f"RISK-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        
        print(f"🆔 Generated assessment ID: {assessment_id}")
        logger.info(f"🆔 Generated assessment ID: {assessment_id}")

        # Extract parameters
        hazard_type = parameters.get('hazard_type', '')
        affected_area = parameters.get('affected_area', '')
        population_at_risk = parameters.get('population_at_risk', '')
        
        print(f"🔍 Extracted parameters - Hazard: {hazard_type}, Area: {affected_area}, Population: {population_at_risk}")
        logger.info(f"🔍 Extracted parameters - Hazard: {hazard_type}, Area: {affected_area}, Population: {population_at_risk}")

        # Calculate risk score and level
        risk_score = calculate_risk_score(hazard_type, population_at_risk)
        risk_level = get_risk_level(risk_score)
        
        print(f"🧮 Calculated risk - Score: {risk_score}, Level: {risk_level}")
        logger.info(f"🧮 Calculated risk - Score: {risk_score}, Level: {risk_level}")

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

        print(f"📊 Structured assessment data: {assessment_data}")
        logger.info(f"📊 Structured assessment data: {assessment_data}")
        
        print("💾 Attempting to save to Firestore...")
        logger.info("💾 Attempting to save to Firestore...")

        # Save to Firestore with detailed logging
        firestore_path = f"arems-profiles/risk-assessments/assessments/{assessment_id}"
        print(f"📁 Firestore path: {firestore_path}")
        logger.info(f"📁 Firestore path: {firestore_path}")
        
        assessment_ref = (db.collection('arems-profiles')
                         .document('risk-assessments')
                         .collection('assessments')
                         .document(assessment_id))
        
        # Try the save operation
        print("🔄 Executing Firestore set operation...")
        logger.info("🔄 Executing Firestore set operation...")
        
        result = assessment_ref.set(assessment_data)
        
        print(f"✅ Firestore set operation completed: {result}")
        logger.info(f"✅ Firestore set operation completed: {result}")
        
        success_msg = f"✅ SUCCESSFULLY SAVED RISK ASSESSMENT: {assessment_id}"
        print(success_msg)
        logger.info(success_msg)

        # Return response with session parameters for routing
        response = {
            "fulfillmentResponse": {
                "messages": [{"text": {"text": [f"Risk assessment completed. ID: {assessment_id}"]}}]
            },
            "sessionInfo": {
                "parameters": {
                    "assessment_id": assessment_id,
                    "risk_level": risk_level,  # This is critical for your routes
                    "risk_score": risk_score
                }
            }
        }
        
        print(f"📤 Returning response: {response}")
        logger.info(f"📤 Returning response: {response}")
        
        return response
        
    except Exception as e:
        error_msg = f"❌ ERROR IN RISK ASSESSMENT: {str(e)}"
        print(error_msg)
        logger.error(error_msg)
        import traceback
        traceback_msg = f"Full traceback: {traceback.format_exc()}"
        print(traceback_msg)
        logger.error(traceback_msg)
        return {
            "fulfillmentResponse": {
                "messages": [{"text": {"text": ["Error processing risk assessment. Please try again."]}}]
            }
        }

def calculate_risk_score(hazard_type, population_risk):
    """Calculate risk score based on hazard and population"""
    
    print(f"🧮 Calculating risk score for: {hazard_type} + {population_risk}")
    logger.info(f"🧮 Calculating risk score for: {hazard_type} + {population_risk}")
    
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
    
    calculation_msg = f"Risk calculation: {hazard_type} ({base_score}) × {population_risk} ({multiplier}) = {final_score}"
    print(calculation_msg)
    logger.info(calculation_msg)
    
    return min(final_score, 100)

def get_risk_level(score):
    """Convert risk score to risk level"""
    if score >= 80:
        level = 'CRITICAL'
    elif score >= 60:
        level = 'HIGH'
    elif score >= 40:
        level = 'MEDIUM'
    else:
        level = 'LOW'
    
    print(f"🏆 Risk level for score {score}: {level}")
    logger.info(f"🏆 Risk level for score {score}: {level}")
    
    return level

# ============================================================================
# TELEGRAM WEBHOOK HANDLER - Full Featured (Same as before)
# ============================================================================

def handle_telegram_webhook(request):
    """Handle Telegram webhook requests with full functionality"""
    
    print("📱 === PROCESSING TELEGRAM WEBHOOK ===")
    logger.info("📱 === PROCESSING TELEGRAM WEBHOOK ===")
    
    try:
        req_json = request.get_json(silent=True)
        if not req_json:
            print("❌ No JSON data in Telegram request")
            logger.error("❌ No JSON data in Telegram request")
            return {"status": "error", "message": "No data"}, 400

        print(f"📱 Telegram request data: {req_json}")
        logger.info(f"📱 Telegram request data: {req_json}")

        # Handle different types of Telegram updates
        if "message" in req_json:
            return handle_telegram_message(req_json["message"])
        else:
            print("📱 Non-message Telegram update received")
            logger.info("📱 Non-message Telegram update received")
            return {"status": "success", "message": "Update processed"}

    except Exception as e:
        error_msg = f"❌ ERROR in Telegram webhook: {str(e)}"
        print(error_msg)
        logger.error(error_msg)
        import traceback
        traceback_msg = f"Full traceback: {traceback.format_exc()}"
        print(traceback_msg)
        logger.error(traceback_msg)
        return {"status": "error", "message": str(e)}, 500

def handle_telegram_message(message):
    """Process individual Telegram messages"""
    
    try:
        chat_id = message["chat"]["id"]
        text = message.get("text", "")
        username = message["from"].get("username", "unknown")
        
        print(f"📱 Telegram message from {username} ({chat_id}): {text}")
        logger.info(f"📱 Telegram message from {username} ({chat_id}): {text}")
        
        # Update user profile
        update_user_profile(chat_id, {
            'username': username,
            'last_active': firestore.SERVER_TIMESTAMP
        })
        
        # Store message
        store_message(chat_id, text)
        
        # Handle different types of content
        if "document" in message:
            print(f"📄 Document received from {username}")
            logger.info(f"📄 Document received from {username}")
            handle_document(message["document"], chat_id)
        elif "photo" in message:
            print(f"📸 Photo received from {username}")
            logger.info(f"📸 Photo received from {username}")
            handle_photo(message["photo"], chat_id)
        else:
            # Handle text message - you can add emergency keyword detection here
            if any(keyword in text.lower() for keyword in ['emergency', 'urgent', 'help', 'disaster']):
                response_text = f"🚨 Emergency detected! For immediate assistance, please use our Dialogflow CX emergency system or call emergency services. You said: {text}"
            else:
                response_text = f"Message received: {text}"
            
            send_message(chat_id, response_text)
        
        print("✅ Telegram message processed successfully")
        logger.info("✅ Telegram message processed successfully")
        return {"status": "success", "message": "Telegram message processed"}
        
    except Exception as e:
        error_msg = f"❌ ERROR processing Telegram message: {str(e)}"
        print(error_msg)
        logger.error(error_msg)
        return {"status": "error", "message": str(e)}, 500

# ============================================================================
# TELEGRAM UTILITY FUNCTIONS - Same as before
# ============================================================================

def get_file_path(file_id):
    """Get file path from Telegram for download"""
    try:
        url = f"{TELEGRAM_API_URL}/getFile"
        response = requests.get(url, params={"file_id": file_id})
        if response.status_code == 200:
            file_path = response.json()["result"]["file_path"]
            print(f"📁 Retrieved file path: {file_path}")
            logger.info(f"📁 Retrieved file path: {file_path}")
            return file_path
        else:
            print(f"❌ Failed to get file path: {response.status_code}")
            logger.error(f"❌ Failed to get file path: {response.status_code}")
            return None
    except Exception as e:
        print(f"❌ Error getting file path: {str(e)}")
        logger.error(f"❌ Error getting file path: {str(e)}")
        return None

def send_message(chat_id, text):
    """Send message to Telegram user"""
    try:
        url = f"{TELEGRAM_API_URL}/sendMessage"
        response = requests.post(url, json={"chat_id": chat_id, "text": text})
        if response.status_code == 200:
            print(f"✅ Message sent to {chat_id}: {text[:50]}...")
            logger.info(f"✅ Message sent to {chat_id}: {text[:50]}...")
        else:
            print(f"❌ Failed to send message: {response.status_code}")
            logger.error(f"❌ Failed to send message: {response.status_code}")
    except Exception as e:
        print(f"❌ Error sending message: {str(e)}")
        logger.error(f"❌ Error sending message: {str(e)}")

def handle_document(document, chat_id):
    """Handle document uploads to Cloud Storage"""
    try:
        file_id = document.get("file_id")
        file_name = document.get("file_name", "unnamed_file")
        file_size = document.get("file_size", 0)
        
        print(f"📄 Processing document: {file_name} ({file_size} bytes)")
        logger.info(f"📄 Processing document: {file_name} ({file_size} bytes)")
        
        file_path = get_file_path(file_id)
        if file_path:
            file_url = f"https://api.telegram.org/file/bot{TELEGRAM_TOKEN}/{file_path}"
            response = requests.get(file_url)
            
            if response.status_code == 200:
                # Get user info for organized storage
                user_ref = db.collection('arems-profiles').document('users').collection('profiles').document(str(chat_id))
                user_doc = user_ref.get()
                username = user_doc.get('username') if user_doc.exists else 'unknown'
                
                # Create organized storage path
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                date = datetime.now().strftime('%B_%d_%Y')
                storage_path = f"users/{chat_id}_{username}/{date}/documents/{timestamp}_{file_name}"
                
                # Upload to Cloud Storage
                blob = bucket.blob(storage_path)
                blob.upload_from_string(response.content)
                
                print(f"✅ Document uploaded: {storage_path}")
                logger.info(f"✅ Document uploaded: {storage_path}")
                
                send_message(chat_id, f"📄 Document '{file_name}' received and stored successfully!")
            else:
                print(f"❌ Failed to download document: {response.status_code}")
                logger.error(f"❌ Failed to download document: {response.status_code}")
                send_message(chat_id, "Sorry, couldn't process your document. Please try again.")
        else:
            send_message(chat_id, "Sorry, couldn't access your document. Please try again.")
            
    except Exception as e:
        print(f"❌ Error handling document: {str(e)}")
        logger.error(f"❌ Error handling document: {str(e)}")
        send_message(chat_id, "Sorry, there was an error processing your document.")

def handle_photo(photos, chat_id):
    """Handle photo uploads to Cloud Storage with enhanced error handling"""
    try:
        # Get user profile info
        user_ref = db.collection('arems-profiles').document('users').collection('profiles').document(str(chat_id))
        user_doc = user_ref.get()
        username = user_doc.get('username') if user_doc.exists else 'unknown'
        
        # Process the highest resolution photo
        photo = photos[-1]
        file_id = photo.get("file_id")
        file_size = photo.get("file_size", 0)
        
        print(f"📸 Processing photo from {username}: {file_size} bytes")
        logger.info(f"📸 Processing photo from {username}: {file_size} bytes")
        
        file_path = get_file_path(file_id)
        
        if file_path:
            file_url = f"https://api.telegram.org/file/bot{TELEGRAM_TOKEN}/{file_path}"
            response = requests.get(file_url)
            
            if response.status_code == 200:
                # Create organized storage path
                timestamp = datetime.now().strftime('%I-%M-%p')
                date = datetime.now().strftime('%B_%d_%Y')
                storage_path = f"users/{chat_id}_{username}/{date}/photos/photo_{timestamp}.jpg"
                
                # Upload to Cloud Storage
                blob = bucket.blob(storage_path)
                blob.upload_from_string(response.content)
                
                print(f"✅ Photo uploaded: {storage_path}")
                logger.info(f"✅ Successfully uploaded photo from {username} ({chat_id}) to {storage_path}")
                
                send_message(chat_id, "📸 Photo received and stored successfully!")
            else:
                print(f"❌ Failed to download photo: {response.status_code}")
                logger.error(f"❌ Failed to download photo: {response.status_code}")
                send_message(chat_id, "Sorry, couldn't process your photo. Please try again.")
        else:
            send_message(chat_id, "Sorry, couldn't access your photo. Please try again.")
            
    except Exception as e:
        print(f"❌ Error handling photo: {str(e)}")
        logger.error(f"❌ Error handling photo: {str(e)}")
        send_message(chat_id, "Sorry, there was an error processing your photo.")

def update_user_profile(chat_id: str, updates: Dict[Any, Any]) -> None:
    """Update user profile in Firestore with enhanced error handling"""
    try:
        user_ref = db.collection('arems-profiles').document('users').collection('profiles').document(str(chat_id))
        user_doc = user_ref.get()
        
        if not user_doc.exists:
            # Create new user profile
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
            
            print(f"👤 Created new user profile for {chat_id}")
            logger.info(f"👤 Created new user profile for {chat_id}")
        else:
            # Update existing user profile
            updates['last_active'] = firestore.SERVER_TIMESTAMP
            if 'total_messages' in updates:
                updates['total_messages'] = firestore.Increment(updates['total_messages'])
            user_ref.update(updates)
            
            print(f"👤 Updated user profile for {chat_id}")
            logger.info(f"👤 Updated user profile for {chat_id}")
            
    except Exception as e:
        print(f"❌ Error updating user profile for {chat_id}: {str(e)}")
        logger.error(f"❌ Error updating user profile for {chat_id}: {str(e)}")

def store_message(chat_id, text):
    """Store message in Firestore with organized structure"""
    try:
        # Get user info
        user_ref = db.collection('arems-profiles').document('users').collection('profiles').document(str(chat_id))
        user_doc = user_ref.get()
        username = user_doc.get('username') if user_doc.exists else 'unknown'
        
        # Create organized message storage
        chat_id_with_username = f"{chat_id}_{username}"
        current_date = datetime.now()
        date_str = current_date.strftime('%B_%d_%Y')
        time_str = current_date.strftime('%I:%M %p')
        
        # Store individual message
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
        
        # Update daily summary
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
        
        # Update user profile with latest message info
        update_user_profile(chat_id, {
            'total_messages': 1,
            'last_message': text,
            'last_message_time': f"{date_str} at {time_str}"
        })
        
        print(f"💬 Message stored for {username}: {text[:50]}...")
        logger.info(f"💬 Message stored for {username}: {text[:50]}...")
        
    except Exception as e:
        print(f"❌ Error storing message: {str(e)}")
        logger.error(f"❌ Error storing message: {str(e)}")
    
    return True