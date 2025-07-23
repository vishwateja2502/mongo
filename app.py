from flask import Flask, jsonify, request
import json
import time
import threading
import requests
from pymongo import MongoClient, errors

app = Flask(__name__)

# Import OpenAI with error handling for version compatibility
try:
    from openai import OpenAI
    # OpenAI client for LLM call
    llm_client = OpenAI(
        base_url="https://openrouter.ai/api/v1",
        api_key="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"
        
    )
    print("✅ OpenAI client initialized successfully")
except Exception as e:
    print(f"❌ OpenAI client initialization failed: {e}")
    # Fallback - app will still work for serving existing data
    llm_client = None

# MongoDB connection
client = MongoClient("mongodb+srv://vishwateja2502:vishwa%4025@cluster0.ig42emq.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client["CallAnalysisDB"]
collection = db["CallInsights"]

# Ensure uniqueness
collection.create_index("CallId", unique=True)

# Retell API configuration
RETELL_API_KEY = "key_735ce7ee7176a4d8a1da3856db44"

# Auto-processing control
auto_processing_enabled = True

def get_retell_calls_with_correct_api(limit=5):
    """Get only the most recent 5 ended calls - focus on new calls only"""
    try:
        # CORRECT endpoint from Retell documentation
        url = "https://api.retellai.com/v2/list-calls"
        
        headers = {
            "Authorization": f"Bearer {RETELL_API_KEY}",
            "Content-Type": "application/json"
        }
        
        # Filter for only recent ENDED calls - limit to 5 most recent
        body = {
            "filter_criteria": {
                "call_status": ["ended"]  # Use "ended" as per API allowed values
            }
        }
        
        response = requests.post(
            url,
            headers=headers,
            json=body,
            timeout=15
        )
        
        if response.status_code == 200:
            data = response.json()
            
            # The response should contain call data
            if isinstance(data, list):
                calls = data[:5]  # Only get first 5 most recent
            elif isinstance(data, dict):
                calls = (data.get("calls") or data.get("data") or data.get("results") or [])[:5]
            else:
                calls = []
            
            if calls and len(calls) > 0:
                # Convert to CallObject format - ONLY calls with actual transcripts
                call_objects = []
                for call_data in calls:
                    call_id = call_data.get('call_id') or call_data.get('id') or ''
                    transcript = call_data.get('transcript') or ''
                    call_status = call_data.get('call_status', '')
                    
                    # ONLY process ended calls with actual transcript content
                    if (call_id and 
                        call_status == "ended" and 
                        transcript and 
                        len(transcript.strip()) > 20):  # At least 20 characters of actual content
                        
                        call_obj = CallObject(call_id=call_id, transcript=transcript)
                        call_objects.append(call_obj)
                
                return call_objects
            else:
                return []
                
        elif response.status_code == 400:
            print(f"❌ Bad Request (400) - Request format issue")
            print(f"📄 Response: {response.text}")
        elif response.status_code == 401:
            print("❌ Retell API authentication failed - check your API key")
        elif response.status_code == 403:
            print("❌ Access forbidden - API key doesn't have permission")
        else:
            print(f"❌ Retell API error: {response.status_code}")
            print(f"📄 Response: {response.text}")
        
        return []
        
    except Exception as e:
        print(f"❌ Retell API connection failed: {e}")
        return []

class CallObject:
    """Simple call object"""
    def __init__(self, call_id, transcript):
        self.call_id = call_id
        self.transcript = transcript

# Prompt builder (unchanged as requested)
def build_prompt(transcript):
    return f"""
Analyze this call transcript and return ONLY a valid JSON object with no additional text or explanation:

TRANSCRIPT:
\"\"\"{transcript}\"\"\"

Extract these 8 elements and format as JSON:

1. Sentiment — Overall sentiment with brief reasoning
2. Customer Emotion Journey — How emotions changed during call  
3. Topic Identification — Main subjects discussed
4. Primary Call Intent — What caller wanted to achieve
5. Transfer Reason — Why transfer occurred (or "No transfer occurred")
6. Competitors Mentioned — Any competitors mentioned (or "None mentioned")
7. Key Themes Identified — Main themes/patterns
8. Overall Context — Brief call summary

Return ONLY this JSON structure with no other text because im directly returning the JSON which you are giving as output.So please do not add any additional text or explanation. and give only in json:

{{
  "sentiment": "your analysis here in 1-2 sentences",
  "customer_emotion_journey": "your analysis here in 1-2 sentences",
  "topic_identification": "your analysis here in 1-2 sentences", 
  "primary_call_intent": "your analysis here in 1-2 sentences",
  "transfer_reason": "your analysis here in 1-2 sentences",
  "competitors_mentioned": "your analysis here in 1-2 sentences",
  "key_themes_identified": "your analysis here in 1-2 sentences",
  "overall_context": "your analysis here in 1-2 sentences"
}}"""

def process_single_call(call, call_index):
    """Process a single call and return result"""
    try:
        transcript = call.transcript if hasattr(call, 'transcript') else ""
        call_id = call.call_id if hasattr(call, 'call_id') else f"CALL_{call_index}"
        
        if not transcript or not transcript.strip() or len(transcript.strip()) <= 20:
            # Skip calls without proper transcript content
            return {"call_id": call_id, "status": "skipped", "message": "No transcript content - call may not be completed yet"}
        
        # Build prompt and get analysis
        prompt = build_prompt(transcript)
        
        try:
            if llm_client is None:
                # If OpenAI client failed to initialize, store error
                analysis_data = {
                    "sentiment": "OpenAI client not available - deployment issue",
                    "customer_emotion_journey": "OpenAI client not available - deployment issue",
                    "topic_identification": "OpenAI client not available - deployment issue",
                    "primary_call_intent": "OpenAI client not available - deployment issue", 
                    "transfer_reason": "OpenAI client not available - deployment issue",
                    "competitors_mentioned": "OpenAI client not available - deployment issue",
                    "key_themes_identified": "OpenAI client not available - deployment issue",
                    "overall_context": "OpenAI client not available - deployment issue"
                }
            else:
                response = llm_client.chat.completions.create(
                    model="meta-llama/llama-3-8b-instruct",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.1,
                    max_tokens=2000,
                    timeout=30
                )
                response = llm_client.chat.completions.create(
                    model="meta-llama/llama-3-8b-instruct",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.1,
                    max_tokens=2000,
                    timeout=30
                )
                
                # Parse the LLM response
                analysis_text = response.choices[0].message.content.strip()
                
                # Try delimited format first
                if '|||' in analysis_text:
                    try:
                        parts = analysis_text.split('|||')
                        if len(parts) >= 8:
                            analysis_data = {
                                "sentiment": parts[0].replace('SENTIMENT:', '').strip(),
                                "customer_emotion_journey": parts[1].replace('CUSTOMER_EMOTION_JOURNEY:', '').strip(),
                                "topic_identification": parts[2].replace('TOPIC_IDENTIFICATION:', '').strip(),
                                "primary_call_intent": parts[3].replace('PRIMARY_CALL_INTENT:', '').strip(),
                                "transfer_reason": parts[4].replace('TRANSFER_REASON:', '').strip(),
                                "competitors_mentioned": parts[5].replace('COMPETITORS_MENTIONED:', '').strip(),
                                "key_themes_identified": parts[6].replace('KEY_THEMES_IDENTIFIED:', '').strip(),
                                "overall_context": parts[7].replace('OVERALL_CONTEXT:', '').strip()
                            }
                        else:
                            return {"call_id": call_id, "status": "error", "message": "Insufficient delimited parts"}
                    except Exception as e:
                        return {"call_id": call_id, "status": "error", "message": str(e)}
                
                # Fallback to JSON parsing
                else:
                    try:
                        analysis_data = json.loads(analysis_text)
                    except json.JSONDecodeError:
                        # Try to repair truncated JSON
                        try:
                            fixed_text = analysis_text.strip()
                            if fixed_text.count('"') % 2 == 1:
                                fixed_text += '"'
                            if not fixed_text.endswith('}'):
                                fixed_text += '}'
                            
                            analysis_data = json.loads(fixed_text)
                        except:
                            # Extract partial data using regex
                            try:
                                import re
                                
                                sentiment_match = re.search(r'"sentiment":\s*"([^"]*)"', analysis_text)
                                emotion_match = re.search(r'"customer_emotion_journey":\s*"([^"]*)"', analysis_text)
                                topic_match = re.search(r'"topic_identification":\s*"([^"]*)"', analysis_text)
                                intent_match = re.search(r'"primary_call_intent":\s*"([^"]*)"', analysis_text)
                                transfer_match = re.search(r'"transfer_reason":\s*"([^"]*)"', analysis_text)
                                competitors_match = re.search(r'"competitors_mentioned":\s*"([^"]*)"', analysis_text)
                                themes_match = re.search(r'"key_themes_identified":\s*"([^"]*)"', analysis_text)
                                context_match = re.search(r'"overall_context":\s*"([^"]*)"', analysis_text)
                                
                                analysis_data = {
                                    "sentiment": sentiment_match.group(1) if sentiment_match else "Unable to parse - truncated response",
                                    "customer_emotion_journey": emotion_match.group(1) if emotion_match else "Unable to parse - truncated response",
                                    "topic_identification": topic_match.group(1) if topic_match else "Unable to parse - truncated response", 
                                    "primary_call_intent": intent_match.group(1) if intent_match else "Unable to parse - truncated response",
                                    "transfer_reason": transfer_match.group(1) if transfer_match else "Unable to parse - truncated response",
                                    "competitors_mentioned": competitors_match.group(1) if competitors_match else "Unable to parse - truncated response",
                                    "key_themes_identified": themes_match.group(1) if themes_match else "Unable to parse - truncated response",
                                    "overall_context": context_match.group(1) if context_match else "Unable to parse - truncated response"
                                }
                            except:
                                analysis_data = {
                                    "sentiment": f"PARSING ERROR - Raw response: {analysis_text[:100]}...",
                                    "customer_emotion_journey": "PARSING ERROR - Unable to extract data",
                                    "topic_identification": "PARSING ERROR - Unable to extract data", 
                                    "primary_call_intent": "PARSING ERROR - Unable to extract data",
                                    "transfer_reason": "PARSING ERROR - Unable to extract data",
                                    "competitors_mentioned": "PARSING ERROR - Unable to extract data",
                                    "key_themes_identified": "PARSING ERROR - Unable to extract data",
                                    "overall_context": "PARSING ERROR - Unable to extract data"
                                }
        
        except Exception as api_error:
            analysis_data = {
                "sentiment": f"API ERROR: {str(api_error)[:100]}",
                "customer_emotion_journey": "API ERROR - Unable to process call",
                "topic_identification": "API ERROR - Unable to process call",
                "primary_call_intent": "API ERROR - Unable to process call", 
                "transfer_reason": "API ERROR - Unable to process call",
                "competitors_mentioned": "API ERROR - Unable to process call",
                "key_themes_identified": "API ERROR - Unable to process call",
                "overall_context": "API ERROR - Unable to process call"
            }
        
        # Prepare document for MongoDB
        document = {
            "CallId": call_id,
            "sentiment": analysis_data.get("sentiment", ""),
            "customer_emotion_journey": analysis_data.get("customer_emotion_journey", ""),
            "topic_identification": analysis_data.get("topic_identification", ""),
            "primary_call_intent": analysis_data.get("primary_call_intent", ""),
            "transfer_reason": analysis_data.get("transfer_reason", ""),
            "competitors_mentioned": analysis_data.get("competitors_mentioned", ""),
            "key_themes_identified": analysis_data.get("key_themes_identified", ""),
            "overall_context": analysis_data.get("overall_context", "")
        }
        
        # Insert into MongoDB
        try:
            result = collection.insert_one(document)
            return {"call_id": call_id, "status": "success", "inserted_id": str(result.inserted_id)}
        except errors.DuplicateKeyError:
            return {"call_id": call_id, "status": "duplicate", "message": "Already exists"}
        except Exception as db_error:
            return {"call_id": call_id, "status": "error", "message": str(db_error)}
            
    except Exception as e:
        return {"call_id": call_id if 'call_id' in locals() else "unknown", "status": "error", "message": str(e)}

def auto_check_for_new_calls():
    """Check for new calls every 10 seconds"""
    while auto_processing_enabled:
        try:
            print("🔍 Checking for new calls...")
            
            # Get only 5 most recent calls from Retell API  
            recent_calls = get_retell_calls_with_correct_api(limit=5)
            
            if recent_calls:
                print(f"📋 Retrieved {len(recent_calls)} ended calls with transcripts from Retell")
                
                # Filter for truly NEW calls (not in database)
                new_calls = []
                already_processed = 0
                
                for call in recent_calls:
                    if call.call_id:
                        existing_call = collection.find_one({"CallId": call.call_id})
                        if not existing_call:
                            new_calls.append(call)
                        else:
                            already_processed += 1
                
                print(f"📊 Status: {len(new_calls)} new calls, {already_processed} already in database")
                
                if new_calls:
                    print(f"🎯 Found {len(new_calls)} new calls! Pushing to database...")
                    for call in new_calls:
                        print(f"📞 Processing: {call.call_id}")
                        result = process_single_call(call, 1)
                        if result["status"] == "success":
                            print(f"✅ Successfully stored in database")
                        elif result["status"] == "duplicate":
                            print(f"⚠️ Already exists in database")
                        elif result["status"] == "success_empty_transcript":
                            print(f"✅ Stored (empty transcript)")
                        else:
                            # Show the actual error message
                            error_msg = result.get('message', 'No error message provided')
                            print(f"❌ Error: {error_msg}")
                            print(f"   Status: {result.get('status', 'unknown')}")
                        time.sleep(1)  # Rate limiting
                    print(f"🎉 Completed! {len(new_calls)} new calls processed")
                else:
                    print("ℹ️ No new calls found - all calls already in database")
            else:
                print("ℹ️ No calls retrieved from Retell API")
            
            print("⏰ Waiting 10 seconds before next check...\n")
            # Wait 10 seconds before next check
            time.sleep(10)
            
        except Exception as e:
            print(f"❌ Auto-processing error: {e}")
            print("⏰ Retrying in 10 seconds...\n")
            time.sleep(10)

def start_auto_processing():
    """Start auto-processing in background thread"""
    auto_thread = threading.Thread(target=auto_check_for_new_calls, daemon=True)
    auto_thread.start()
    print("🤖 Auto-processing started - ready to process new calls")

# Webhook endpoint for Retell to send new call data
@app.route("/webhook/retell", methods=["POST"])
def retell_webhook():
    """Webhook endpoint for Retell to send new call data"""
    try:
        data = request.get_json()
        
        # Extract call data from webhook
        call_id = data.get("call_id", f"WEBHOOK_{int(time.time())}")
        transcript = data.get("transcript", "")
        
        print(f"📥 Received webhook for call: {call_id}")
        
        # Create call object
        call = CallObject(call_id, transcript)
        
        # Check if call already exists
        existing_call = collection.find_one({"CallId": call_id})
        if existing_call:
            return jsonify({"message": "Call already processed", "call_id": call_id}), 200
        
        # Process the new call
        result = process_single_call(call, 1)
        
        return jsonify({
            "message": "Call processed successfully",
            "result": result
        }), 200
        
    except Exception as e:
        print(f"❌ Webhook error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/auto-processing/status", methods=["GET"])
def get_auto_status():
    """Get auto-processing status"""
    return jsonify({
        "auto_processing_enabled": auto_processing_enabled,
        "message": "Auto-processing is " + ("ENABLED" if auto_processing_enabled else "DISABLED"),
        "webhook_endpoint": "/webhook/retell"
    })

@app.route("/auto-processing/stop", methods=["POST"])
def stop_auto_processing():
    """Stop auto-processing"""
    global auto_processing_enabled
    auto_processing_enabled = False
    return jsonify({"message": "Auto-processing stopped"})

@app.route("/auto-processing/start", methods=["POST"])
def start_auto_processing_endpoint():
    """Start auto-processing"""
    global auto_processing_enabled
    auto_processing_enabled = True
    start_auto_processing()
    return jsonify({"message": "Auto-processing started"})

@app.route("/analyze-call", methods=["POST"])
def analyze_single_call():
    """Analyze a single call - for manual processing"""
    try:
        data = request.get_json()
        transcript = data.get("transcript", "")
        call_id = data.get("call_id", f"MANUAL_{int(time.time())}")
        
        # Create call object
        call = CallObject(call_id, transcript)
        
        result = process_single_call(call, 1)
        return jsonify(result)
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/get-analysis/<call_id>", methods=["GET"])
def get_analysis(call_id):
    """Get analysis for a specific call"""
    try:
        document = collection.find_one({"CallId": call_id})
        if document:
            document['_id'] = str(document['_id'])
            return jsonify(document)
        else:
            return jsonify({"error": "Call not found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/get-all-analysis", methods=["GET"])
def get_all_analysis():
    """Get all call analyses"""
    try:
        documents = list(collection.find())
        for doc in documents:
            doc['_id'] = str(doc['_id'])
        return jsonify(documents)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/public-data", methods=["GET"])
def get_public_data():
    """Public endpoint for live call analysis data - MAIN SHARING ENDPOINT"""
    try:
        documents = list(collection.find())
        for doc in documents:
            doc['_id'] = str(doc['_id'])
        return jsonify({
            "total_calls": len(documents),
            "last_updated": time.strftime("%Y-%m-%d %H:%M:%S"),
            "data": documents
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint for deployment"""
    try:
        collection.count_documents({})
        return jsonify({
            "status": "healthy",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "database_connected": True,
            "auto_processing_enabled": auto_processing_enabled
        })
    except Exception as e:
        return jsonify({
            "status": "unhealthy",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "database_connected": False,
            "error": str(e)
        }), 500

@app.route("/stats", methods=["GET"])
def get_stats():
    """Get database statistics"""
    try:
        total_calls = collection.count_documents({})
        success_calls = collection.count_documents({"sentiment": {"$not": {"$regex": "ERROR"}}})
        error_calls = collection.count_documents({"sentiment": {"$regex": "ERROR"}})
        
        return jsonify({
            "total_calls": total_calls,
            "successful_analyses": success_calls,
            "error_analyses": error_calls,
            "success_rate": f"{(success_calls/total_calls*100):.1f}%" if total_calls > 0 else "0%"
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/", methods=["GET"])
def home():
    """Home endpoint with available routes"""
    try:
        total_calls = collection.count_documents({})
        return jsonify({
            "message": "Call Analysis API - Production Ready for Render",
            "status": "operational",
            "total_calls_in_database": total_calls,
            "auto_processing_enabled": auto_processing_enabled,
            "available_endpoints": {
                "/public-data": "🌐 PUBLIC: Live call analysis data (SHARE THIS LINK)",
                "/webhook/retell": "POST: Webhook for Retell to send new call data",
                "/analyze-call": "POST: Analyze a single call (JSON: {transcript, call_id})",
                "/auto-processing/status": "GET: Check auto-processing status",
                "/auto-processing/start": "POST: Start auto-processing",
                "/auto-processing/stop": "POST: Stop auto-processing",
                "/get-analysis/<call_id>": "GET: Get analysis for specific call",
                "/get-all-analysis": "GET: Get all call analyses",
                "/stats": "GET: Database statistics",
                "/health": "GET: Health check for deployment"
            },
            "main_sharing_url": "/public-data",
            "webhook_url": "/webhook/retell",
            "last_updated": time.strftime("%Y-%m-%d %H:%M:%S")
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    print("🚀 Starting Call Analysis API - NO RETELL SDK DEPENDENCIES...")
    print("📋 Available endpoints:")
    print("  🌐 GET /public-data - PUBLIC: Live call analysis data")
    print("  📥 POST /webhook/retell - Webhook for new call data")
    print("  POST /analyze-call - Analyze a single call")
    print("  GET /auto-processing/status - Auto-processing status")
    print("  POST /auto-processing/start - Start auto-processing")
    print("  POST /auto-processing/stop - Stop auto-processing")
    print("  GET /get-analysis/<call_id> - Get specific call analysis")
    print("  GET /get-all-analysis - Get all analyses")
    print("  GET /stats - Database statistics")
    print("  GET /health - Health check")
    print("🌐 Main sharing endpoint: /public-data")
    print("📥 Webhook endpoint: /webhook/retell")
    
    # Start auto-processing
    start_auto_processing()
    
    print("✅ Ready for deployment on Render!")
    
    app.run(host='0.0.0.0', port=5000, debug=False)
else:
    # For production deployment
    print("🚀 Call Analysis API - Production Mode")
    print("🌐 Public data endpoint: /public-data")
    print("📥 Webhook endpoint: /webhook/retell")
    start_auto_processing()
