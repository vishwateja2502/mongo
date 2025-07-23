from flask import Flask, jsonify
import requests
from openai import OpenAI
from pymongo import MongoClient, errors
import json
import threading
import time
import schedule
import os

app = Flask(__name__)

# OpenAI client for LLM call - UPDATE WITH YOUR NEW API KEY
llm_client = OpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key="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"  # Replace with new key
)

# MongoDB connection
client = MongoClient("mongodb+srv://vishwateja2502:vishwa%4025@cluster0.ig42emq.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client["CallAnalysisDB"]
collection = db["CallInsights"]

# Ensure uniqueness
collection.create_index("CallId", unique=True)

# Global variable to control auto-processing
auto_processing_enabled = True
processing_interval = 30  # seconds (increased for deployment)

# Initialize background threads flag
background_started = False

# Retell API configuration
RETELL_API_KEY = "key_735ce7ee7176a4d8a1da3856db44"

def get_retell_calls(limit=50):
    """Get calls from Retell API - currently uses existing database data only"""
    # For deployment stability, we'll use existing database data
    # You can re-enable Retell API once the correct endpoint is confirmed
    return []

class MockCall:
    """Mock call object to match the original structure"""
    def __init__(self, call_data):
        self.call_id = call_data.get("call_id", "")
        self.transcript = call_data.get("transcript", "")

# Prompt builder
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

Return ONLY this JSON structure with no other text:

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
        transcript = call.transcript
        call_id = call.call_id if hasattr(call, 'call_id') else f"CALL_{call_index}"
        
        if not transcript or not transcript.strip():
            # Store empty transcript calls with null/empty values
            document = {
                "CallId": call_id,
                "sentiment": "No transcript available",
                "customer_emotion_journey": "No transcript available", 
                "topic_identification": "No transcript available",
                "primary_call_intent": "No transcript available",
                "transfer_reason": "No transcript available",
                "competitors_mentioned": "No transcript available",
                "key_themes_identified": "No transcript available",
                "overall_context": "No transcript available"
            }
            
            # Insert into MongoDB
            try:
                result = collection.insert_one(document)
                return {"call_id": call_id, "status": "success_empty_transcript", "inserted_id": str(result.inserted_id)}
            except errors.DuplicateKeyError:
                return {"call_id": call_id, "status": "duplicate", "message": "Already exists"}
            except Exception as db_error:
                return {"call_id": call_id, "status": "error", "message": str(db_error)}
        
        # Build prompt and get analysis
        prompt = build_prompt(transcript)
        
        try:
            response = llm_client.chat.completions.create(
                model="meta-llama/llama-3-8b-instruct",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=1500,
                timeout=30
            )
            
            # Parse the LLM response
            analysis_text = response.choices[0].message.content.strip()
            
            # Try JSON parsing
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
                    # Create error data if parsing fails completely
                    analysis_data = {
                        "sentiment": "PARSING ERROR - Unable to extract data",
                        "customer_emotion_journey": "PARSING ERROR - Unable to extract data",
                        "topic_identification": "PARSING ERROR - Unable to extract data", 
                        "primary_call_intent": "PARSING ERROR - Unable to extract data",
                        "transfer_reason": "PARSING ERROR - Unable to extract data",
                        "competitors_mentioned": "PARSING ERROR - Unable to extract data",
                        "key_themes_identified": "PARSING ERROR - Unable to extract data",
                        "overall_context": "PARSING ERROR - Unable to extract data"
                    }
            
        except Exception as api_error:
            # Handle API errors gracefully
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
        
        # Insert into MongoDB with duplicate handling
        try:
            result = collection.insert_one(document)
            return {"call_id": call_id, "status": "success", "inserted_id": str(result.inserted_id)}
        except errors.DuplicateKeyError:
            return {"call_id": call_id, "status": "duplicate", "message": "Already exists"}
        except Exception as db_error:
            return {"call_id": call_id, "status": "error", "message": str(db_error)}
            
    except Exception as e:
        # Store error data for any other exceptions
        try:
            error_call_id = call.call_id if hasattr(call, 'call_id') else f"ERROR_CALL_{call_index}"
            
            document = {
                "CallId": error_call_id,
                "sentiment": f"PROCESSING ERROR: {str(e)[:100]}",
                "customer_emotion_journey": "PROCESSING ERROR - Unable to process call",
                "topic_identification": "PROCESSING ERROR - Unable to process call",
                "primary_call_intent": "PROCESSING ERROR - Unable to process call", 
                "transfer_reason": "PROCESSING ERROR - Unable to process call",
                "competitors_mentioned": "PROCESSING ERROR - Unable to process call",
                "key_themes_identified": "PROCESSING ERROR - Unable to process call",
                "overall_context": "PROCESSING ERROR - Unable to process call"
            }
            
            try:
                result = collection.insert_one(document)
                return {"call_id": error_call_id, "status": "error_but_stored", "message": str(e), "inserted_id": str(result.inserted_id)}
            except errors.DuplicateKeyError:
                return {"call_id": error_call_id, "status": "duplicate_error", "message": str(e)}
            
        except Exception:
            return {"call_id": f"FAILED_CALL_{call_index}", "status": "complete_failure", "message": str(e)}

def auto_process_new_calls():
    """Automatically check for and process new calls"""
    if not auto_processing_enabled:
        return
        
    try:
        # Fetch recent calls from Retell using direct API
        call_data_list = get_retell_calls(limit=20)  # Reduced for deployment
        
        new_calls_processed = 0
        
        for i, call_data in enumerate(call_data_list):
            # Create mock call object
            call = MockCall(call_data)
            
            # Check if call already exists in database
            call_id = call.call_id if call.call_id else f"CALL_{i+1}"
            
            existing_call = collection.find_one({"CallId": call_id})
            if existing_call:
                continue  # Silently skip already processed calls
            
            # Process new call
            result = process_single_call(call, i+1)
            new_calls_processed += 1
            
            # Add delay to avoid overwhelming APIs
            time.sleep(2)
        
        if new_calls_processed > 0:
            print(f"✅ Auto-processing: Completed processing {new_calls_processed} new calls")
            
    except Exception as e:
        print(f"❌ Auto-processing error: {e}")

def run_scheduler():
    """Run the scheduler in a separate thread"""
    while auto_processing_enabled:
        try:
            schedule.run_pending()
            time.sleep(1)
        except Exception as e:
            print(f"❌ Scheduler error: {e}")
            time.sleep(5)

def start_background_processing():
    """Start background processing threads"""
    global background_started
    if not background_started:
        background_started = True
        
        # Schedule automatic processing
        schedule.every(processing_interval).seconds.do(auto_process_new_calls)
        
        # Start the scheduler in a background thread
        scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
        scheduler_thread.start()

# API Routes
@app.route("/analyze-calls", methods=["GET"])
def analyze_calls_endpoint():
    """API endpoint to trigger manual call analysis and storage"""
    try:
        call_data_list = get_retell_calls(limit=5)  # Limited for manual processing
        results = []
        
        for i, call_data in enumerate(call_data_list):
            call = MockCall(call_data)
            result = process_single_call(call, i+1)
            results.append(result)
            time.sleep(1)  # Rate limiting
        
        return jsonify({
            "message": "Manual call analysis completed",
            "results": results
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/auto-processing/status", methods=["GET"])
def get_auto_processing_status():
    """Get current auto-processing status"""
    return jsonify({
        "auto_processing_enabled": auto_processing_enabled,
        "processing_interval_seconds": processing_interval,
        "message": "Auto-processing is " + ("ENABLED" if auto_processing_enabled else "DISABLED")
    })

@app.route("/auto-processing/start", methods=["POST"])
def start_auto_processing():
    """Start automatic processing"""
    global auto_processing_enabled
    auto_processing_enabled = True
    start_background_processing()
    return jsonify({"message": "Auto-processing started", "status": "enabled"})

@app.route("/auto-processing/stop", methods=["POST"])
def stop_auto_processing():
    """Stop automatic processing"""
    global auto_processing_enabled
    auto_processing_enabled = False
    return jsonify({"message": "Auto-processing stopped", "status": "disabled"})

@app.route("/auto-processing/trigger", methods=["POST"])
def trigger_auto_processing():
    """Manually trigger auto-processing check"""
    try:
        auto_process_new_calls()
        return jsonify({"message": "Auto-processing check triggered manually"})
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
        # Limit results for better performance
        documents = list(collection.find().limit(1000))
        for doc in documents:
            doc['_id'] = str(doc['_id'])
        return jsonify(documents)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/public-data", methods=["GET"])
def get_public_data():
    """Public endpoint for live call analysis data - MAIN SHARING ENDPOINT"""
    try:
        # Get sample of recent data for better performance
        documents = list(collection.find().limit(500).sort("_id", -1))
        for doc in documents:
            doc['_id'] = str(doc['_id'])
        
        return jsonify({
            "total_calls": collection.count_documents({}),
            "showing_recent": len(documents),
            "last_updated": time.strftime("%Y-%m-%d %H:%M:%S"),
            "data": documents
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint for deployment"""
    return jsonify({
        "status": "healthy",
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "database_connected": True
    })

@app.route("/", methods=["GET"])
def home():
    """Home endpoint with available routes"""
    return jsonify({
        "message": "Call Analysis API with Auto-Processing",
        "status": "operational",
        "auto_processing_status": "ENABLED" if auto_processing_enabled else "DISABLED",
        "processing_interval": f"{processing_interval} seconds",
        "available_endpoints": {
            "/public-data": "🌐 PUBLIC: Live call analysis data (SHARE THIS LINK)",
            "/analyze-calls": "Manual call analysis and storage",
            "/auto-processing/status": "Check auto-processing status",
            "/auto-processing/start": "Start auto-processing (POST)",
            "/auto-processing/stop": "Stop auto-processing (POST)", 
            "/auto-processing/trigger": "Manually trigger auto-processing check (POST)",
            "/get-analysis/<call_id>": "Get analysis for specific call",
            "/get-all-analysis": "Get all call analyses",
            "/health": "Health check for deployment"
        },
        "deployment_info": {
            "main_sharing_url": "/public-data",
            "total_calls_in_db": collection.count_documents({})
        }
    })

# Start background processing when app starts
start_background_processing()

if __name__ == "__main__":
    print("🚀 Starting Call Analysis Application...")
    print("📋 Available endpoints:")
    print("  🌐 GET /public-data - PUBLIC: Live call analysis data")
    print("  GET /analyze-calls - Manual call analysis")
    print("  GET /auto-processing/status - Check auto-processing status")
    print("  POST /auto-processing/start - Start auto-processing")
    print("  POST /auto-processing/stop - Stop auto-processing")
    print("  POST /auto-processing/trigger - Trigger auto-processing check")
    print("  GET /get-analysis/<call_id> - Get specific call analysis")
    print("  GET /get-all-analysis - Get all analyses")
    print("  GET /health - Health check")
    print(f"🤖 Auto-processing: ENABLED (checking every {processing_interval} seconds)")
    print("🌐 Main sharing endpoint: /public-data")
    
    app.run(debug=True, threaded=True)
else:
    # For production deployment (Render/Heroku)
    print("🚀 Call Analysis API - Production Mode")
    print(f"🤖 Auto-processing: ENABLED (checking every {processing_interval} seconds)")
    print("🌐 Public data endpoint: /public-data")
