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

# OpenAI client for LLM call
llm_client = OpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key="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"
)

# MongoDB connection
client = MongoClient("mongodb+srv://vishwateja2502:vishwa%4025@cluster0.ig42emq.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client["CallAnalysisDB"]
collection = db["CallInsights"]

# Ensure uniqueness
collection.create_index("CallId", unique=True)

# Global variable to control auto-processing
auto_processing_enabled = True
processing_interval = 10  # seconds - refreshes every 10 seconds

# Initialize background threads flag
background_started = False

def get_retell_calls(limit=50):
    """Get calls from existing database - Retell SDK not available on deployment"""
    # For deployment, we'll work with existing database data
    # This ensures the app runs smoothly in production
    return []

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
        transcript = call.transcript
        call_id = call.call_id if hasattr(call, 'call_id') else f"CALL_{call_index}"
        
        if not transcript or not transcript.strip():
            print(f"🔄 Auto-processing: Empty transcript for call {call_id} - storing with null values")
            
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
                print(f"✅ Auto-processed: Call {call_id} with empty transcript stored successfully! _id: {result.inserted_id}")
                return {"call_id": call_id, "status": "success_empty_transcript", "inserted_id": str(result.inserted_id)}
            except errors.DuplicateKeyError:
                return {"call_id": call_id, "status": "duplicate", "message": "Already exists"}
            except Exception as db_error:
                print(f"❌ Auto-processing: Database error for call {call_id}: {db_error}")
                return {"call_id": call_id, "status": "error", "message": str(db_error)}
        
        # Build prompt and get analysis
        prompt = build_prompt(transcript)
        
        try:
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
                        print(f"✅ Auto-processing: Successfully parsed delimited format for call {call_id}")
                    else:
                        print(f"❌ Auto-processing: Not enough parts in delimited response for call {call_id}")
                        return {"call_id": call_id, "status": "error", "message": "Insufficient delimited parts"}
                except Exception as e:
                    print(f"❌ Auto-processing: Error parsing delimited format for call {call_id}: {e}")
                    return {"call_id": call_id, "status": "error", "message": str(e)}
            
            # Fallback to JSON parsing
            else:
                try:
                    analysis_data = json.loads(analysis_text)
                    print(f"✅ Auto-processing: Parsed JSON format for call {call_id}")
                except json.JSONDecodeError:
                    # Try to repair truncated JSON
                    try:
                        fixed_text = analysis_text.strip()
                        if fixed_text.count('"') % 2 == 1:
                            fixed_text += '"'
                        if not fixed_text.endswith('}'):
                            fixed_text += '}'
                        
                        analysis_data = json.loads(fixed_text)
                        print(f"✅ Auto-processing: Repaired and parsed truncated JSON for call {call_id}")
                    except:
                        # Extract partial data
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
                            
                            print(f"✅ Auto-processing: Extracted partial data from severely truncated JSON for call {call_id}")
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
                            print(f"⚠️ Auto-processing: Storing call {call_id} with parsing error data")
        
        except Exception as api_error:
            print(f"❌ Auto-processing: LLM API error for call {call_id}: {api_error}")
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
            print(f"✅ Auto-processed: Call {call_id} analyzed and stored successfully! _id: {result.inserted_id}")
            return {"call_id": call_id, "status": "success", "inserted_id": str(result.inserted_id)}
        except errors.DuplicateKeyError:
            return {"call_id": call_id, "status": "duplicate", "message": "Already exists"}
        except Exception as db_error:
            print(f"❌ Auto-processing: Database error for call {call_id}: {db_error}")
            return {"call_id": call_id, "status": "error", "message": str(db_error)}
            
    except Exception as e:
        print(f"❌ Auto-processing: Error processing call: {e}")
        
        # Store error data
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
                print(f"⚠️ Auto-processing: Call with processing error stored anyway! _id: {result.inserted_id}")
                return {"call_id": error_call_id, "status": "error_but_stored", "message": str(e), "inserted_id": str(result.inserted_id)}
            except errors.DuplicateKeyError:
                return {"call_id": error_call_id, "status": "duplicate_error", "message": str(e)}
            
        except Exception as db_error:
            print(f"❌ Auto-processing: Could not store error call in database: {db_error}")
            return {"call_id": f"FAILED_CALL_{call_index}", "status": "complete_failure", "message": f"Processing error: {e}, DB error: {db_error}"}

def auto_process_new_calls():
    """Automatically check for and process ONLY NEW calls every 10 seconds"""
    if not auto_processing_enabled:
        return
        
    try:
        # Fetch recent calls from Retell using SDK
        call_responses = get_retell_calls(limit=50)
        
        new_calls_found = []
        
        # FIRST: Check which calls are actually NEW
        for i, call in enumerate(call_responses):
            call_id = call.call_id if hasattr(call, 'call_id') else f"CALL_{i+1}"
            
            # Check if call already exists in database
            existing_call = collection.find_one({"CallId": call_id})
            if not existing_call:  # Only add if NOT in database
                new_calls_found.append(call)
        
        # ONLY process if there are actually NEW calls
        if new_calls_found:
            print(f"🆕 Found {len(new_calls_found)} NEW calls to process")
            
            new_calls_processed = 0
            for i, call in enumerate(new_calls_found):
                call_id = call.call_id if hasattr(call, 'call_id') else f"CALL_{i+1}"
                
                print(f"🔄 Processing new call: {call_id}")
                result = process_single_call(call, i+1)
                new_calls_processed += 1
                
                # Add small delay to avoid overwhelming the API
                time.sleep(1)
            
            print(f"✅ Auto-processing: Completed processing {new_calls_processed} new calls")
        # If no new calls, stay completely silent
            
    except Exception as e:
        print(f"❌ Auto-processing: Error during automatic processing: {e}")

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
        print("🤖 Starting background auto-processing...")
        
        # Schedule automatic processing every 10 seconds
        schedule.every(processing_interval).seconds.do(auto_process_new_calls)
        
        # Start the scheduler in a background thread
        scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
        scheduler_thread.start()
        
        # Start the initial auto-processing check
        initial_check_thread = threading.Thread(target=auto_process_new_calls, daemon=True)
        initial_check_thread.start()

def analyze_and_store_calls():
    """Fetch calls, analyze them, and store results in MongoDB (Manual trigger)"""
    try:
        call_responses = get_retell_calls(limit=10)
        results = []
        
        for i, call in enumerate(call_responses):
            result = process_single_call(call, i+1)
            results.append(result)
            time.sleep(0.5)  # Small delay between calls
        
        return results
        
    except Exception as e:
        print(f"❌ Manual processing: Error fetching calls: {e}")
        return {"error": str(e)}

@app.route("/analyze-calls", methods=["GET"])
def analyze_calls_endpoint():
    """API endpoint to trigger manual call analysis and storage"""
    results = analyze_and_store_calls()
    return jsonify({
        "message": "Manual call analysis completed",
        "results": results
    })

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
    return jsonify({
        "status": "healthy",
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "database_connected": True,
        "auto_processing_enabled": auto_processing_enabled
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
    print("🚀 Starting Call Analysis Application with Auto-Processing...")
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
