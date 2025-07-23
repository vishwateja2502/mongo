from flask import Flask, jsonify
from retell import Retell
from openai import OpenAI
from pymongo import MongoClient, errors
import json

app = Flask(__name__)

# Retell client for transcripts
retell_client = Retell(
    api_key="key_735ce7ee7176a4d8a1da3856db44",
)

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

Return ONLY this JSON structure with no other text besause im directly returing the JSON which you are giving as output.So please do not add any additional text or explanation. and give only in json:

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

def analyze_and_store_calls():
    """Fetch calls, analyze them, and store results in MongoDB"""
    try:
        # Fetch call transcripts from Retell
        call_responses = retell_client.call.list(limit=2)
        # call_responses=retell_client.call.list(limit=1000, pagination_key='call_c0919097a6f236485e0c0245138')
        
        results = []
        
        for i, call in enumerate(call_responses):
            try:
                transcript = call.transcript
                call_id = call.call_id if hasattr(call, 'call_id') else f"CALL_{i+1}"
                
                if not transcript or not transcript.strip():
                    print(f"Empty transcript for call #{i+1} - storing with null values")
                    
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
                        print(f"✅ Call #{i+1} ({call_id}) with empty transcript stored successfully! _id: {result.inserted_id}")
                        results.append({
                            "call_id": call_id,
                            "status": "success_empty_transcript",
                            "inserted_id": str(result.inserted_id)
                        })
                    except errors.DuplicateKeyError:
                        print(f"⚠️ Call {call_id} already exists in database, skipping...")
                        results.append({
                            "call_id": call_id,
                            "status": "duplicate",
                            "message": "Already exists"
                        })
                    except Exception as db_error:
                        print(f"❌ Database error for call {call_id}: {db_error}")
                        results.append({
                            "call_id": call_id,
                            "status": "error",
                            "message": str(db_error)
                        })
                    
                    continue
                
                # Build prompt and get analysis
                prompt = build_prompt(transcript)
                
                response = llm_client.chat.completions.create(
                    model="meta-llama/llama-3-8b-instruct",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.1,
                    max_tokens=2000  # Increased from 1000
                )
                
                # Parse the LLM response - handle both delimited and JSON formats
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
                            print(f"✅ Successfully parsed delimited format for call #{i+1}")
                        else:
                            print(f"❌ Not enough parts in delimited response for call #{i+1} (got {len(parts)}, need 8)")
                            continue
                    except Exception as e:
                        print(f"❌ Error parsing delimited format for call #{i+1}: {e}")
                        continue
                
                # Fallback to JSON parsing if no delimiters found
                else:
                    try:
                        # Try JSON parsing as fallback
                        analysis_data = json.loads(analysis_text)
                        print(f"✅ Parsed JSON format for call #{i+1}")
                    except json.JSONDecodeError:
                        # Try to repair truncated JSON
                        try:
                            # Add missing closing quotes and braces for common truncation patterns
                            fixed_text = analysis_text.strip()
                            
                            # Count quotes to see if we need to close a string
                            if fixed_text.count('"') % 2 == 1:  # Odd number of quotes
                                fixed_text += '"'
                            
                            # Add missing closing brace if needed
                            if not fixed_text.endswith('}'):
                                fixed_text += '}'
                            
                            # Try parsing the fixed JSON
                            analysis_data = json.loads(fixed_text)
                            print(f"✅ Repaired and parsed truncated JSON for call #{i+1}")
                            
                        except:
                            # More aggressive repair for severely truncated JSON
                            try:
                                # Extract what we can from the partial JSON
                                import re
                                
                                # Try to extract individual fields even from broken JSON
                                sentiment_match = re.search(r'"sentiment":\s*"([^"]*)"', analysis_text)
                                emotion_match = re.search(r'"customer_emotion_journey":\s*"([^"]*)"', analysis_text)
                                topic_match = re.search(r'"topic_identification":\s*"([^"]*)"', analysis_text)
                                intent_match = re.search(r'"primary_call_intent":\s*"([^"]*)"', analysis_text)
                                transfer_match = re.search(r'"transfer_reason":\s*"([^"]*)"', analysis_text)
                                competitors_match = re.search(r'"competitors_mentioned":\s*"([^"]*)"', analysis_text)
                                themes_match = re.search(r'"key_themes_identified":\s*"([^"]*)"', analysis_text)
                                context_match = re.search(r'"overall_context":\s*"([^"]*)"', analysis_text)
                                
                                # Build partial data from what we found
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
                                
                                print(f"✅ Extracted partial data from severely truncated JSON for call #{i+1}")
                                
                            except:
                                print(f"❌ Failed to parse both delimited and JSON for call #{i+1}")
                                print(f"Raw response: {analysis_text[:300]}...")
                                
                                # MUST UPLOAD - Store with error message in all fields
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
                                print(f"⚠️ Storing call #{i+1} with parsing error data - NO DATA WILL BE LOST")
                
                # Prepare document for MongoDB (without transcript)
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
                    print(f"✅ Call #{i+1} ({call_id}) analyzed and stored successfully! _id: {result.inserted_id}")
                    results.append({
                        "call_id": call_id,
                        "status": "success",
                        "inserted_id": str(result.inserted_id)
                    })
                except errors.DuplicateKeyError:
                    print(f"⚠️ Call {call_id} already exists in database, skipping...")
                    results.append({
                        "call_id": call_id,
                        "status": "duplicate",
                        "message": "Already exists"
                    })
                except Exception as db_error:
                    print(f"❌ Database error for call {call_id}: {db_error}")
                    results.append({
                        "call_id": call_id,
                        "status": "error",
                        "message": str(db_error)
                    })
                    
            except Exception as e:
                print(f"❌ Error processing call #{i+1}: {e}")
                
                # MUST UPLOAD - Store with error message even if processing fails
                try:
                    error_call_id = call.call_id if hasattr(call, 'call_id') else f"ERROR_CALL_{i+1}"
                    
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
                    
                    result = collection.insert_one(document)
                    print(f"⚠️ Call #{i+1} with processing error stored anyway! _id: {result.inserted_id}")
                    results.append({
                        "call_id": error_call_id,
                        "status": "error_but_stored",
                        "message": str(e),
                        "inserted_id": str(result.inserted_id)
                    })
                    
                except Exception as db_error:
                    print(f"❌ Could not store error call #{i+1} in database: {db_error}")
                    results.append({
                        "call_id": f"FAILED_CALL_{i+1}",
                        "status": "complete_failure",
                        "message": f"Processing error: {e}, DB error: {db_error}"
                    })
        
        return results
        
    except Exception as e:
        print(f"❌ Error fetching calls: {e}")
        return {"error": str(e)}

@app.route("/analyze-calls", methods=["GET"])
def analyze_calls_endpoint():
    """API endpoint to trigger call analysis and storage"""
    results = analyze_and_store_calls()
    return jsonify({
        "message": "Call analysis completed",
        "results": results
    })



@app.route("/get-analysis/<call_id>", methods=["GET"])
def get_analysis(call_id):
    """Get analysis for a specific call"""
    try:
        document = collection.find_one({"CallId": call_id})
        if document:
            # Convert ObjectId to string for JSON serialization
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
        # Convert ObjectIds to strings for JSON serialization
        for doc in documents:
            doc['_id'] = str(doc['_id'])
        return jsonify(documents)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/", methods=["GET"])
def home():
    """Home endpoint with available routes"""
    return jsonify({
        "message": "Call Analysis API",
        "available_endpoints": {
            "/analyze-calls": "Fetch calls from Retell, analyze with LLM, and store in MongoDB",
            "/get-analysis/<call_id>": "Get analysis for specific call",
            "/get-all-analysis": "Get all call analyses"
        }
    })

if __name__ == "__main__":
    print("🚀 Starting Call Analysis Application...")
    print("📋 Available endpoints:")
    print("  GET /analyze-calls - Analyze and store calls")
    print("  GET /get-analysis/<call_id> - Get specific call analysis")
    print("  GET /get-all-analysis - Get all analyses")
    
    app.run(debug=True)