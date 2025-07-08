from flask import Flask, request, jsonify, render_template,send_from_directory
import requests
import os
from dotenv import load_dotenv
import json
from supabase import create_client, Client
from datetime import datetime

load_dotenv()

app = Flask(__name__)

SUPABASE_URL = os.getenv("URL")
SUPABASE_KEY = os.getenv("KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


SECRET_KEY = os.getenv("TRIGGER_SECRET")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
REPO_NAME = "AlbatrossC/sppu-result-tracker"
WORKFLOW_FILE = "fetch.yml"
REF_BRANCH = "main"



@app.route('/')
def index():
    try:
        # Fetch the latest comparison data
        response = supabase.table("sppu_comparison").select("json_file").execute()
        rows = response.data

        if not rows:
            return "No data found in sppu_comparison table.", 404

        latest_row = rows[-1]
        json_raw = latest_row.get("json_file")
        result_data = json.loads(json_raw) if isinstance(json_raw, str) else json_raw

        # Convert created_at format to desired output
        created_at_raw = result_data.get("created_at", "N/A")
        formatted_created_at = "N/A"

        try:
            if created_at_raw != "N/A":
                # Parse original format
                dt_obj = datetime.strptime(created_at_raw, "%H:%M %d %B %Y")
                # Format to '10:08 PM 08 July'
                formatted_created_at = dt_obj.strftime("%I:%M %p %d %B").lstrip('0')
        except Exception as e:
            print(f"Error parsing created_at: {e}")
            formatted_created_at = created_at_raw  # fallback to original

        # Check if there are new subjects added
        added_results = result_data.get("added", [])
        
        # If no new subjects are added, fetch latest results from sppu_results table
        latest_results = []
        if not added_results:  # If added array is empty
            try:
                # Fetch the latest entry from sppu_results table
                results_response = supabase.table("sppu_results").select("json_text").order("created_at", desc=True).limit(1).execute()
                
                if results_response.data:
                    latest_results_row = results_response.data[0]
                    json_text = latest_results_row.get("json_text")
                    
                    if json_text:
                        # Parse the JSON text
                        results_data = json.loads(json_text) if isinstance(json_text, str) else json_text
                        
                        # Get first 4 results
                        if isinstance(results_data, list):
                            latest_results = results_data[:4]
                        else:
                            latest_results = []
                            
            except Exception as e:
                print(f"Error fetching latest results: {e}")
                latest_results = []

        return render_template(
            "index.html",
            created_at=formatted_created_at,
            added=added_results,
            removed=result_data.get("removed", []),
            unchanged=result_data.get("unchanged", []),
            latest_results=latest_results
        )

    except Exception as e:
        return f"An error occurred: {e}", 500
    
    
@app.route("/api/trigger", methods=["POST"])
def serve_trigger_script():
    data = request.get_json()
    if not data or data.get("key") != SECRET_KEY:
        return jsonify({"error": "Unauthorized"}), 401

    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json"
    }

    payload = {
        "ref": REF_BRANCH
    }

    url = f"https://api.github.com/repos/{REPO_NAME}/actions/workflows/{WORKFLOW_FILE}/dispatches"
    response = requests.post(url, headers=headers, json=payload)

    if response.status_code == 204:
        return jsonify({"message": "GitHub Actions triggered!"})
    else:
        return jsonify({
            "error": "Failed to trigger workflow",
            "status_code": response.status_code,
            "details": response.text
        }), response.status_code
    
@app.route('/robots.txt')
def robots():
    return send_from_directory('.', 'robots.txt')

if __name__ == "__main__":
    app.run(debug=True)
