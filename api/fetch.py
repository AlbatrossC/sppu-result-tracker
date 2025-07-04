import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from supabase import create_client, Client
from datetime import datetime
from dotenv import load_dotenv
import os
import json
import logging
import sys
import pytz  # for IST support

# 🛠️ Load environment variables
load_dotenv()
url = os.getenv("URL")
key = os.getenv("KEY")
supabase: Client = create_client(url, key)

# 🔧 Logging Configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# 🌐 SPPU Result Page URL
target_url = "https://onlineresults.unipune.ac.in/Result/Dashboard/Default"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

# 🕒 Get IST time for filename and created_at
ist = pytz.timezone('Asia/Kolkata')
now = datetime.now(ist)
created_at = now.strftime("%H:%M %d %B %Y")
time_suffix = now.strftime("%H_%M")
file_name = f"sppu_result_{time_suffix}.html"

# 🧠 Set up Retry Strategy
session = requests.Session()
retry_strategy = Retry(
    total=3,
    backoff_factor=5,  # exponential backoff: 5s, 10s, 20s
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)

def fetch_html_online():
    logging.info("🌐 Fetching SPPU result page...")
    if target_url.startswith("https://") and "verify=False" in str(session.get):
        logging.warning("⚠️ SSL verification disabled. Consider enabling it in production.")

    try:
        response = session.get(target_url, headers=headers, timeout=30, verify=False)
        if response.status_code == 200:
            logging.info("✅ Successfully fetched HTML content.")
            return response.text
        else:
            logging.error(f"❌ HTTP {response.status_code} from SPPU site.")
            sys.exit(1)
    except requests.exceptions.Timeout:
        logging.error("❌ Timeout: SPPU site did not respond within 30 seconds.")
        sys.exit(1)
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Network error while fetching SPPU site: {e}")
        sys.exit(1)

def upload_html_to_db(file_name, html_body):
    try:
        logging.info("📤 Uploading HTML to 'html_files' table...")
        supabase.table("html_files").insert({
            "file_name": file_name,
            "created_at": created_at,
            "html_body": html_body
        }).execute()
        logging.info("✅ HTML uploaded.")
    except Exception as e:
        logging.error(f"❌ Failed to upload HTML to Supabase: {e}")
        sys.exit(1)

def fetch_html_from_db(file_name):
    try:
        logging.info(f"🔍 Fetching HTML from Supabase for file: {file_name}")
        response = supabase.table("html_files").select("*").eq("file_name", file_name).execute()
        records = response.data
        if not records:
            logging.error("❌ No HTML file found in database.")
            sys.exit(1)
        return records[0]["html_body"]
    except Exception as e:
        logging.error(f"❌ Supabase fetch error: {e}")
        sys.exit(1)

def parse_html(html_content):
    logging.info("🔎 Parsing HTML for result entries...")
    soup = BeautifulSoup(html_content, "html.parser")
    results = []

    for row in soup.find_all("tr"):
        tds = row.find_all("td")
        if len(tds) == 4:
            try:
                course = tds[1].get_text(strip=True)
                date = tds[2].get_text(strip=True)
                results.append({
                    "course_name": course,
                    "result_date": date
                })
            except Exception as e:
                logging.warning(f"⚠️ Skipping malformed row: {e}")

    logging.info(f"✅ Parsed {len(results)} entries.")
    return results

def upload_json_to_db(file_name, json_data):
    try:
        json_file_name = file_name.replace(".html", ".json")
        logging.info("📤 Uploading parsed JSON to 'json_files' table...")
        supabase.table("json_files").insert({
            "file_name": json_file_name,
            "created_at": created_at,
            "json_text": json.dumps(json_data, ensure_ascii=False)
        }).execute()
        logging.info("✅ JSON uploaded.")
    except Exception as e:
        logging.error(f"❌ Failed to upload JSON to Supabase: {e}")
        sys.exit(1)

def main():
    try:
        html_content = fetch_html_online()
        upload_html_to_db(file_name, html_content)
        html_from_db = fetch_html_from_db(file_name)
        parsed_data = parse_html(html_from_db)
        upload_json_to_db(file_name, parsed_data)
        logging.info("🎉 All steps completed successfully.")
    except Exception as e:
        logging.exception(f"❌ Unhandled exception: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
