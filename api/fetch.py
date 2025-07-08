import os
import sys
import json
import logging
import requests
import pytz
from datetime import datetime
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from psycopg2 import pool
from supabase import create_client, Client
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ========== ENV & LOGGING SETUP ==========
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

# ========== TIME SETUP ==========
ist = pytz.timezone('Asia/Kolkata')
now = datetime.now(ist)
created_at = now.strftime("%H:%M %d %B %Y")
time_suffix = now.strftime("%H_%M")
file_name = f"sppu_result_{time_suffix}.html"
file_base_name = f"sppu_result_{time_suffix}"

# ========== SUPABASE & DB SETUP ==========
supabase: Client = create_client(os.getenv("URL"), os.getenv("KEY"))

db_pool = pool.SimpleConnectionPool(
    minconn=1,
    maxconn=5,
    host=os.getenv('DB_HOST'),
    database=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASS'),
    port=5432
)

# ========== FETCH & PARSE HTML ==========
target_url = "https://onlineresults.unipune.ac.in/Result/Dashboard/Default"
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

session = requests.Session()
retry_strategy = Retry(
    total=3,
    backoff_factor=5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)

def fetch_html_online():
    try:
        logging.info("🌐 Fetching SPPU result page...")
        response = session.get(target_url, headers=headers, timeout=30, verify=False)
        response.raise_for_status()
        logging.info("✅ Successfully fetched HTML content.")
        return response.text
    except Exception as e:
        logging.error(f"❌ Error fetching HTML: {e}")
        raise

def parse_html(html_content):
    logging.info("🔍 Parsing HTML content...")
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
                logging.warning(f"⚠️ Skipped row due to error: {e}")

    logging.info(f"✅ Parsed {len(results)} entries.")
    return results

# ========== UPLOAD TO SUPABASE ==========
def upload_combined_data(file_name, html_body, json_data):
    try:
        logging.info("📤 Uploading HTML + JSON to sppu_results...")
        supabase.table("sppu_results").insert({
            "file_name": file_name,
            "created_at": created_at,
            "html_body": html_body,
            "json_text": json.dumps(json_data, ensure_ascii=False)
        }).execute()
        logging.info("✅ sppu_results upload complete.")
    except Exception as e:
        logging.error(f"❌ Upload to sppu_results failed: {e}")
        raise

def upload_comparison_result_to_supabase(data):
    try:
        logging.info("📤 Uploading comparison result to sppu_comparison...")
        supabase.table("sppu_comparison").insert({
            "json_file": json.dumps(data, ensure_ascii=False)
        }).execute()
        logging.info("✅ sppu_comparison upload complete.")
    except Exception as e:
        logging.error(f"❌ Upload to sppu_comparison failed: {e}")
        raise

# ========== COMPARISON LOGIC ==========
def fetch_latest_two():
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT json_text, created_at FROM sppu_results ORDER BY created_at DESC LIMIT 2;")
            results = cur.fetchall()
            output = []
            for row in results:
                json_data = row[0]
                if isinstance(json_data, str):
                    json_data = json.loads(json_data)
                output.append((json_data, row[1]))
            return output
    finally:
        db_pool.putconn(conn)

def to_composite_dict(data):
    return {
        f"{item['course_name'].strip()}|{item['result_date'].strip()}": item
        for item in data if 'course_name' in item and 'result_date' in item
    }

def compare(old_dict, new_dict):
    old_keys = set(old_dict.keys())
    new_keys = set(new_dict.keys())

    return {
        "added": [new_dict[k] for k in new_keys - old_keys],
        "removed": [old_dict[k] for k in old_keys - new_keys],
        "unchanged": [new_dict[k] for k in old_keys & new_keys]
    }

def save_to_json(data, filename):
    try:
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        logging.info(f"💾 Comparison saved to {filename}")
    except Exception as e:
        logging.error(f"❌ Error saving JSON file: {e}")

# ========== MAIN ==========
def main():
    try:
        # Step 1: Fetch and upload results
        html_content = fetch_html_online()
        parsed_data = parse_html(html_content)
        upload_combined_data(file_base_name, html_content, parsed_data)

        # Step 2: Fetch last two entries
        records = fetch_latest_two()
        if len(records) < 2:
            logging.warning("⚠️ Not enough records to compare.")
            return

        (new_data, new_ts), (old_data, old_ts) = records
        if not isinstance(new_data, list) or not isinstance(old_data, list):
            logging.warning("⚠️ Invalid data format for comparison.")
            return

        # Step 3: Compare and save
        comparison = compare(to_composite_dict(old_data), to_composite_dict(new_data))
        final_result = {"created_at": str(new_ts), **comparison}
        save_to_json(final_result, "comparison_result.json")
        upload_comparison_result_to_supabase(final_result)

        logging.info("🎉 Job completed successfully.")
    except Exception as e:
        logging.exception("❌ Script failed with exception.")

if __name__ == "__main__":
    main()
