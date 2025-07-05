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
import pytz
import certifi

load_dotenv()
url = os.getenv("URL")
key = os.getenv("KEY")
supabase: Client = create_client(url, key)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

ist = pytz.timezone('Asia/Kolkata')
now = datetime.now(ist)
created_at = now.strftime("%H:%M %d %B %Y")
time_suffix = now.strftime("%H_%M")
file_name = f"sppu_result_{time_suffix}.html"
file_base_name = f"sppu_result_{time_suffix}"

target_url = "https://onlineresults.unipune.ac.in/Result/Dashboard/Default"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

session = requests.Session()
retry_strategy = Retry(
    total=3,
    backoff_factor=5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"]
)
adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
session.mount("https://", adapter)
session.mount("http://", adapter)

def fetch_html_online():
    try:
        logging.info("🌐 Fetching SPPU result page...")
        response = session.get(target_url, headers=headers, timeout=30, verify=False)
        response.raise_for_status()
        logging.info("✅ Successfully fetched HTML content.")
        return response.text
    except requests.exceptions.Timeout:
        logging.error("❌ Timeout: SPPU site did not respond.")
        raise
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Network error: {e}")
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

def upload_combined_data(file_name, html_body, json_data):
    try:
        logging.info("📤 Uploading combined HTML + JSON to Supabase...")
        supabase.table("sppu_results").insert({
            "file_name": file_name.replace(".html", ""),
            "created_at": created_at,
            "html_body": html_body,
            "json_text": json.dumps(json_data, ensure_ascii=False)
        }).execute()
        logging.info("✅ Combined data uploaded.")
    except Exception as e:
        logging.error(f"❌ Upload failed: {e}")
        raise

def main():
    try:
        html_content = fetch_html_online()
        parsed_data = parse_html(html_content)
        upload_combined_data(file_base_name, html_content, parsed_data)
        logging.info(f"📄 File created: {file_name}")
        logging.info("🎉 Job completed successfully.")
    except Exception as e:
        logging.exception("❌ Unhandled error occurred.")
        sys.exit(1)

if __name__ == "__main__":
    main()
