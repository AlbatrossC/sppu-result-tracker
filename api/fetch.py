# fetch.py
import os, sys, json, logging, requests, pytz
from datetime import datetime
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from supabase import create_client, Client

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Time and file setup
ist = pytz.timezone('Asia/Kolkata')
now = datetime.now(ist)
created_at = now.strftime("%H:%M %d %B %Y")
file_base_name = f"sppu_result_{now.strftime('%H_%M')}"

# Supabase client
supabase: Client = create_client(os.getenv("URL"), os.getenv("KEY"))

# Target
target_url = "https://onlineresults.unipune.ac.in/Result/Dashboard/Default"
headers = {"User-Agent": "Mozilla/5.0"}

session = requests.Session()
retry = Retry(total=3, backoff_factor=5, status_forcelist=[429, 500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry)
session.mount("https://", adapter)
session.mount("http://", adapter)

def fetch_html():
    res = session.get(target_url, headers=headers, timeout=30, verify=False)
    res.raise_for_status()
    return res.text

def parse_html(html):
    soup = BeautifulSoup(html, "html.parser")
    results = []
    for row in soup.find_all("tr"):
        tds = row.find_all("td")
        if len(tds) == 4:
            course = tds[1].get_text(strip=True)
            date = tds[2].get_text(strip=True)
            results.append({"course_name": course, "result_date": date})
    return results

def upload(html_body, json_data):
    supabase.table("sppu_results").insert({
        "file_name": file_base_name,
        "created_at": created_at,
        "html_body": html_body,
        "json_text": json.dumps(json_data, ensure_ascii=False)
    }).execute()

def main():
    try:
        html = fetch_html()
        data = parse_html(html)
        upload(html, data)
        logging.info("✅ Data uploaded to sppu_results")
    except Exception as e:
        logging.error(f"❌ Fetch error: {e}")

if __name__ == "__main__":
    main()
