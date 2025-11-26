import extract
import parse
import database
import traceback
from datetime import datetime

def run_workflow():
    print("\n==================== SPPU Result Monitor ====================")
    print(f"[WF] Workflow start: {datetime.now()}\n")

    try:
        print("[WF] Fetching HTML...")
        html = extract.fetch_html()
        if not html:
            print("[WF] ERROR: HTML fetch failed.")
            return
        print("[WF] HTML fetched.")

        print("[WF] Parsing HTML...")
        scraped = parse.parse_html_content(html)
        if not scraped:
            print("[WF] WARNING: Parsed 0 valid items. Aborting.")
            return
        print(f"[WF] Parsed {len(scraped)} items.")

        print("[WF] Syncing database...")
        database.sync_database(scraped)
        print("[WF] Database sync done.")

    except Exception as e:
        print("\n[WF] FATAL ERROR in workflow:", e)
        print(traceback.format_exc())

    print(f"\n[WF] Workflow finish: {datetime.now()}")
    print("=============================================================\n")


if __name__ == "__main__":
    run_workflow()
