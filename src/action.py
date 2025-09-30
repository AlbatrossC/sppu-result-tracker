# actions.py
import extract
import parse
import database
from io import StringIO

def run_workflow():
    print("🔹 Starting centralized workflow...\n" + "="*50)

    # Step 1: Extract HTML
    html_content = extract.fetch_html()
    if not html_content:
        print("Failed to fetch HTML. Exiting workflow.")
        return

    # Step 2: Parse HTML to JSON
    json_data = parse.parse_html_content(html_content)
    print(f"✅ Parsed {len(json_data)} records from HTML")

    if not json_data:
        print("No valid data parsed. Exiting workflow.")
        return

    # Step 3: Sync with database
    database.sync_database(json_data)

    print("🔹 Workflow completed successfully!\n" + "="*50)


if __name__ == "__main__":
    run_workflow()
