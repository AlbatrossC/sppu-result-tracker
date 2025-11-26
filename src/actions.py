import extract
import parse
import database

def run_workflow():
    print("Starting workflow...\n" + "="*60)

    html = extract.fetch_html()
    if not html:
        print("Failed to fetch HTML.")
        return
    print("Fetched HTML.")

    scraped = parse.parse_html_content(html)
    if not scraped:
        print("Parsed 0 items.")
        return
    print(f"Parsed {len(scraped)} records.")

    print("Syncing database...")
    database.sync_database(scraped)
    print("Database sync complete.")

    print("\nWorkflow finished.\n" + "="*60)

if __name__ == "__main__":
    run_workflow()
