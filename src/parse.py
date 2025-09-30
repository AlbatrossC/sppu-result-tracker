from bs4 import BeautifulSoup
import json

def parse_html_content(html_content: str) -> list:
    """Parse HTML content and return JSON-like list of dicts."""
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
                print("Skipping row due to error:", e)
    return results

if __name__ == "__main__":
    # fallback to standalone mode
    with open("sppu_result_page.html", "r", encoding="utf-8") as f:
        html_content = f.read()
    data = parse_html_content(html_content)
    with open("sppu_subjects.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    print(f"Parsed {len(data)} records and saved to sppu_subjects.json")
