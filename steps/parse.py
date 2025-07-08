#Script to parse the html file recieved from extract.py source code. It outputs a json file

from bs4 import BeautifulSoup
import json

with open("sppu_result_page.html", "r", encoding="utf-8") as file:
    html_content = file.read()

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

with open("sppu_subjects.json", "w", encoding="utf-8") as json_file:
    json.dump(results, json_file, indent=4, ensure_ascii=False)

print(f"✅ Extracted {len(results)} subjects and saved to sppu_subjects.json")
