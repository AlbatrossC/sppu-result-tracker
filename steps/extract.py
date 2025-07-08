#Script to fetch view-soruce code of the results website
import requests

url = "https://onlineresults.unipune.ac.in/Result/Dashboard/Default"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

response = requests.get(url, headers=headers, verify=False)

if response.status_code == 200:
    with open("sppu_result_page.html", "w", encoding="utf-8") as file:
        file.write(response.text)
    print("HTML source code saved as sppu_result_page.html")
else:
    print(f"Failed to fetch page. Status code: {response.status_code}")
