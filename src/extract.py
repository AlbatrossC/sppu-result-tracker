import requests

def fetch_html() -> str:
    """Fetch HTML from the results page and return as string."""
    url = "https://onlineresults.unipune.ac.in/Result/Dashboard/Default"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }

    try:
        response = requests.get(url, headers=headers, verify=False)
        if response.status_code == 200:
            print("✅ HTML fetched successfully")
            return response.text
        else:
            print(f"Failed to fetch page. Status code: {response.status_code}")
            return ""
    except Exception as e:
        print(f"Error fetching HTML: {e}")
        return ""

if __name__ == "__main__":
    # fallback to save file if running standalone
    html_content = fetch_html()
    if html_content:
        with open("sppu_result_page.html", "w", encoding="utf-8") as f:
            f.write(html_content)
        print("HTML saved as sppu_result_page.html")
