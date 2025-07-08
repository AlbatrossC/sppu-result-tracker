import os
import json
from datetime import datetime
from psycopg2 import pool
from dotenv import load_dotenv

load_dotenv()

# Setup PostgreSQL connection pool
db_pool = pool.SimpleConnectionPool(
    minconn=1,
    maxconn=5,
    host=os.getenv('DB_HOST'),
    database=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASS'),
    port=5432
)

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
    """Convert list of objects to dict with composite key"""
    return {
        f"{item['course_name'].strip()}|{item['result_date'].strip()}": item
        for item in data if 'course_name' in item and 'result_date' in item
    }

def compare(old_dict, new_dict):
    old_keys = set(old_dict.keys())
    new_keys = set(new_dict.keys())

    added_keys = new_keys - old_keys
    removed_keys = old_keys - new_keys
    unchanged_keys = old_keys & new_keys

    return {
        "added": [new_dict[k] for k in added_keys],
        "removed": [old_dict[k] for k in removed_keys],
        "unchanged": [new_dict[k] for k in unchanged_keys]
    }

def save_to_json(data, filename):
    try:
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        print(f"Output saved to {filename}")
    except Exception as e:
        print(f"Error saving to file: {str(e)}")

def main():
    try:
        records = fetch_latest_two()

        if len(records) < 2:
            result = {"error": "Not enough records to compare"}
            print(json.dumps(result, indent=2))
            save_to_json(result, "comparison_result.json")
            return

        (new_data, new_ts), (old_data, old_ts) = records

        if not isinstance(new_data, list) or not isinstance(old_data, list):
            result = {"error": "Invalid JSON structure"}
            print(json.dumps(result, indent=2))
            save_to_json(result, "comparison_result.json")
            return

        new_dict = to_composite_dict(new_data)
        old_dict = to_composite_dict(old_data)

        comparison = compare(old_dict, new_dict)

        final_result = {
            "created_at": str(new_ts),
            **comparison
        }

        print(json.dumps(final_result, indent=2))
        save_to_json(final_result, "comparison_result.json")

    except Exception as e:
        error_result = {"error": str(e)}
        print(json.dumps(error_result, indent=2))
        save_to_json(error_result, "comparison_result.json")

if __name__ == "__main__":
    main()
