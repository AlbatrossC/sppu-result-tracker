# comparison.py
import os, json, logging
from dotenv import load_dotenv
from supabase import create_client
from psycopg2 import pool

load_dotenv()
logging.basicConfig(level=logging.INFO)

supabase = create_client(os.getenv("URL"), os.getenv("KEY"))

db_pool = pool.SimpleConnectionPool(
    1, 5,
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
            rows = cur.fetchall()
            if len(rows) < 2:
                return None
            return [(json.loads(row[0]), row[1]) for row in rows]
    finally:
        db_pool.putconn(conn)

def compare(old, new):
    old_dict = {f"{d['course_name']}|{d['result_date']}": d for d in old}
    new_dict = {f"{d['course_name']}|{d['result_date']}": d for d in new}

    added = [new_dict[k] for k in new_dict.keys() - old_dict.keys()]
    removed = [old_dict[k] for k in old_dict.keys() - new_dict.keys()]
    unchanged = [new_dict[k] for k in old_dict.keys() & new_dict.keys()]
    return {"added": added, "removed": removed, "unchanged": unchanged}

def upload_comparison(result):
    supabase.table("sppu_comparison").insert({
        "json_file": json.dumps(result, ensure_ascii=False)
    }).execute()

def main():
    records = fetch_latest_two()
    if not records:
        logging.warning("⛔ Not enough data to compare. Run fetch.py again later.")
        return

    (new_data, new_ts), (old_data, _) = records
    comparison_result = {"created_at": str(new_ts), **compare(old_data, new_data)}

    with open("comparison_result.json", "w") as f:
        json.dump(comparison_result, f, indent=2)
    logging.info("💾 comparison_result.json saved")

    upload_comparison(comparison_result)
    logging.info("✅ Comparison uploaded")

if __name__ == "__main__":
    main()
