import duckdb
import os

def get_from_db():
    print("Data fetched.")
#     # --- Configuration ---
#     SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
#     JSON_FILE = os.path.join(SCRIPT_DIR, "Toronto_Island_Ferry_Ticket_Counts.json")
#     TABLE_NAME = "ferry_ticket_counts"
#     DB_PATH = os.path.join(SCRIPT_DIR, "my_database.duckdb")

#     # --- Connect ---
#     con = duckdb.connect(DB_PATH)

#     # --- Read local JSON file into a table ---
#     con.sql(f"""
#         CREATE OR REPLACE TABLE {TABLE_NAME} AS
#         SELECT *
#         FROM read_json_auto('{JSON_FILE}');
#     """)

#     # --- Query the results ---
#     print(con.sql(f"SELECT count(*) AS row_count FROM {TABLE_NAME}").fetchone())
#     print(con.sql(f"SELECT * FROM {TABLE_NAME} LIMIT 5").df())

#     con.close()

def main():
    get_from_db()

if __name__ == "__main__":
    main()
