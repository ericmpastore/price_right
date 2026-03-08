import duckdb
import os

def get_from_db(in_file,table_name):
  # --- Configuration ---
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    CSV_FILE_1 = os.path.join(SCRIPT_DIR, in_file)
    TABLE_NAME = table_name
    DB_PATH = os.path.join(SCRIPT_DIR, "my_database.duckdb")

    # --- Connect ---
    con = duckdb.connect(DB_PATH)

    # --- Read local CSV file into a table ---
    con.sql(f"""
        CREATE OR REPLACE TABLE {TABLE_NAME} AS
        SELECT *
        FROM read_csv('{CSV_FILE_1}');
    """)

    # --- Query the results ---
    # print(con.sql(f"SELECT count(*) AS row_count FROM {TABLE_NAME}").fetchone())
    # print(con.sql(f"SELECT * FROM {TABLE_NAME} LIMIT 5").df())

    con.close()

def main():
    get_from_db("price_history.csv","price_history")

if __name__ == "__main__":
    main()
