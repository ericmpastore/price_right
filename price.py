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

    con.close()

def main():
    # --- Create tables in database ---
    get_from_db("price_history.csv","price_history")
    get_from_db("products.csv","products")
    get_from_db("transactions.csv","transactions")

    # --- Join tables on date ---
    con = duckdb.connect(os.path.join(os.path.dirname(os.path.abspath(__file__)), "my_database.duckdb"))

    # --- Query the database to determine the total revenue for all transactions
    print(con.sql(f"""
        SELECT
            FLOOR(SUM(t.quantity * ph.price)) AS total_revenue
        FROM transactions t
        JOIN products pr
            ON t.pizza_id = pr.pizza_id
        JOIN (
            SELECT
                pizza_id,
                effective_date,
                price,
                LEAD(effective_date) OVER (
                    PARTITION BY pizza_id
                    ORDER BY effective_date
                ) AS next_effective_date
            FROM price_history
        ) ph
            ON  t.pizza_id = ph.pizza_id
            AND t.order_date >= ph.effective_date
            AND (ph.next_effective_date IS NULL OR t.order_date < ph.next_effective_date)
                
              """))

if __name__ == "__main__":
    main()
