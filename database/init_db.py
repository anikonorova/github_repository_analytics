"""
Creates the DuckDB database and initialises the raw_data schema + tables by executing init.sql.
"""

import duckdb


def init_database():
    conn = duckdb.connect("database/analytics.db")

    with open("database/init.sql", "r") as f:
        sql = f.read()

    conn.execute(sql)
    print("Database initialised")

    # Verify tables were created
    conn.sql("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema = 'raw_data'
    """).show()

    conn.close()

if __name__ == "__main__":
    init_database()