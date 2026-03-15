"""Initialize the DuckDB database and create the raw tables from `init.sql`."""

import duckdb

def init_database():
    conn = duckdb.connect("./database/analytics.db")

    with open("database/init.sql", "r") as f:
        sql = f.read()

    conn.execute(sql)
    print("Database initialised")

    # Check to see if tables were created
    conn.sql("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema = 'raw_data'
    """).show()

    conn.close()

if __name__ == "__main__":
    init_database()