import duckdb

conn = duckdb.connect("database/analytics.db")

with open("database/init.sql", "r") as f:
    sql = f.read()

conn.execute(sql)
print("Database initialized")

tables = conn.execute("""
    SELECT table_schema, table_name 
    FROM information_schema.tables
    WHERE table_schema = 'raw_data'
""").fetchall()

print("\nCreated tables:")
for schema, table in tables:
    print(f"  - {schema}.{table}")

conn.close()