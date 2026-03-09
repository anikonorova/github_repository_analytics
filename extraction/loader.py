import duckdb
import json
from datetime import datetime, timezone

DB_PATH = "/Users/aanikonorova/projects/github_repository_analytics/database/analytics.db"


def get_connection():
    return duckdb.connect(DB_PATH)


def load_pull_requests(pull_requests: list):
    conn = get_connection()
    extracted_at = datetime.now(timezone.utc)

    # Full refresh — delete before insert
    conn.execute("DELETE FROM raw_data.pull_requests")

    for pr in pull_requests:
        conn.execute("""
            INSERT INTO raw_data.pull_requests (id, payload, _extracted_at)
            VALUES (?, ?, ?)
        """, [str(pr["id"]), json.dumps(pr), extracted_at])

    print(f"Loaded {len(pull_requests)} pull requests")
    conn.close()


def load_issues(issues: list):
    conn = get_connection()
    extracted_at = datetime.now(timezone.utc)

    conn.execute("DELETE FROM raw_data.issues")

    for issue in issues:
        conn.execute("""
            INSERT INTO raw_data.issues (id, payload, _extracted_at)
            VALUES (?, ?, ?)
        """, [str(issue["id"]), json.dumps(issue), extracted_at])

    print(f"Loaded {len(issues)} issues")
    conn.close()


def load_commits(commits: list):
    conn = get_connection()
    extracted_at = datetime.now(timezone.utc)

    conn.execute("DELETE FROM raw_data.commits")

    for commit in commits:
        conn.execute("""
            INSERT INTO raw_data.commits (sha, payload, _extracted_at)
            VALUES (?, ?, ?)
        """, [commit["sha"], json.dumps(commit), extracted_at])

    print(f"Loaded {len(commits)} commits")
    conn.close()


def load_reviews(reviews: list):
    conn = get_connection()
    extracted_at = datetime.now(timezone.utc)

    conn.execute("DELETE FROM raw_data.reviews")

    for review in reviews:
        conn.execute("""
            INSERT INTO raw_data.reviews (id, pr_id, payload, _extracted_at)
            VALUES (?, ?, ?, ?)
        """, [str(review["id"]), str(review["pr_number"]), json.dumps(review), extracted_at])

    print(f"Loaded {len(reviews)} reviews")
    conn.close()
