"""
Single entry point for the extraction + load phase of the pipeline.

Execution order:
  1. Initialise the database (creates schemas and tables if they don't exist)
  2. Extract all entities from the GitHub API
  3. Load raw JSON payloads into DuckDB
"""

from dotenv import load_dotenv

from database.init_db import init_database
from extraction.extractors.pull_requests import fetch_pull_requests, fetch_pull_requests_with_stats
from extraction.extractors.issues import fetch_issues
from extraction.extractors.commits import fetch_commits
from extraction.extractors.reviews import fetch_reviews
from extraction.loader import load_pull_requests, load_issues, load_commits, load_reviews

load_dotenv()

if __name__ == "__main__":
    print("--- Initialising database ---")
    init_database()

    print("\n--- Pull Requests ---")
    prs = fetch_pull_requests()
    prs = fetch_pull_requests_with_stats(prs)
    load_pull_requests(prs)

    print("\n--- Issues ---")
    issues = fetch_issues()
    load_issues(issues)

    print("\n--- Commits ---")
    commits = fetch_commits()
    load_commits(commits)

    print("\n--- Reviews ---")
    reviews = fetch_reviews(prs)
    load_reviews(reviews)

    print("\nPipeline complete")