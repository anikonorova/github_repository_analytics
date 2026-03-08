"""
Single entry point for the extraction + load phase of the pipeline.

Execution order:
  1. Initialise the database (creates schemas and tables if they don't exist)
  2. Extract all entities from the GitHub API
  3. Load raw JSON payloads into DuckDB
"""

from dotenv import load_dotenv

from database.init_db import init_database
from extraction.extractors.pull_requests import fetch_pull_requests
from extraction.extractors.issues import fetch_issues
from extraction.extractors.commits import fetch_commits
from extraction.extractors.reviews import fetch_reviews
from extraction.loader import load_pull_requests, load_issues, load_commits, load_reviews

load_dotenv()

if __name__ == "__main__":
    print("--- Initialising database ---")
    init_database()

    print("\n--- Extracting ---")
    prs     = fetch_pull_requests()
    issues  = fetch_issues()
    commits = fetch_commits()
    # Reviews require PR numbers, so we pass the already-fetched PR list
    reviews = fetch_reviews(prs)

    print("\n--- Loading ---")
    load_pull_requests(prs)
    load_issues(issues)
    load_commits(commits)
    load_reviews(reviews)

    print("\nPipeline complete")
