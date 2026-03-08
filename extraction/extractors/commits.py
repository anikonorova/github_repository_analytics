from extraction.github_client import GitHubClient
from extraction.config import SINCE_DATE


def fetch_commits() -> list:
    client = GitHubClient()
    print("Fetching commits...")

    commits = client.get_paginated(
        "commits",
        params={"since": SINCE_DATE}
    )

    print(f"Fetched {len(commits)} commits")
    return commits
