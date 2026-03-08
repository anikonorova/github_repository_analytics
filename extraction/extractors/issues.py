from extraction.github_client import GitHubClient
from extraction.config import SINCE_DATE


def fetch_issues() -> list:
    client = GitHubClient()
    print("Fetching issues...")

    raw = client.get_paginated(
        "issues",
        params={
            "state": "all",
            "since": SINCE_DATE,
        }
    )

    # Exclude pull requests — they appear in the /issues response too
    issues = [item for item in raw if "pull_request" not in item]

    print(f"Fetched {len(issues)} issues (filtered {len(raw) - len(issues)} PRs)")
    return issues
