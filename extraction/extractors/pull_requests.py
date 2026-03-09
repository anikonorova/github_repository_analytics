from extraction.github_client import GitHubClient
from extraction.config import SINCE_DATE


def fetch_pull_requests() -> list:
    client = GitHubClient()
    print("Fetching pull requests...")

    all_prs = []
    params = {
        "state": "all",
        "sort": "updated",       # sort by last-updated date
        "direction": "desc",     # most recently updated first
        "per_page": 100,
    }
    page = 1

    while True:
        params["page"] = page
        data = client.get("pulls", params)

        if not data:
            break

        # Stop once we reach PRs older than the cutoff
        last_updated = data[-1].get("updated_at", "")
        if last_updated and last_updated < SINCE_DATE:
            # Include items on this page that are still within range
            data = [pr for pr in data if pr.get("updated_at", "") >= SINCE_DATE]
            all_prs.extend(data)
            break

        all_prs.extend(data)
        print(f"Fetched page {page} ({len(data)} items)")

        if len(data) < 100:
            break

        page += 1

    print(f"Fetched {len(all_prs)} pull requests")
    return all_prs

detail_fields = [
    "additions", "deletions", "changed_files",
    "mergeable", "mergeable_state", "auto_merge",
    "commits", "comments", "review_comments",
]


def fetch_pull_requests_with_stats(pull_requests: list) -> list:
    client = GitHubClient()
    print(f"Fetching stats for {len(pull_requests)} PRs...")

    for i, pr in enumerate(pull_requests):
        detail = client.get(f"pulls/{pr['number']}")
        pr.update({k: detail[k] for k in detail_fields if k in detail})

        if (i + 1) % 50 == 0:
            print(f"Processed {i + 1}/{len(pull_requests)} PRs")

    print(f"Enriched {len(pull_requests)} PRs")
    return pull_requests