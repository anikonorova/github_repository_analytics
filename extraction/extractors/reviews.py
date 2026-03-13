from extraction.github_client import GitHubClient

def fetch_reviews(pull_requests: list) -> list:
    client = GitHubClient()
    print(f"Fetching reviews for {len(pull_requests)} PRs...")

    all_reviews = []

    for i, pr in enumerate(pull_requests):
        pr_number = pr["number"]
        reviews = client.get(f"pulls/{pr_number}/reviews")

        for review in reviews:
            review["pr_number"] = pr_number

        all_reviews.extend(reviews)

        if (i + 1) % 50 == 0:
            print(f"  Processed {i + 1}/{len(pull_requests)} PRs")

    print(f"Fetched {len(all_reviews)} reviews")
    return all_reviews