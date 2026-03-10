import requests
import os
import time
from dotenv import load_dotenv

load_dotenv()


class GitHubClient:

    def __init__(self):
        self.token = os.getenv("GITHUB_TOKEN")
        self.owner = os.getenv("REPO_OWNER")
        self.repo = os.getenv("REPO_NAME")
        self.base_url = f"https://api.github.com/repos/{self.owner}/{self.repo}"
        self.headers = {
            "Authorization": f"token {self.token}",
            "Accept": "application/vnd.github.v3+json"
        }

    def get(self, resource, params=None):
        url = f"{self.base_url}/{resource}"
        while True:  # keep retrying until success or a non-rate-limit error
            try:
                response = requests.get(url, headers=self.headers, params=params)
            except requests.exceptions.ConnectionError as e:
                # GitHub closed the connection without responding — usually means we're sending too fast
                print(f"Connection error: {e}. Waiting 60s before retry...")
                time.sleep(60)
                continue  # retry the same request

            if response.status_code == 403:
                msg = response.json().get("message", "")
                if "rate limit" in msg.lower():  # only auto-retry for rate-limit 403s
                    reset_ts = int(response.headers.get("X-RateLimit-Reset", 0))  # unix timestamp when limit resets
                    wait = max(reset_ts - int(time.time()), 0) + 5  # seconds to wait, +5s buffer
                    print(f"Rate limited. Waiting {wait}s until reset...")
                    time.sleep(wait)
                    continue  # retry the same request

            if not response.ok:  # any other error: print and raise as before
                print(f"Error {response.status_code}: {response.url}")
                print(f"{response.json().get('message', 'Unknown error')}")
                response.raise_for_status()

            return response.json()

    def get_paginated(self, resource, params=None):
        if params is None:
            params = {}

        params["per_page"] = 100  # max allowed by GitHub API
        page = 1
        all_results = []

        while True:
            params["page"] = page
            data = self.get(resource, params)

            if not data:
                break

            all_results.extend(data)
            print(f"Fetched page {page} ({len(data)} items)")

            if len(data) < 100:
                # Last page — fewer results than the page size
                break

            page += 1

        print(f"Total: {len(all_results)} items fetched")
        return all_results