import requests
import os
import time
from dotenv import load_dotenv
from extraction.config import REPO_OWNER, REPO_NAME

load_dotenv()

class GitHubClient:
    def __init__(self):
        self.token = os.getenv("GITHUB_TOKEN")
        self.owner = REPO_OWNER
        self.repo = REPO_NAME
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
                # If github closed the connection without responding usually it means
                # we're sending too fast so we need sleep for a while
                print(f"Connection error: {e}. Waiting 60s before retry...")
                time.sleep(60)
                continue  # retry the same request

            # Overcomming RateLimitter exceptions
            if response.status_code == 403:
                msg = response.json().get("message", "")
                # naive check that it's rate limit exception 
                if "rate limit" in msg.lower(): 
                    reset_ts = int(response.headers.get("X-RateLimit-Reset", 0))
                    wait = max(reset_ts - int(time.time()), 0) + 5
                    print(f"Rate limited. Waiting {wait}s until reset...")
                    time.sleep(wait)
                    # retry again after reset_ts time
                    continue  

            #  print all other errors in log
            if not response.ok:
                print(f"Error {response.status_code}: {response.url}")
                print(f"{response.json().get('message', 'Unknown error')}")
                response.raise_for_status()

            return response.json()

    def get_paginated(self, resource, params=None):
        if params is None:
            params = {}

        # 100 is GitHub API limitation
        params["per_page"] = 100 
        page = 1
        all_results = []

        while True:
            params["page"] = page
            data = self.get(resource, params)

            if not data:
                break

            all_results.extend(data)
            print(f"Fetched page {page} ({len(data)} items)")

            # Last page
            if len(data) < 100:
                break

            page += 1

        print(f"Total: {len(all_results)} items fetched")
        return all_results