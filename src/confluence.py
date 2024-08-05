from urllib.parse import urlparse, parse_qs
from dotenv import dotenv_values
import pandas as pd
import requests


class ConfluenceWorker():
    def __init__(self) -> None:
        secrets: dict = dotenv_values(".env")

        self._base_url: str = secrets['confluence_url']
        self._api_token: str = secrets['confluence_api_token']

    def get_page_info(self, url):
        parsed_url = urlparse(url)
        page_id = parse_qs(parsed_url.query)['pageId'][0]
        search_url = f'{self._base_url}/rest/api/content/{page_id}?expand=body.storage,version'
        print(search_url)

        headers = {
            "Authorization": f"Bearer {self._api_token}",
            'Accept': 'application/json'
        }
        response = requests.get(search_url, headers=headers)
        if response.status_code == 200:
            results = response.json()
            if results:
                search_results = results
                results: dict = {
                    "page_id": page_id,
                    "page_version": search_results['version']["number"],
                    "page_title": search_results['title'],
                    "current_content": search_results['body']['storage']['value'],
                    "page_url": f'{self._base_url}/rest/api/content/{page_id}'
                }
                return results
            else:
                print("Page not found.")
        else:
            print(f"Error: {response.status_code}")
            print(response.text)
