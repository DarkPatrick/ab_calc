from urllib.parse import urlparse, parse_qs
from dotenv import dotenv_values
import requests
import re
import json


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

    def get_page_info_by_title(self, space_key, page_title):
        api_url = f"{self._base_url}/rest/api/content?spaceKey={space_key}&title={page_title.replace(' ', '+')}&expand=body.storage,version"
        headers = {
            "Authorization": f"Bearer {self._api_token}",
            'Accept': 'application/json'
        }
        response = requests.get(api_url, headers=headers)
        print(response.status_code)
        if response.status_code == 200:
            results = response.json()['results']
            if results:
                return results[0]
            else:
                print("Page not found.")
        else:
            print(f"Error: {response.status_code}")
            print(response.text)


    def update_expand_element(self, content, outer_title, inner_title, new_content):
        def debug_print(message):
            print(f"DEBUG: {message}")
        def find_matching_close_tag(content, start_index):
            open_count = 1
            search_start = start_index
            while open_count > 0:
                open_tag = content.find('<ac:structured-macro', search_start)
                close_tag = content.find('</ac:structured-macro>', search_start)
                
                if close_tag == -1:
                    return -1
                
                if open_tag != -1 and open_tag < close_tag:
                    open_count += 1
                    search_start = open_tag + 1
                else:
                    open_count -= 1
                    search_start = close_tag + 1
    
            return close_tag
        outer_start_pattern = rf'<ac:structured-macro[^>]*ac:name="ui-expand"[^>]*>.*?<ac:parameter[^>]*ac:name="title"[^>]*>{re.escape(outer_title)}</ac:parameter>.*?<ac:rich-text-body>'
    
        outer_match = re.search(outer_start_pattern, content, re.DOTALL | re.IGNORECASE)
        if outer_match:
            start_index = outer_match.end()
            end_index = find_matching_close_tag(content, start_index)
            
            if end_index == -1:
                debug_print("Error: Could not find matching close tag for outer element")
                return content
            
            outer_content = content[start_index:end_index]
            # debug_print(f"Found outer element: {outer_title}")
            # debug_print(f"Outer content snippet: {outer_content[:200]}...")
            
            # Pattern for the inner expand element
            inner_pattern = rf'(<ac:structured-macro[^>]*ac:name="ui-expand"[^>]*>.*?<ac:parameter[^>]*ac:name="title"[^>]*>{re.escape(inner_title)}</ac:parameter>.*?<ac:rich-text-body>)(.*?)(</ac:rich-text-body>.*?</ac:structured-macro>)'
            
            def replacer(inner_match):
                debug_print(f"Found inner element: {inner_title}")
                existing_content = inner_match.group(2)
                # return f"{inner_match.group(1)}{new_content}{inner_match.group(3)}"
                return f"{inner_match.group(1)}{existing_content}\n{new_content}{inner_match.group(3)}"
            
            updated_inner_content = re.sub(inner_pattern, replacer, outer_content, flags=re.DOTALL | re.IGNORECASE)
            
            if updated_inner_content == outer_content:
                debug_print(f"Inner expand element with title '{inner_title}' not found or not updated.")
                # debug_print(f"Inner content search area: {outer_content}")
            else:
                debug_print("Inner content updated successfully")
            
            updated_content = content[:start_index] + updated_inner_content + content[end_index:]
            debug_print("Content updated successfully")
        else:
            debug_print(f"Outer expand element with title '{outer_title}' not found.")
            updated_content = content
        
        return updated_content

    def upload_data(self, page_url, content):
        headers = {
            "Authorization": f"Bearer {self._api_token}",
            "Content-Type": "application/json"
        }
        response = requests.put(page_url, headers=headers, data=json.dumps(content))
        if response.status_code == 200:
            print("Page updated successfully!")
        else:
            print(f"Failed to update page. Status code: {response.status_code}")
            print(response.text)
