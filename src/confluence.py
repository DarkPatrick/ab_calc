from urllib.parse import urlparse, parse_qs
from dotenv import dotenv_values
import requests
import re
import json
from urllib.parse import urlparse, unquote
import base64


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
        return None

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
                # return results[0]
                return {
                    "page_id": results[0]['id'],
                    "page_version": results[0]['version']["number"],
                    "page_title": results[0]['title'],
                    "current_content": results[0]['body']['storage']['value'],
                    "page_url": f"{self._base_url}/rest/api/content/{results[0]['id']}"
                }
            else:
                print("Page not found.")
        else:
            print(f"Error: {response.status_code}")
            print(response.text)
        return None


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
    
    def get_iterations_list(self, content):
        pattern = r'(?<=<h1>Results)(.*)'
        matches = re.search(pattern, content, re.DOTALL | re.IGNORECASE)
        start_index = matches.start()
        open_tag_cnt = 0
        iterations_dict = {}
        input_string = content[start_index:]
        i = 0
        first_titles = []
        first_titles_iter = 0
        second_titles = []
        second_titles_iter = 0
        while i < len(input_string):
            if input_string[i:].startswith('<ac:structured-macro'):
                open_tag_cnt += 1
                macro_block = input_string[i:input_string.find('</ac:structured-macro>', i) + len('</ac:structured-macro>')]
                title_match = re.search(r'ac:name="ui-expand"[^>]*>.*?<ac:parameter[^>]*ac:name="title"[^>]*>(.*?)</ac:parameter>', macro_block)
                if title_match:
                    title = title_match.group(1)
                    if open_tag_cnt - 1 == 0:
                        first_titles.append(title)
                        iterations_dict[title] = {}
                        first_titles_iter += 1
                    elif open_tag_cnt - 1 == 1:
                        iterations_dict[first_titles[first_titles_iter - 1]] = {title: {}}
                        second_titles.append(first_titles[first_titles_iter - 1])
                        second_titles_iter += 1
                i += len('<ac:structured-macro')
            elif input_string[i:].startswith('</ac:structured-macro>'):
                open_tag_cnt -= 1
                i += len('</ac:structured-macro>')
            else:
                i += 1
        return iterations_dict

    def parse_confluence_url(self, url):
        parsed_url = urlparse(url)
        constant_path = "/display/"

        if parsed_url.path.startswith(constant_path):
            remaining_path = parsed_url.path[len(constant_path):]

            path_segments = remaining_path.split('/')

            if len(path_segments) >= 2:
                space_key = unquote(path_segments[0])
                page_title = unquote(path_segments[1])
                return space_key, page_title
            else:
                return None, None
        else:
            return None, None

    def upload_image(self, file_path, file_name, page_id):
        with open(file_path, 'rb') as file:
            file_content = file.read()
        # encoded_content = base64.b64encode(file_content).decode('utf-8')
        upload_url = f'{self._base_url}/rest/api/content/{page_id}/child/attachment'
        # print(upload_url)
        # auth = ("e.semin@mu.se", self._api_token)
        # payload = {
        #     'file': encoded_content,
        #     'comment': 'Uploaded via API',
        #     'minorEdit': True
        # }
        headers = {
            'Authorization': f'Bearer {self._api_token}',
            'X-Atlassian-Token': 'no-check'
        }
        response = requests.post(upload_url, headers=headers, files={'file': (file_name, file_content, "application/octet-stream")})
        if response.status_code == 200:
            print('Image uploaded successfully')
            return response.json()['results'][0]['id']
        else:
            print(f'Failed to upload image. Status code: {response.status_code}')
            print(response.text)
        return None

    def generate_image_markup(self, image_file_name, width=250, height=250):
        image_markup = f'<ac:image ac:width="{width}" ac:height="{height}"><ri:attachment ri:filename="{image_file_name}" ri:version-at-save="1" /></ac:image>'
        return image_markup

