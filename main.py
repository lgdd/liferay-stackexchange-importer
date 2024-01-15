from dotenv import load_dotenv
import os
import requests
import re
import py7zr
from tqdm import tqdm
from requests.auth import HTTPBasicAuth
import xml.etree.ElementTree as ET
from pathlib import Path

stackexchange_topics = ["ai", "beer", "coffee"]
stackexchange_base_url = 'https://archive.org/download/stackexchange/'

load_dotenv()

user_email = os.environ.get("LIFERAY_USER_EMAIL")
user_password = os.environ.get("LIFERAY_USER_PASSWORD")
host = os.environ.get("LIFERAY_HOST")
site_friendly_url = os.environ.get("LIFERAY_SITE_FRIENDLY_URL", "guest")
basic_auth = HTTPBasicAuth(user_email, user_password)

def download_file(url, filename):
    response = requests.get(url, stream=True)
    response.raise_for_status()  # Check if the request was successful
    total_size = int(response.headers.get('content-length', 0))
    block_size = 1024
    with open(filename, 'wb') as file, tqdm(
        desc=filename.name,
        total=total_size,
        unit='iB',
        unit_scale=True,
        unit_divisor=block_size,
    ) as bar:
        for data in response.iter_content(block_size):
            bar.update(len(data))
            file.write(data)

def extract_file_from_archive(archive_path, file_to_extract, extract_path):
    with py7zr.SevenZipFile(archive_path, 'r') as archive:
        all_files = archive.getnames()
        if file_to_extract in all_files:
            print(f"Extracting {file_to_extract}...")
            archive.extract(targets=[file_to_extract], path=extract_path)
            print(f"Extraction completed. File is located in: {extract_path}/{file_to_extract}")
        else:
            raise Exception(f"File {file_to_extract} not found in the archive.")
        
def download_stackexchange_topics():
    topics = []
    with open('topics.txt', 'r') as file:
        for line in file:
            topics.append(line.strip())
    for topic in topics:
        archive_name = f'{topic}.stackexchange.com.7z'
        url = stackexchange_base_url + archive_name

        archive_path = Path("data") / archive_name

        if not archive_path.exists():
            download_file(url, archive_path)
        else:
            print("Archive already exists. Proceeding to extraction.")
        file_to_extract = 'Posts.xml'
        extract_path =  Path("data") / f'{topic}.stackexchange.com'

        extract_path.mkdir(exist_ok=True)

        extract_file_from_archive(archive_path, file_to_extract, extract_path)

def fetch_site_id():
    url = f"{host}/o/headless-admin-user/v1.0/sites/by-friendly-url-path/{site_friendly_url}"
    response = requests.get(url, auth=basic_auth)

    if response.status_code == 200:
        data = response.json()
        return data.get("id")
    else:
        raise Exception(f"[status={response.status_code}] Failed to fetch default site 'guest' ID")

def fetch_existing_sections(site_id):
    url = f"{host}/o/headless-delivery/v1.0/sites/{site_id}/message-board-sections"
    response = requests.get(url, auth=basic_auth)

    if response.status_code == 200:
        existing_sections = {}
        data = response.json()
        for folder in data.get("items"):
            existing_sections[folder.get("title")] = folder.get("id")
        return existing_sections
    else:
        raise Exception(f"[status={response.status_code}] Failed to fetch message board sections from site with ID = {site_id}")

def create_section(section_name):
    url = f"{host}/o/headless-delivery/v1.0/sites/{site_id}/message-board-sections"
    payload = {
        "title": section_name
    }
    response = requests.post(url, json=payload, auth=basic_auth, headers={'Content-Type': 'application/json'})
    if response.status_code == 200:
        data = response.json()
        section_id = data.get("id")
        print(f"Section '{section_name}' has been created with ID={section_id}")
        return section_id
    else:
        raise Exception(f"[status={response.status_code}] Failed to message board section for site with ID = {site_id}")

def fetch_existing_threads(section_id):
    url = f"{host}/o/headless-delivery/v1.0/message-board-sections/{section_id}/message-board-threads"
    response = requests.get(url, auth=basic_auth)

    if response.status_code == 200:
        existing_threads = {}
        data = response.json()
        for content in data.get("items"):
            existing_threads[content.get("headline")] = content.get("id")
        return existing_threads
    else:
        raise Exception(f"[status={response.status_code}] Failed to fetch contents from folder with ID={section_id}")

def create_thread(section_id, title, body, answer):
    url = f"{host}/o/headless-delivery/v1.0/message-board-sections/{section_id}/message-board-threads"
    payload = {
        "headline": title,
        "articleBody": body,
        "showAsQuestion": True,
        "encodingFormat": "html"
    }

    response = requests.post(url, json=payload, auth=basic_auth, headers={'Content-Type': 'application/json'})
    if response.status_code == 200:
        data = response.json()
        thread_id = data.get("id")
        if answer is not None:
            create_thread_answer(thread_id, answer)
    else:
        raise Exception(f"[status={response.status_code}] Failed to thread for section with ID={section_id}:\n{response.text}")

def create_thread_answer(thread_id, answer):
    url = f"{host}/o/headless-delivery/v1.0/message-board-threads/{thread_id}/message-board-messages"
    payload = {
        "articleBody": answer,
        "showAsAnswer": True,
        "encodingFormat": "html"
    }

    response = requests.post(url, json=payload, auth=basic_auth, headers={'Content-Type': 'application/json'})
    if response.status_code == 200:
        data = response.json()
        thread_id = data.get("id")
    else:
        raise Exception(f"[status={response.status_code}] Failed to create thread answer for thread with ID={thread_id}:\n{response.text}")

def parse_posts_xml(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()
    posts = {}
    answers = {}
    for row in root.findall('row'):
        post_id = row.get('Id')
        post_type = row.get('PostTypeId')
        if post_type == '1':
            posts[post_id] = {
                'Title': row.get('Title'),
                'Body': row.get('Body'),
                'AcceptedAnswerId': row.get('AcceptedAnswerId')
            }
        elif post_type == '2':
            answers[post_id] = row.get('Body')

    for post_id, post in posts.items():
        accepted_answer_id = post.get('AcceptedAnswerId')
        if accepted_answer_id and accepted_answer_id in answers:
            post['AcceptedAnswerBody'] = answers[accepted_answer_id]
    return posts

try:
    data_folder = Path("data")
    data_folder.mkdir(parents=True, exist_ok=True)
    download_stackexchange_topics()
    site_id = fetch_site_id()
    existing_folders = fetch_existing_sections(site_id)
    folders_to_create = []
    for topic in stackexchange_topics:
        topic_folder_name = f'{topic}.stackexchange.com'
        if topic_folder_name in existing_folders:
            print(f"Category '{topic_folder_name}' already exists with ID = {existing_folders[topic_folder_name]}")
        else:
            folders_to_create.append(topic_folder_name)
            print(f"Category to create: {topic_folder_name}")
    for folder in folders_to_create:
        folder_id = create_section(folder)
        existing_folders[folder] = folder_id
    requests.get(f"{host}/o/healdess")
    for folder_name, folder_id in existing_folders.items():
        posts = parse_posts_xml(os.path.join("data", folder_name, "Posts.xml"))
        print(f"{folder_name} has {len(posts)} posts")
        existing_contents = fetch_existing_threads(folder_id)
        for post in tqdm(posts.values(), desc=f"Uploading {folder_name} data"):
            if not post.get('Title') in existing_contents:
                create_thread(folder_id, post.get('Title'), post.get('Body'), post.get('AcceptedAnswerBody', None))

except Exception as e:
    print(str(e))