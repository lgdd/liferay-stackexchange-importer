from dotenv import load_dotenv
import os
import requests
import aiohttp
import asyncio
import py7zr
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio
from requests.auth import HTTPBasicAuth
import xml.etree.ElementTree as ET
from pathlib import Path

stackexchange_topics = []
stackexchange_base_url = 'https://archive.org/download/stackexchange/'

load_dotenv()

user_email = os.environ.get("LIFERAY_USER_EMAIL")
user_password = os.environ.get("LIFERAY_USER_PASSWORD")
host = os.environ.get("LIFERAY_HOST")
site_friendly_url = os.environ.get("LIFERAY_SITE_FRIENDLY_URL", "guest")
basic_auth = HTTPBasicAuth(user_email, user_password)

def get_topics():
    topics = []
    with open('topics.txt', 'r') as file:
        for line in file:
            topics.append(line.strip())
    return topics

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
        
def download_stackexchange_topics(topics):
    for topic in topics:
        archive_name = f'{topic}.stackexchange.com.7z'
        url = stackexchange_base_url + archive_name

        archive_path = Path("data") / archive_name

        if not archive_path.exists():
            download_file(url, archive_path)
        else:
            print(f"'{archive_name}' already exists. Proceeding to extraction.")
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
    print(f"Creating {section_name}...")
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

def fetch_existing_thread_titles(section_id):
    print("Fetching existing message board threads to avoid duplicates...")
    url = f"{host}/o/headless-delivery/v1.0/message-board-sections/{section_id}/message-board-threads"
    existing_threads = []
    page = 1
    pageSize = 60
    total_count = float('inf')

    while len(existing_threads) < total_count:
        response = requests.get(url, params={'page': page, "pageSize": pageSize}, auth=basic_auth)

        if response.status_code == 200:
            data = response.json()
            total_count = data.get('totalCount', 0)
            existing_threads.extend([content.get("headline") for content in data.get("items", [])])
            page += 1
        else:
            raise Exception(f"[status={response.status_code}] Failed to fetch contents from folder with ID={section_id}")

    return existing_threads

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

async def create_thread_async(semaphore, session, section_id, title, body, answer):
    async with semaphore:
        url = f"{host}/o/headless-delivery/v1.0/message-board-sections/{section_id}/message-board-threads"
        payload = {
            "headline": title,
            "articleBody": body,
            "showAsQuestion": True,
            "encodingFormat": "html"
        }

        async with session.post(url, json=payload, headers={'Content-Type': 'application/json'}) as response:
            if response.status == 200:
                data = await response.json()
                thread_id = data.get("id")
                if answer is not None:
                    create_thread_answer(thread_id, answer)
            else:
                raise Exception(f"[status={response.status}] Failed to thread for section with ID={section_id}:\n{await response.text()}")

async def create_threads_async(folder_id, posts):
    async with aiohttp.ClientSession(auth=aiohttp.BasicAuth(user_email, user_password)) as session:
        semaphore = asyncio.Semaphore(10)
        tasks = []
        for post in posts.values():
            title = post.get('Title')
            body = post.get('Body')
            answer = post.get('AcceptedAnswerBody', None)
            task = create_thread_async(semaphore, session, folder_id, title, body, answer)
            tasks.append(task)

        for task in tqdm_asyncio.as_completed(tasks, desc=f"Uploading {topic_name} data"):
            await task

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
    stackexchange_topics = get_topics()
    download_stackexchange_topics(stackexchange_topics)
    site_id = fetch_site_id()
    existing_sections = fetch_existing_sections(site_id)
    sections = {}
    for topic in stackexchange_topics:
        topic_name = f'{topic}.stackexchange.com'
        if topic_name not in existing_sections:
            section_id = create_section(topic_name)
            sections[topic_name] = section_id
        else:
            sections[topic_name] = existing_sections[topic_name]
    for topic_name, section_id in sections.items():
        posts = parse_posts_xml(os.path.join("data", topic_name, "Posts.xml"))
        print(f"{topic_name} has {len(posts)} posts")
        existing_thread_titles = fetch_existing_thread_titles(section_id)
        posts_to_create = {}
        for post_id, post in posts.items():
            if post.get('Title') not in existing_thread_titles:
                posts_to_create[post_id] = post

        print(f"{topic_name} has {len(posts_to_create)} posts to create")
        asyncio.run(create_threads_async(section_id, posts_to_create))

except Exception as e:
    print(str(e))