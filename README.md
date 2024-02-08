# Liferay Stack Exchange Importer
 
Python script that download archive data of stackexchange.com (https://archive.org/download/stackexchange) and import each post and its accepted answer as message board categories, threads & messages in Liferay.

## Requirements

- Python 3.10+
- Liferay DXP/Portal u92+/ga92+

## Usage

```
pip install -r requirements.txt
python main.py
```

## Configuration

### Stack Exchange Data

The script will download the list of topics found in [topics.txt](topics.txt) and download the corresponding archive on https://archive.org/download/stackexchange. If you want to easily change the list, you can take any topics from the list in [all_topics](all_topics.txt).

### Liferay

The script is looking for environment variables that you can store in an `.env` file such as:

```
LIFERAY_USER_EMAIL=test@liferay.com
LIFERAY_USER_PASSWORD=test
LIFERAY_HOST=http://localhost:8080
```
