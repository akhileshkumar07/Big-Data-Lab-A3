import os
import requests
from bs4 import BeautifulSoup
import random

BASE_URL = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/'
YEAR = '2021'
OUTPUT_PATH = '/home/akhilesh/big-data-lab/'
NUM_FILES = 1

# Fetch the webpage of the datasets
page_response = requests.get(BASE_URL + YEAR + '/')
if page_response.status_code == 200:
    with open(os.path.join(OUTPUT_PATH, 'page_fetch.html'), 'wb') as page_file:
        page_file.write(page_response.content)

# Extract csv file links
def extract_csv_links(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    csv_links = [link.get('href') for link in soup.find_all('a') if link.get('href').endswith('.csv')]
    return csv_links

with open(os.path.join(OUTPUT_PATH, 'page_fetch.html'), 'r', encoding='utf-8') as file:
    html_content = file.read()
    csv_links = extract_csv_links(html_content)

# Select random data files from the available list of csv files
selected_files = random.sample(csv_links, NUM_FILES)

# Fetch individual csv files
for filename in selected_files:
    file_url = BASE_URL + YEAR + '/' + filename
    response = requests.get(file_url)
    if response.status_code == 200:
        with open(os.path.join(OUTPUT_PATH, filename), 'wb') as file:
            file.write(response.content)

# Delete the page_fetch.html file
os.remove(os.path.join(OUTPUT_PATH, 'page_fetch.html'))