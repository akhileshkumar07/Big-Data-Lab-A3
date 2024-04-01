import os
import requests
from bs4 import BeautifulSoup
import random
import yaml

BASE_URL = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/'
OUTPUT_PATH = os.getcwd()
NUM_FILES = 5

# Load YEAR variable from params.yaml
with open('params.yaml', 'r') as yaml_file:
    config = yaml.safe_load(yaml_file)
    YEAR = config['YEAR']

# Fetch the webpage of the datasets
page_response = requests.get(BASE_URL + YEAR + '/')
if page_response.status_code == 200:
    with open(os.path.join(OUTPUT_PATH, 'page_fetch.html'), 'wb') as page_file:
        page_file.write(page_response.content)

# Extract csv file links
with open(os.path.join(OUTPUT_PATH, 'page_fetch.html'), 'r', encoding='utf-8') as file:
    html_content = file.read()
    soup = BeautifulSoup(html_content, 'html.parser')
    csv_links = [link.get('href') for link in soup.find_all('a') if link.get('href').endswith('.csv')]

# Select random data files from the available list of csv files
selected_files = random.sample(csv_links, NUM_FILES)

# Fetch individual csv files
print("Downloading CSV Files...")
for filename in selected_files:
    file_url = BASE_URL + YEAR + '/' + filename
    response = requests.get(file_url)
    if response.status_code == 200:
        with open(os.path.join(OUTPUT_PATH, filename), 'wb') as file:
            file.write(response.content)
print("Download Complete.")

# Delete the page_fetch.html file
os.remove(os.path.join(OUTPUT_PATH, 'page_fetch.html'))