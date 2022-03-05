#Authors:
#   v1
#   Orkun Berk Yuzbasioglu, orkunberk@hotmail.com
#   Okan Yurt,  okanyurt@hotmail.com
#   5.03.2022
#   Getting hourly past weather data for Turkish cities
#   Input: STARTYEAR - ENDYEAR

import os
import requests
import csv

from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from urllib.parse import urljoin

CURRENT_YEAR = datetime.now().year
YEARS = input('Tarih aralıklarını giriniz (örn: 2016 - 2020)')
BEGINNING_YEAR = int(YEARS.split('-')[0].strip())
ENDING_YEAR = min(int(YEARS.split('-')[1].strip()), CURRENT_YEAR)
BASE_URL = 'https://www.ncei.noaa.gov/data/global-hourly/access/'
IS_VERBOSE = int(input('Işlem Detayı Terminale Basılsın mı? (0/1)'))
SAVE_PATH = './data'

# creating directory for results
try:
        os.mkdir('data')
except FileExistsError:
        pass

# read city id's for Turkey from turkey_cities.csv file
with open('turkey_cities.csv', 'r') as file:
    reader = csv.reader(file, delimiter=',')
    next(reader, None)  # skip the headers
    data_read = [row for row in reader]

# construct Turkey's cities as a list
cities_turkey = []

for row  in data_read:
        cities_turkey.append(row[1].strip())

# construct Turkey's id as a list
ids_turkey = []

for row  in data_read:
        ids_turkey.append(row[0].strip())

# Scraping the hourly weather for each city of Turkey and printing the number of cities remaining as output

for year in range(BEGINNING_YEAR, ENDING_YEAR+1):

    for ix, id in enumerate(ids_turkey):

        file_name = id + '_' + str(year) + '.csv'
        
        completeName = '/'.join([SAVE_PATH, file_name])
        
        url = urljoin(BASE_URL, str(year)) + '/' + id + '.csv'

        if IS_VERBOSE:
            print('Number of cities remaining {} for year {}:'.format(len(ids_turkey) - (ix+1), year))

        session = requests.Session()
        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)

        response = session.get(url)

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            pass
        else:
            with open(completeName, "wb") as text_file:
                text_file.write(response.content)