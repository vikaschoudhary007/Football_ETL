import json
from datetime import datetime
import geocoder
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()


NO_IMAGE = 'https://upload.wikimedia.org/wikipedia/commons/thumb/0/0a/No-image-available.png/480px-No-image-available.png'

# EXTRACTION LAYER
def get_wikipedia_page(url):
    import requests
    print("getting wikipedia page...", url)

    try:
        res = requests.get(url, timeout=10)
        res.raise_for_status() # check if request was successful or not
        return res.text

    except requests.RequestException as e:
        print(f"Error : {e}")

def get_wikipedia_data(html):
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    table = soup.find_all("table", {"class": "wikitable sortable sticky-header"})[0]
    table_rows = table.find_all('tr')

    return table_rows


def clean_text(text):
    text = str(text).strip()
    text = text.replace('&nbsp', '')
    if text.find(' ♦'):
        text = text.split(' ♦')[0]
    if text.find(' ['):
        text = text.split('[')[0]

    return text.replace('\n', '')


def extract_wikipedia_data(**kwargs):
    url = kwargs["url"]
    html = get_wikipedia_page(url)
    data = get_wikipedia_data(html)

    output = []

    for i in range(1, len(data)):
        tds = data[i].find_all('td')
        values = {
            'rank': i,
            'stadium': clean_text(tds[0].text),
            'capacity': clean_text(tds[1].text).replace(',',''),
            'region': clean_text(tds[2].text),
            'country': clean_text(tds[3].text),
            'city': clean_text(tds[4].text),
            'images': 'https://' + tds[5].find('img').get('src').split('//')[1] if tds[5].find('img') else "NO_IMAGE",
            'home_team': clean_text(tds[6].text)
        }
        output.append(values)
    json_rows = json.dumps(output)
    kwargs['ti'].xcom_push(key='rows', value=json_rows)
    return "OK"

# TRANSFORMATION LAYER

# def get_long_lat(country, city):
#     geolocator = Nominatim(user_agent='football_etl')
#     try:
#         location = geolocator.geocode(f'{city}, {country}')
#         if location:
#             return location.latitude, location.longitude
#     except Exception as e:
#         print(f"Geocoding error: {e}")
#     return None

def get_long_lat(country, city):
    location = geocoder.arcgis(f'{city}, {country}')

    if location.ok:
        return location.latlng[0], location.latlng[1]

    return None

def transform_wikipedia_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='extract_data_from_wikipedia')

    data = json.loads(data)

    stadiums_df = pd.DataFrame(data)
    stadiums_df['images'] = stadiums_df['images'].apply(lambda x: x if x not in ['NO_IMAGE', '', None] else NO_IMAGE)
    stadiums_df['location'] = stadiums_df.apply(lambda x: get_long_lat(x['country'], x['stadium']), axis=1)
    stadiums_df['capacity'] = stadiums_df['capacity'].astype(int)

    # handle duplicates
    duplicates = stadiums_df[stadiums_df.duplicated(['location'])]
    duplicates['location'] = duplicates.apply(lambda x: get_long_lat(x['country'], x['city']), axis=1)

    stadiums_df.update(duplicates)

    # push to xcom
    kwargs['ti'].xcom_push(key='rows', value=stadiums_df.to_json())
    return "OK"

def write_wikipedia_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='transform_wikipedia_data')

    data = json.loads(data)

    data = pd.DataFrame(data)

    file_name = ('stadiums_cleaned' + str(datetime.now().date()) + "_" + str(datetime.now().time()).replace(':', '_') + ".csv")

    # data.to_csv('data/'+file_name, index=False)
    #fR64oAFXJPZZw5A1NDJzloNGr4YvuaJmMLAZ2r2eS//14rIeL5oyDIBEV9y/+AOsPUQ59emdr67T+AStC6F51g==

    # abfs://containerName@StorageAccountName.dfs.core.windows.net
    data.to_csv('abfs://footballdataetl@footballdataetl.dfs.core.windows.net/data/'+file_name,
                storage_options= {
            'account_key': os.getenv('API_KEY')
        }, index=False)


    return "OK"