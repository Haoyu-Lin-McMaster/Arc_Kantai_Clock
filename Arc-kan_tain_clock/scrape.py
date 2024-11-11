import re
import pytz
import datetime
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient, UpdateOne
import json
import uuid
from tqdm import tqdm

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/95.0.4638.69 Safari/537.36 Edg/95.0.1020.53'
}

weekday = {
    'Monday': 0,
    'Tuesday': 1,
    'Wednesday': 2,
    'Thursday': 3,
    'Friday': 4,
    'Saturday': 5,
    'Sunday': 6,
    '一': 0,
    '二': 1,
    '三': 2,
    '四': 3,
    '五': 4,
    '六': 5,
    '日': 6
}

now = datetime.datetime.now().replace(second=0, microsecond=0)
curr_year = str(now.year)
curr_month = now.month

def get_curr_season(eng=False):
    if curr_month >= 1 and curr_month <= 3:
        curr_season = (curr_year + '/winter') if eng else (curr_year + '01')
    elif curr_month >= 4 and curr_month <= 6:
        curr_season = (curr_year + '/spring') if eng else (curr_year + '04')
    elif curr_month >= 7 and curr_month <= 9:
        curr_season = (curr_year + '/summer') if eng else (curr_year + '07')
    elif curr_month >= 10 and curr_month <= 12:
        curr_season = (curr_year + '/fall') if eng else (curr_year + '10')
    return curr_season

def to_local_time(day, time, zone='cst'):
    source_tz = pytz.timezone('Asia/Shanghai') if zone == 'cst' else pytz.timezone('Asia/Tokyo')
    try:
        hour, minute = map(int, time.split(':'))
    except ValueError:
        raise ValueError(f"Invalid time format: '{time}'")
    original_time = datetime.datetime.now().replace(hour=hour, minute=minute, second=0, microsecond=0)
    original_time = source_tz.localize(original_time)
    local_time = original_time.astimezone()
    day += (local_time.day - original_time.day)
    day %= 7
    local_hour_minute = local_time.strftime('%H:%M')
    return day, local_hour_minute

def anime_chs():
    anime_list = []
    url = 'https://yuc.wiki/{}/'.format(get_curr_season(True))
    page_text = requests.get(url=url, headers=headers).content
    soup = BeautifulSoup(page_text, 'html.parser')
    animes = soup.find_all('div', {'style': 'float:left'})
    for anime in tqdm(animes, desc="Processing CHS"):
        try:
            name = anime.find('td', class_=re.compile('^date_title.*')).text
            date = anime.find('p', {'class': 'imgtext'}).text.split('~')[0] + '/' + curr_year
            time = anime.find('p', {'class': 'imgep'}).text.split('~')[0]
            img = anime.find('img')['src']
            date = datetime.datetime.strptime(date, "%m/%d/%Y")
            day = weekday[date.strftime("%A")]
            if int(time[:2]) >= 24:
                time = '0' + str(int(time[:2]) - 24) + time[2:]
                day = (day + 1) % 7
            day, time = to_local_time(day, time)
            anime_list.append({"name": name, "day": day, "time": time, "timezone": "Asia/Shanghai", "img": img})
        except Exception as e:
            print(e, "scraping error")
    return anime_list

def anime_cht():
    anime_list = []
    url = 'https://acgsecrets.hk/bangumi/{}/'.format(get_curr_season())
    page_text = requests.get(url=url, headers=headers).content
    soup = BeautifulSoup(page_text, 'html.parser')
    content = soup.find('div', {'id': 'acgs-anime-icons'})
    animes = content.find_all('div', recursive=False)
    for anime in tqdm(animes, desc="Processing CHT"):
        name = anime.find('div', {'class': 'anime_name'}).text
        day = weekday[anime.find('div', {'class': 'day'}).text]
        time = anime.find('div', {'class': 'time'}).text
        img = anime.find('img', {'class': 'img-fit-cover'})['src']
        day, time = to_local_time(day, time)
        anime_list.append({"name": name, "day": day, "time": time, "timezone": "Asia/Taipei", "img": img})
    return anime_list

def get_date_time(url):
    page_text = requests.get(url=url, headers=headers).content
    soup = BeautifulSoup(page_text, 'html.parser')
    content_span = soup.find('span', string=re.compile('Broadcast:'))
    if not content_span or not content_span.next_sibling:
        return None, None
    content = content_span.next_sibling.strip().split()
    if len(content) < 4:
        return None, None
    day = weekday.get(content[0][:-1], None)
    time = content[2]
    zone = content[3][1:-1]
    return to_local_time(day, time, zone)

def anime_eng():
    anime_list = []
    url = 'https://myanimelist.net/anime/season/{}'.format(get_curr_season(True))
    page_text = requests.get(url=url, headers=headers).content
    soup = BeautifulSoup(page_text, 'html.parser')
    content = soup.find('div', {'class': 'seasonal-anime-list js-seasonal-anime-list js-seasonal-anime-list-key-1'})
    animes = content.find_all('div', {'class': 'js-anime-category-producer seasonal-anime js-seasonal-anime js-anime-type-all js-anime-type-1'})
    for anime in tqdm(animes, desc="Processing ENG"):
        name = anime.find('span', {'class': 'js-title'}).text
        link = anime.find('a')['href']
        day, time = get_date_time(link)
        if day is None or time is None:
            continue
        img_tag = anime.find('img')
        img = img_tag.get('src') or img_tag.get('data-src')
        anime_list.append({"name": name, "day": day, "time": time, "timezone": "Asia/Tokyo", "img": img})
    return anime_list

def load_mongodb_uri(config_file='config.json'):
    with open(config_file, 'r') as file:
        config = json.load(file)
        mongodb_uri = config.get('mongodb_uri')
        if not mongodb_uri:
            raise ValueError("MongoDB URI not found in the config file.")
        return mongodb_uri

def get_mongo_collection(db_name, collection_name, uri):
    client = MongoClient(uri)
    db = client[db_name]
    collection = db[collection_name]
    return collection

def store_anime_info(anime_name, anime_info_collection):
    """Store or retrieve unique anime identifier based on name."""
    anime_info = anime_info_collection.find_one({"name": anime_name})
    if anime_info:
        return anime_info["anime_id"]
    else:
        anime_id = str(uuid.uuid4())
        anime_info_collection.insert_one({"anime_id": anime_id, "name": anime_name})
        return anime_id

def time_within_one_hour(time1, time2):
    """Check if two times (in HH:MM format) are within one hour of each other."""
    time1 = datetime.datetime.strptime(time1, '%H:%M')
    time2 = datetime.datetime.strptime(time2, '%H:%M')
    return abs((time1 - time2).total_seconds()) <= 3600

def get_anime():
    all_anime = {}
    languages = {"chs": anime_chs(), "cht": anime_cht(), "eng": anime_eng()}
    mongodb_uri = load_mongodb_uri('config.json')
    anime_info_collection = get_mongo_collection('anime_db', 'anime_info_collection', mongodb_uri)

    # Use a bulk update for storing anime information
    anime_bulk_updates = []

    for lang, anime_data in languages.items():
        for anime in tqdm(anime_data, desc=f"Processing {lang.upper()} Data"):
            anime_id = store_anime_info(anime["name"], anime_info_collection)

            if anime_id not in all_anime:
                all_anime[anime_id] = {"anime_id": anime_id, "translations": {}}
            
            added = False
            for existing_lang, data in all_anime[anime_id]["translations"].items():
                if anime["day"] == data["day"] and time_within_one_hour(anime["time"], data["time"]):
                    all_anime[anime_id]["translations"][lang] = {
                        "name": anime["name"],
                        "time": anime["time"],
                        "day": anime["day"],
                        "timezone": anime["timezone"],
                        "image_url": anime["img"]
                    }
                    added = True
                    break
             
            if not added:
                all_anime[anime_id]["translations"][lang] = {
                    "name": anime["name"],
                    "time": anime["time"],
                    "day": anime["day"],
                    "timezone": anime["timezone"],
                    "image_url": anime["img"]
                }

    anime_collection = get_mongo_collection('anime_db', 'anime_collection', mongodb_uri)

    # Prepare bulk updates for the main anime collection
    for anime_id, anime_data in all_anime.items():
        anime_bulk_updates.append(
            UpdateOne(
                {"anime_id": anime_id},
                {"$set": anime_data},
                upsert=True
            )
        )

    # Execute bulk write
    if anime_bulk_updates:
        anime_collection.bulk_write(anime_bulk_updates)

    return all_anime

if __name__ == '__main__':
    anime_data = get_anime()
