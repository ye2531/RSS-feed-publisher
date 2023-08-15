from datetime import datetime
from utils.common_utils import get_dags_folder_path
from modules.settings import RSS_FEED_URL
import feedparser
import logging
import re
import os

logger = logging.getLogger(__name__)

def parse_entry_url(url):
    return re.search(r"url=([^&]+)", url).group(1)

def string_to_date(date):
    return datetime.strptime(date, "%Y-%m-%dT%H:%M:%SZ")

def replace_apostrophes(text):
    return text.replace("'", "''")

def fetch_feed_updates(**context):
    
    start_time = context["ts"]

    logger.info(f"Fetching updates for {start_time}")
    feed = feedparser.parse(RSS_FEED_URL)
    logger.info(f"Fetched {len(feed.entries)} entries from the feed")

    update_time = string_to_date(feed.feed.updated)

    update_time_file_path = os.path.join(get_dags_folder_path(), "temp/update_time.txt")

    with open(update_time_file_path, "r+") as file:
        last_update_time = string_to_date(file.read())

        if update_time > last_update_time:
            file.seek(0)
            file.write(feed.feed.updated)
            file.truncate()

            new_entries = [{"title": entry.title,
                            "link": parse_entry_url(entry.link),
                            "text": "",
                            "published": string_to_date(entry.published)} for entry in feed.entries \
                                if string_to_date(entry.published) > last_update_time]
            
            context["ti"].xcom_push(key="new_entries", value=new_entries)
            return "process_updates"  
          
        else:
            logger.info(f"There were no updates. Stopping execution...")
            return "stop_dag_no_updates"