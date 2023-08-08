from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.dummy import DummyOperator
from settings import RSS_FEED_URL, NAMED_ENTITY
import newspaper
import feedparser
import logging
import re


def parse_entry_url(url):
    return re.search(r"url=([^&]+)", url).group(1)

def _check_feed_updates(**context):
    logger = logging.getLogger(__name__)

    start_time = context["ts"]

    logger.info(f"Fetching updates for {start_time}")
    feed = feedparser.parse(RSS_FEED_URL)
    logger.info(f"Fetched {len(feed.entries)} entries from the feed")

    update_time = datetime.strptime(feed.feed.updated, "%Y-%m-%dT%H:%M:%SZ")

    with open("dags/temp/update_time.txt", "r+") as file:
        last_update_time = datetime.strptime(file.read(), "%Y-%m-%dT%H:%M:%SZ")

        if update_time > last_update_time:
            file.seek(0)
            file.write(feed.feed.updated)
            file.truncate()

            new_entries = [{"id": entry.id,
                            "title": entry.title,
                            "link": parse_entry_url(entry.link),
                            "published": entry.published,
                            "summary": entry.summary} for entry in feed.entries \
                                if datetime.strptime(entry.published, "%Y-%m-%dT%H:%M:%SZ") > last_update_time]
            
            context["ti"].xcom_push(key="new_entries", value=new_entries)
            return "process_updates"  
          
        else:
            logger.info(f"There were no updates. Stopping execution...")
            return "stop_dag_no_updates"

def _extract_article_text(url):
    article = newspaper.Article(url)
    try:
        article.download()
        article.parse()

        return article.text
    
    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.error(f"Cannot download article {url} \n {str(e)}")
        return None

def _process_updates(**context):

    entries = context["ti"].xcom_pull(task_ids="check_feed_updates", key="new_entries")

    entries_clean = []

    for entry in entries:
        entry_text = _extract_article_text(entry["link"])
        if entry_text and NAMED_ENTITY in entry_text:
            entries_clean.append(entry)

    if not entries_clean:
        return "stop_dag_no_articles"
    else:
        context["ti"].xcom_push(key="entries_with_summary", value=entries_clean)
        return "save_and_publish"

def _save_new_entries():
    pass

def _publish_new_entries():
    pass 

with DAG(
    dag_id="news_dag",
    schedule_interval="*/2 * * * *",
    start_date=datetime.now() - timedelta(minutes=2),
):
            
    fetch_feed_updates = BranchPythonOperator(
        task_id='check_feed_updates',
        python_callable=_check_feed_updates
    )

    stop_dag_no_updates = DummyOperator(
        task_id='stop_dag_no_updates'
    )

    process_updates = BranchPythonOperator(
        task_id='process_updates',
        python_callable=_process_updates
    )

    stop_dag_no_articles = DummyOperator(
        task_id='stop_dag_no_articles'
    )

    save_and_publish = DummyOperator(
        task_id='save_and_publish',
    )

    save_new_entries = PythonOperator(
        task_id="save_new_entries",
        python_callable=_save_new_entries,
    )

    publish_new_entries = PythonOperator(
        task_id="publish_new_entries",
        python_callable=_publish_new_entries,
    )

    fetch_feed_updates >> [stop_dag_no_updates, process_updates]
    process_updates >> [stop_dag_no_articles, save_and_publish]
    save_and_publish >> [save_new_entries, publish_new_entries]