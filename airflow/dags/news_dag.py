from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.dummy import DummyOperator
from dotenv import load_dotenv
import newspaper
import feedparser
import logging
import openai
import re
import os

load_dotenv()

openai.my_api_key = os.environ.get("OPENAI_SECRET_KEY")

def parse_entry_url(url):
    return re.search(r"url=([^&]+)", url).group(1)

def _check_feed_updates(**context):
    logger = logging.getLogger(__name__)

    start_time = context["ts"]

    logger.info(f"Fetching updates for {start_time}")
    feed = feedparser.parse(RSS_FEED_URL)
    logger.info(f"Fetched {len(feed)} entries from the feed")

    ts_time = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S%z").replace(tzinfo=None)
    previous_run_time = ts_time - timedelta(minutes=2)
    logger.info(f"Looking for updates between {str(previous_run_time)} and {start_time}")
    
    new_entries = []
    for i, entry in enumerate(feed.entries):
        published_time = datetime.strptime(entry.published, "%Y-%m-%dT%H:%M:%SZ")
        logger.info(f"{i}th entry was pulished at {published_time}")
        if (published_time <= ts_time) and (published_time > previous_run_time):
            new_entries.append({"id": entry.id,
                    "title": entry.title,
                    "link": parse_entry_url(entry.link),
                    "published": entry.published,
                    "summary": ""})


    # new_entries = [{"id": entry.id,
    #                 "title": entry.title,
    #                 "link": entry.link,
    #                 "published": entry.published,
    #                 "summary": entry.summary} for entry in feed.entries if (
    #     (published_time := datetime.strptime(entry.published, "%Y-%m-%dT%H:%M:%SZ")) <= ts_time and
    #     published_time > previous_run_time)]

    if not new_entries:
        logger.info(f"There were no updates between {str(previous_run_time)} and {start_time}. Stopping execution...")
        return "stop_dag_no_updates"
    else:
        context["ti"].xcom_push(key="new_entries", value=new_entries)
        return "process_updates"

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
    entries = context["ti"].xcom_pull(task_id="check_feed_updates", key="new_entries")
    entries_with_summary = []
    for entry in entries:
        entry_text = _extract_article_text(entry["link"])
        if entry_text and NAMED_ENTITY in entry_text:
            message = {"role": "system", "content": OPENAI_PROMPT + entry_text}
            chat = openai.ChatCompletion.create(model="gpt-3.5-turbo", messages=[message])
            reply = chat.choices[0].message.content
            entry["summary"] = reply
            entries_with_summary.append(entry)
    if not entries_with_summary:
        return "stop_dag_no_articles"
    else:
        context["ti"].xcom_push(key="entries_with_summary", value=entries_with_summary)
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