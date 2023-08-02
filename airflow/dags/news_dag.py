from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.dummy import DummyOperator
import feedparser
import logging
import json


RSS_FEED_URL = ""

def _check_feed_updates(**context):
    logger = logging.getLogger(__name__)

    start_time = context["ts"]

    logger.info(f"Fetching updates for {start_time}")
    feed = feedparser.parse(RSS_FEED_URL)

    ts_time = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S%z").replace(tzinfo=None)
    previous_run_time = ts_time - timedelta(minutes=2)

    new_entries = [{"id": entry.id,
                    "title": entry.title,
                    "link": entry.link,
                    "published": entry.published,
                    "summary": entry.summary} for entry in feed.entries if (
        (published_time := datetime.strptime(entry.published, "%Y-%m-%dT%H:%M:%SZ")) <= ts_time and
        published_time > previous_run_time)]

    if not new_entries:
        logger.info(f"There were no updates between {str(previous_run_time)} and {start_time}. Stopping execution...")
        return "stop_dag_execution"
    else:
        context["ti"].xcom_push(key="new_entries", value=new_entries)
        return "process_updates"

def _process_updates(**context):
    entries = context["ti"].xcom_pull(task_id="check_feed_updates", key="new_entries")

with DAG(
    dag_id="news_dag",
    schedule_interval="*/2 * * * *",
    start_date=datetime.now() - timedelta(minutes=2),
):
            
    fetch_feed_updates = BranchPythonOperator(
        task_id='check_feed_updates',
        python_callable=_check_feed_updates
    )

    stop_execution = DummyOperator(
        task_id='stop_dag_execution'
    )

    process_updates = PythonOperator(
        task_id='process_updates',
        python_callable=_process_updates
    )

    fetch_feed_updates >> [stop_execution, process_updates]