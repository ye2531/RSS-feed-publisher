from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from settings import RSS_FEED_URL, NAMED_ENTITY, CHAT_ID
from airflow.models import Variable
import newspaper
import feedparser
import logging
import re


logger = logging.getLogger(__name__)

def parse_entry_url(url):
    return re.search(r"url=([^&]+)", url).group(1)

def string_to_date(date):
    return datetime.strptime(date, "%Y-%m-%dT%H:%M:%SZ")

def replace_apostrophes(text):
    return text.replace("'", "''")

def _check_feed_updates(**context):
    
    start_time = context["ts"]

    logger.info(f"Fetching updates for {start_time}")
    feed = feedparser.parse(RSS_FEED_URL)
    logger.info(f"Fetched {len(feed.entries)} entries from the feed")

    update_time = string_to_date(feed.feed.updated)

    with open("dags/temp/update_time.txt", "r+") as file:
        last_update_time = string_to_date(file.read())

        if update_time > last_update_time:
            file.seek(0)
            file.write(feed.feed.updated)
            file.truncate()

            new_entries = [{"title": entry.title,
                            "link": parse_entry_url(entry.link),
                            "published": string_to_date(entry.published)} for entry in feed.entries \
                                if string_to_date(entry.published) > last_update_time]
            
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
        logger.error(f"Cannot download article {url} \n {str(e)}")
        return None

def _process_updates(**context):
    
    entries = context["ti"].xcom_pull(task_ids="check_feed_updates", key="new_entries")

    entries_clean = []

    with open("dags/sql/postgres_query.sql", "w") as f:
        for entry in entries:
            entry_text = _extract_article_text(entry["link"])
            if entry_text and NAMED_ENTITY in entry_text:

                f.write(
                    "INSERT INTO articles(published, title, link, article_text, dag_run_time) VALUES ("
                    f"\'{entry['published']}\', \'{replace_apostrophes(entry['title'])}\', \'{entry['link']}\',\'{replace_apostrophes(entry_text)}\', \'{context['ts']}\'"
                    ");\n"
                )

                entries_clean.append(entry)

    if not entries_clean:
        return "stop_dag_no_articles"
    else:
        context["ti"].xcom_push(key="new_entries_count", value=len(entries_clean))
        context["ti"].xcom_push(key="new_entries_clean", value=entries_clean)
        return "save_and_publish"

def _generate_telegram_message(**context):

    entries = context["ti"].xcom_pull(task_ids="process_updates", key="new_entries_clean")

    if len(entries) > 1:
        message = f"{NAMED_ENTITY} was mentioned in the following articles:\n\n"
        for entry in entries:
            message += f"[{entry['title']}]({entry['link']})\n\n"
        context["ti"].xcom_push(key="telegram_message", value=message)
    else: 
        message = f"{NAMED_ENTITY} was mentioned in the following article:\n\n*{entries[0]['title']}*\n\n{entries[0]['link']}"
        context["ti"].xcom_push(key="telegram_message", value=message)

with DAG(
    dag_id="news_dag",
    schedule_interval="*/2 * * * *",
    start_date=datetime.now() - timedelta(minutes=2),
    template_searchpath="/opt/airflow/dags/sql",
    max_active_runs=1,
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

    save_new_entries = PostgresOperator(
        task_id="save_new_entries",
        postgres_conn_id="postgres_default",
        sql=["articles_schema.sql", "postgres_query.sql"]
    )

    generate_telegram_message = PythonOperator(
        task_id="generate_telegram_message",
        python_callable=_generate_telegram_message
    )

    publish_new_entries = TelegramOperator(
        task_id="publish_new_entries",
        token=Variable.get("TELEGRAM_BOT_TOKEN"),
        chat_id=CHAT_ID,
        text="{{ task_instance.xcom_pull(task_ids='generate_telegram_message', key='telegram_message') }}",
        telegram_kwargs={
            "parse_mode":"MarkDown",
            "disable_web_page_preview":"False" if "{{ task_instance.xcom_pull(task_ids='process_updates', key='new_entries_count') }}" == 1 else "True"
        }
    )

    fetch_feed_updates >> [stop_dag_no_updates, process_updates]
    process_updates >> [stop_dag_no_articles, save_and_publish]
    save_and_publish >> [save_new_entries, generate_telegram_message]
    generate_telegram_message >> publish_new_entries