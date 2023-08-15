from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.models import Variable
from modules.settings import NAMED_ENTITY, CHAT_ID
from modules.feed_handler import fetch_feed_updates
from modules.article_handler import process_updates
from utils.common_utils import get_dags_folder_path
from datetime import datetime, timedelta
import os


def replace_apostrophes(text):
    return text.replace("'", "''")
    
def _generate_sql_statements(**context):

    entries = context["ti"].xcom_pull(task_ids="process_updates", key="new_entries_clean")

    sql_statements = ""

    for entry in entries:
         sql_statements += "INSERT INTO articles(published, title, link, article_text, dag_run_time) VALUES (" \
                          f"\'{entry['published']}\', \'{replace_apostrophes(entry['title'])}\', \'{entry['link']}\',\'{replace_apostrophes(entry['text'])}\', \'{context['ts']}\'" \
                           ");\n"
         
    sql_file_path = os.path.join(get_dags_folder_path(), "sql/postgres_query.sql")

    with open(sql_file_path, "w") as f:
            f.write(sql_statements)

def _generate_telegram_message(**context):

    entries = context["ti"].xcom_pull(task_ids="process_updates", key="new_entries_clean")

    article_plural = "articles" if len(entries) > 1 else "article"
    message = f"{NAMED_ENTITY} was mentioned in the following {article_plural}:\n\n"

    for entry in entries:
            message += f"[{entry['title']}]({entry['link']})\n\n"

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
        python_callable=fetch_feed_updates
    )

    stop_dag_no_updates = DummyOperator(
        task_id='stop_dag_no_updates'
    )

    process_updates = BranchPythonOperator(
        task_id='process_updates',
        python_callable=process_updates
    )

    generate_sql_statements = PythonOperator(
        task_id='generate_sql_statements',
        python_callable=_generate_sql_statements
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
        telegram_kwargs={"parse_mode":"MarkDown"}
    )

    fetch_feed_updates >> [stop_dag_no_updates, process_updates]
    process_updates >> [stop_dag_no_articles, save_and_publish]
    save_and_publish >> [generate_sql_statements, generate_telegram_message]
    generate_sql_statements >> save_new_entries
    generate_telegram_message >> publish_new_entries