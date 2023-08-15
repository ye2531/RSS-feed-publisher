from modules.settings import NAMED_ENTITY
import newspaper
import logging 

logger = logging.getLogger(__name__)

def extract_article_text(url):
    article = newspaper.Article(url)
    try:
        article.download()
        article.parse()

        return article.text, article.title
    
    except Exception as e:
        logger.error(f"Cannot download article {url} \n {str(e)}")
        return None

def process_updates(**context):
    
    entries = context["ti"].xcom_pull(task_ids="check_feed_updates", key="new_entries")

    entries_clean = []

    for entry in entries:
        entry_text, entry_title = extract_article_text(entry["link"])
        if entry_text and (NAMED_ENTITY in entry_text or NAMED_ENTITY in entry["title"]):
            entry["title"] = entry_title
            entry["text"] = entry_text
            entries_clean.append(entry)

    if not entries_clean:
        return "stop_dag_no_articles"
    else:
        context["ti"].xcom_push(key="new_entries_count", value=len(entries_clean))
        context["ti"].xcom_push(key="new_entries_clean", value=entries_clean)
        return "save_and_publish"