import logging
import newspaper
from modules.settings import NAMED_ENTITY


logger = logging.getLogger(__name__)


def extract_article_text(url):
    """
    Extracts text and title from a given article URL.

    Args:
        url (str): The URL of the article.

    Returns:
        tuple: A tuple containing the extracted article text and title. If extraction
               fails, returns None.
    """
    article = newspaper.Article(url)
    try:
        article.download()
        article.parse()
        return article.text, article.title

    except Exception as e:
        logger.error(f"Cannot download article {url} \n {str(e)}")
        return None


def process_updates(**context):
    """
    Processes updates for new entries and filters them based on named entity presence.

    Args:
        context (dict): The task context dictionary containing information for task execution.

    Returns:
        str: Returns "stop_dag_no_articles" if no clean entries are found, or "save_and_publish" if
             clean entries are present. Pushes the count and clean entries to XCom.
    """
    entries = context["ti"].xcom_pull(task_ids="check_feed_updates", key="new_entries")

    entries_clean = []

    for entry in entries:
        entry_text, entry_title = extract_article_text(entry["link"])
        if entry_text and (
            NAMED_ENTITY in entry_text or NAMED_ENTITY in entry["title"]
        ):
            entry["title"] = entry_title
            entry["text"] = entry_text
            entries_clean.append(entry)

    if not entries_clean:
        return "stop_dag_no_articles"
    else:
        context["ti"].xcom_push(key="new_entries_count", value=len(entries_clean))
        context["ti"].xcom_push(key="new_entries_clean", value=entries_clean)
        return "save_and_publish"