from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    "owner":            "Nisrine",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

dag = DAG(
    dag_id            = "pipeline_books",
    description       = "Scraping + Bronze→Silver + Silver→Gold + Streamlit",
    default_args      = default_args,
    schedule_interval = "0 2 * * *",
    start_date        = days_ago(1),
    catchup           = False,
    tags              = ["bigdata", "books", "emsi"],
)

SCRIPTS_DIR = "/opt/airflow/scripts"

scrape_books_toscrape = BashOperator(
    task_id      = "scrape_books_toscrape",
    bash_command = "cd /opt/airflow && PYTHONPATH=/opt/airflow scrapy crawl books_toscrape",
    dag          = dag,
)

scrape_openlibrary = BashOperator(
    task_id      = "scrape_openlibrary",
    bash_command = "cd /opt/airflow && PYTHONPATH=/opt/airflow scrapy crawl books_openlibrary",
    dag          = dag,
)

bronze_to_silver = BashOperator(
    task_id      = "bronze_to_silver",
    bash_command = f"python {SCRIPTS_DIR}/bronze_to_silver.py",
    dag          = dag,
)

silver_to_gold = BashOperator(
    task_id      = "silver_to_gold",
    bash_command = f"python {SCRIPTS_DIR}/silver_to_gold.py",
    dag          = dag,
)

gold_to_mysql = BashOperator(
    task_id      = "gold_to_mysql",
    bash_command = f"python {SCRIPTS_DIR}/load_to_mysql.py",
    dag          = dag,
)

refresh_streamlit = BashOperator(
    task_id      = "refresh_streamlit",
    bash_command = "docker restart streamlit || true",
    dag          = dag,
)

# ─── ORDRE D'EXÉCUTION ───────────────────────────────────────────────────────

[scrape_books_toscrape, scrape_openlibrary] >> bronze_to_silver >> silver_to_gold >> gold_to_mysql >> refresh_streamlit