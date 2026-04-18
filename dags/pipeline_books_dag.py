from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

#configuration du DAG
default_args = {
    "owner":            "Nisrine",
    "depends_on_past":  False,               
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,                   # 2 tentatives si échec
    "retry_delay":      timedelta(minutes=5),
}

dag = DAG(
    dag_id          = "pipeline_books",
    description     = "Scraping + Bronze→Silver + Silver→Gold",
    default_args    = default_args,
    schedule_interval = "0 2 * * *",         # everyday at 2utc (3am)
    start_date      = days_ago(1),
    catchup         = False,                 # ne pas rejouer les runs passés
    tags            = ["bigdata", "books", "emsi"],
)

#chemins dans le conteneur Airflow
#les scripts et le projet Scrapy sont montés dans /opt/airflow/
SCRIPTS_DIR = "/opt/airflow/scripts"
SCRAPY_DIR  = "/opt/airflow/scrapy_project"


# ÉTAPE 1 — Scraping des 2 sources
# Chaque spider écrit ses JSON dans MinIO Bronze automatiquement
# via les pipelines Scrapy (MinIOPipeline + KafkaPipeline)


scrape_books_toscrape = BashOperator(
    task_id         = "scrape_books_toscrape",
    bash_command    = f"cd {SCRAPY_DIR} && scrapy crawl books_toscrape",
    dag             = dag,
)

scrape_openlibrary = BashOperator(
    task_id         = "scrape_openlibrary",
    bash_command    = f"cd {SCRAPY_DIR} && scrapy crawl books_openlibrary",
    dag             = dag,
)

#scrape_google_books = BashOperator(
#    task_id         = "scrape_google_books",
#    bash_command    = f"cd {SCRAPY_DIR} && scrapy crawl books_google",
#    dag             = dag,
#)

# ÉTAPE 2 — Bronze → Silver
# Ne démarre qu'une fois les 3 scrapers terminés.
# Lit tout le Bronze (tous les runs passés), déduplique,
# nettoie, et écrit le Silver en Parquet partitionné.

bronze_to_silver = BashOperator(
    task_id         = "bronze_to_silver",
    bash_command    = f"python {SCRIPTS_DIR}/bronze_to_silver.py",
    dag             = dag,
)

# ÉTAPE 3 — Silver → Gold
# Produit les 5 tables analytiques pour Superset

silver_to_gold = BashOperator(
    task_id         = "silver_to_gold",
    bash_command    = f"python {SCRIPTS_DIR}/silver_to_gold.py",
    dag             = dag,
)



#ordre d'exécution

# scrape_books_toscrape
# scrape_openlibrary    
# bronze_to_silver ─> silver_to_gold
# Les 2 scrapers tournent en parallèle.
# bronze_to_silver attend que les 2 soient finis.
# silver_to_gold attend que bronze_to_silver soit fini.


[scrape_books_toscrape, scrape_openlibrary] >> bronze_to_silver >> silver_to_gold