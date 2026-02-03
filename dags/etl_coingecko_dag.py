"""
etl_coingecko_dag.py
DAG ETL pour extraire, transformer et charger les données CoinGecko
Exécution quotidienne à 12h
"""

import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# le dossier scripts au chemin Python
sys.path.insert(0, "/usr/local/airflow/scripts")

from extract import extraire_données
from transform import transformer_données
from load import charger_données

# ─────────────────────────────────────
# Configuration du DAG
# ─────────────────────────────────────
default_args = {
    "owner": "zineb",
    "depends_on_past": False,
    "start_date": datetime(2026, 2, 2),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,                          # 3 tentatives en cas d'échec
    "retry_delay": timedelta(minutes=5),   # 5 minutes entre les tentatives
}

dag = DAG(
    dag_id="etl_coingecko",
    default_args=default_args,
    description="Pipeline ETL quotidien - Données CoinGecko",
    schedule_interval="0 12 * * *",  # Quotidien à 12h (format CRON)
    catchup=False,
    tags=["etl", "coingecko", "crypto"],
)

# ─────────────────────────────────────
# Tâche 0 : Vérification de la connexion API
# ─────────────────────────────────────
tache_verifie_api = BashOperator(
    task_id="verifier_api",
    bash_command="curl -s -o /dev/null -w '%{http_code}' https://api.coingecko.com/api/v3/ping",
    dag=dag,
)

# ─────────────────────────────────────
# Tâche 1 : Extraction
# ─────────────────────────────────────
tache_extract = PythonOperator(
    task_id="extraction_coingecko",
    python_callable=extraire_données,
    dag=dag,
)

# ─────────────────────────────────────
# Tâche 2 : Transformation
# ─────────────────────────────────────
tache_transform = PythonOperator(
    task_id="transformation_données",
    python_callable=transformer_données,
    dag=dag,
)

# ─────────────────────────────────────
# Tâche 3 : Chargement
# ─────────────────────────────────────
tache_load = PythonOperator(
    task_id="chargement_données",
    python_callable=charger_données,
    dag=dag,
)

# ─────────────────────────────────────
# Tâche 4 : Notification de fin
# ─────────────────────────────────────
tache_fin = BashOperator(
    task_id="pipeline_termine",
    bash_command='echo "✅ Pipeline ETL CoinGecko terminé avec succès le $(date)"',
    dag=dag,
)

# ─────────────────────────────────────
# Définir les dépendances (l'ordre d'exécution)
# ─────────────────────────────────────
tache_verifie_api >> tache_extract >> tache_transform >> tache_load >> tache_fin

