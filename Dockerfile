FROM apache/airflow:2.10.4

# Passer en root pour installer les packages
USER root

# Mettre à jour et installer les packages nécessaires
RUN apt-get update && apt-get install -y curl

# Revenir en user airflow
USER airflow

# Installer les packages Python nécessaires
RUN pip install --no-cache-dir requests pandas