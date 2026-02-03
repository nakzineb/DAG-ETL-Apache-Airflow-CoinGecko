"""
load.py
Chargement des données transformées en CSV (format analytique)
"""

import json
import csv
import os
from datetime import datetime

# Dossiers
INPUT_DIR = "/opt/airflow/data/processed"
OUTPUT_DIR = "/opt/airflow/data/processed"


def charger_données():
    """
    Fonction principale de chargement.
    Convertit le JSON transformé en CSV pour l'analyse.
    """
    # Générer les noms de fichiers avec la date
    date_aujourd_hui = datetime.now().strftime("%Y-%m-%d")
    fichier_entrée = os.path.join(INPUT_DIR, f"coingecko_transformed_{date_aujourd_hui}.json")
    fichier_sortie = os.path.join(OUTPUT_DIR, f"coingecko_final_{date_aujourd_hui}.csv")

    # Vérifier que le fichier d'entrée existe
    if not os.path.exists(fichier_entrée):
        raise FileNotFoundError(f"[LOAD] ❌ Fichier introuvable : {fichier_entrée}")

    print(f"[LOAD] Lecture du fichier : {fichier_entrée}")

    # Lire les données transformées
    with open(fichier_entrée, "r", encoding="utf-8") as f:
        données = json.load(f)

    if not données:
        raise ValueError("[LOAD] ❌ Pas de données à charger !")

    # Définir les colonnes du CSV
    colonnes = données[0].keys()

    # Sauvegarder en CSV
    with open(fichier_sortie, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=colonnes)
        writer.writeheader()
        writer.writerows(données)

    print(f"[LOAD] ✅ Données chargées en CSV : {fichier_sortie}")
    print(f"[LOAD] ✅ {len(données)} lignes écrites")

    return fichier_sortie


if __name__ == "__main__":
    charger_données()

