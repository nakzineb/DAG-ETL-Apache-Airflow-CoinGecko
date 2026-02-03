"""
extract.py
Extraction des données depuis l'API CoinGecko
"""

import requests
import json
import os
from datetime import datetime

# Configuration
API_URL = "https://api.coingecko.com/api/v3/coins/markets"
PARAMS = {
    "vs_currency": "eur",       # Prix en euros
    "order": "market_cap_desc", # Trié par capitalisation
    "per_page": 10,             # Top 10 cryptomonnaies
    "page": 1,
    "sparkline": False
}

# Dossier de sortie pour les données brutes
OUTPUT_DIR = "/opt/airflow/data/raw"


def extraire_données():
    """
    Fonction principale d'extraction.
    Récupère les données de l'API et les sauvegarde en JSON.
    """
    # Créer le dossier si il n'existe pas
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Générer un nom de fichier avec la date
    date_aujourd_hui = datetime.now().strftime("%Y-%m-%d")
    fichier_sortie = os.path.join(OUTPUT_DIR, f"coingecko_raw_{date_aujourd_hui}.json")

    try:
        # Appel à l'API CoinGecko
        print(f"[EXTRACT] Appel à l'API CoinGecko...")
        response = requests.get(API_URL, params=PARAMS, timeout=30)

        # Vérifier que la requête a réussi
        response.raise_for_status()

        # Convertir en JSON
        données = response.json()

        # Vérifier qu'on a bien reçu des données
        if not données:
            raise ValueError("L'API a retourné des données vides !")

        # Sauvegarder en fichier JSON
        with open(fichier_sortie, "w", encoding="utf-8") as f:
            json.dump(données, f, indent=4, ensure_ascii=False)

        print(f"[EXTRACT] ✅ {len(données)} cryptomonnaies extraites")
        print(f"[EXTRACT] ✅ Données sauvegardées dans : {fichier_sortie}")

        return fichier_sortie  # Retourner le chemin du fichier pour la tâche suivante

    except requests.exceptions.ConnectionError:
        print("[EXTRACT] ❌ Erreur de connexion à l'API !")
        raise
    except requests.exceptions.Timeout:
        print("[EXTRACT] ❌ Timeout lors de l'appel à l'API !")
        raise
    except requests.exceptions.HTTPError as e:
        print(f"[EXTRACT] ❌ Erreur HTTP : {e}")
        raise


if __name__ == "__main__":
    extraire_données()
