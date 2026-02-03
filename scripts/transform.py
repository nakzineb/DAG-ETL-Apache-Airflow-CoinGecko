"""
transform.py
Transformation et nettoyage des données extraites
"""

import json
import csv
import os
from datetime import datetime

# Dossiers
INPUT_DIR = "/opt/airflow/data/raw"
OUTPUT_DIR = "/opt/airflow/data/processed"


def transformer_données():
    """
    Fonction principale de transformation.
    Lit le JSON brut et nettoie/formate les données.
    """
    # Créer le dossier de sortie
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Générer les noms de fichiers avec la date
    date_aujourd_hui = datetime.now().strftime("%Y-%m-%d")
    fichier_entrée = os.path.join(INPUT_DIR, f"coingecko_raw_{date_aujourd_hui}.json")
    fichier_sortie = os.path.join(OUTPUT_DIR, f"coingecko_transformed_{date_aujourd_hui}.json")

    # Vérifier que le fichier d'entrée existe
    if not os.path.exists(fichier_entrée):
        raise FileNotFoundError(f"[TRANSFORM] ❌ Fichier introuvable : {fichier_entrée}")

    print(f"[TRANSFORM] Lecture du fichier : {fichier_entrée}")

    # Lire les données brutes
    with open(fichier_entrée, "r", encoding="utf-8") as f:
        données_brutes = json.load(f)

    # Liste pour stocker les données transformées
    données_transformées = []

    for crypto in données_brutes:
        # Extraire uniquement les champs nécessaires et les renommer
        crypto_nettoyée = {
            "id": crypto.get("id", "inconnu"),
            "nom": crypto.get("name", "inconnu"),
            "symbole": crypto.get("symbol", "inconnu").upper(),
            "prix_eur": round(crypto.get("current_price", 0), 2),
            "capitalisation_marché": crypto.get("market_cap", 0),
            "rang_marché": crypto.get("market_cap_rank", 0),
            "variation_24h": round(crypto.get("price_change_percentage_24h", 0), 2),
            "prix_max_24h": round(crypto.get("high_24h", 0), 2),
            "prix_min_24h": round(crypto.get("low_24h", 0), 2),
            "volume_24h": crypto.get("total_volume", 0),
            "date_extraction": date_aujourd_hui
        }
        données_transformées.append(crypto_nettoyée)

    # Sauvegarder les données transformées
    with open(fichier_sortie, "w", encoding="utf-8") as f:
        json.dump(données_transformées, f, indent=4, ensure_ascii=False)

    print(f"[TRANSFORM] ✅ {len(données_transformées)} cryptomonnaies transformées")
    print(f"[TRANSFORM] ✅ Données sauvegardées dans : {fichier_sortie}")

    return fichier_sortie


if __name__ == "__main__":
    transformer_données()
