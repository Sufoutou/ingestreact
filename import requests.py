import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

# Configuration
GITHUB_TOKEN = "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"  # Remplacez par votre token
HEADERS = {"Authorization": f"Bearer {GITHUB_TOKEN}"}
BASE_URL = "https://api.github.com"

# Fonction pour récupérer les données d'un dépôt
def fetch_repo_data(owner, repo):
    url = f"{BASE_URL}/repos/{owner}/{repo}"
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    return response.json()

# Fonction pour récupérer les issues (limité à 100 issues par défaut)
def fetch_issues(owner, repo):
    url = f"{BASE_URL}/repos/{owner}/{repo}/issues?state=all&per_page=100"
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    return response.json()

# Fonction pour récupérer les pull requests (limité à 100 PR par défaut)
def fetch_pull_requests(owner, repo):
    url = f"{BASE_URL}/repos/{owner}/{repo}/pulls?state=all&per_page=100"
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    return response.json()

# Fonction pour nettoyer les données avant de les écrire en Parquet
def clean_data(data):
    df = pd.DataFrame(data)

    # Supprimer les colonnes contenant des types complexes ou vides
    for column in df.columns:
        if isinstance(df[column].iloc[0], (dict, list)):
            print(f"Suppression de la colonne complexe : {column}")
            df.drop(columns=[column], inplace=True)

    return df

# Fonction pour convertir les données en fichier Parquet
def save_to_parquet(data, filename):
    df = clean_data(data)  # Nettoyage des données avant conversion

    if df.empty:
        print(f"Aucune donnée disponible pour {filename}.")
        return

    table = pa.Table.from_pandas(df)  # Utilisation correcte de PyArrow
    pq.write_table(table, filename)

# Exemple d'utilisation
if __name__ == "__main__":
    owner = "facebook"
    repo = "react"

    # Création du dossier de sortie pour stocker les fichiers Parquet
    os.makedirs("data", exist_ok=True)

    # Récupération des données du dépôt
    print("Récupération des données du dépôt...")
    repo_data = fetch_repo_data(owner, repo)
    save_to_parquet([repo_data], "data/repo.parquet")

    # Récupération des issues
    print("Récupération des issues...")
    issues_data = fetch_issues(owner, repo)
    save_to_parquet(issues_data, "data/issues.parquet")

    # Récupération des pull requests
    print("Récupération des pull requests...")
    pulls_data = fetch_pull_requests(owner, repo)
    save_to_parquet(pulls_data, "data/pulls.parquet")

    print("Toutes les données ont été collectées et enregistrées au format Parquet dans le dossier 'data'.")
