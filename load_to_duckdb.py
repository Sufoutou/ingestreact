import duckdb
import os

# Définir le chemin vers le dossier contenant les fichiers Parquet
data_folder = "C:/Users/romai/Documents/data"  # Remplacez par le chemin correct si nécessaire

# Vérifier si le dossier existe
if not os.path.exists(data_folder):
    raise FileNotFoundError(f"Le dossier {data_folder} n'existe pas. Vérifiez le chemin.")

# Connexion à DuckDB
conn = duckdb.connect("react_data_lake.duckdb")

# Chargement des fichiers Parquet dans DuckDB
print("Chargement des données dans DuckDB...")

# Charger chaque fichier Parquet avec son chemin complet
repo_file = os.path.join(data_folder, "repo.parquet")
issues_file = os.path.join(data_folder, "issues.parquet")
pulls_file = os.path.join(data_folder, "pulls.parquet")

# Vérifier si les fichiers existent avant de tenter de les charger
if not os.path.exists(repo_file):
    raise FileNotFoundError(f"Fichier {repo_file} introuvable.")
if not os.path.exists(issues_file):
    raise FileNotFoundError(f"Fichier {issues_file} introuvable.")
if not os.path.exists(pulls_file):
    raise FileNotFoundError(f"Fichier {pulls_file} introuvable.")

# Fonction pour recréer une table uniquement si elle existe déjà
def create_or_replace_table(table_name, file_path):
    existing_tables = conn.execute("SHOW TABLES").fetchall()
    if (table_name,) in existing_tables:
        conn.execute(f"DROP TABLE {table_name}")
        print(f"Table '{table_name}' supprimée.")
    conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{file_path}')")
    print(f"Table '{table_name}' créée.")

# Créer ou remplacer les tables avec le préfixe raw_
create_or_replace_table("raw_repo", repo_file)
create_or_replace_table("raw_issues", issues_file)
create_or_replace_table("raw_pulls", pulls_file)

print("Données chargées dans DuckDB.")

# Afficher les tables disponibles dans la base de données
print("\nTables disponibles dans la base de données :")
tables = conn.execute("SHOW TABLES").fetchall()
for table in tables:
    print(table[0])

# Exemple de requête pour afficher les 5 premières lignes de chaque table
for table in tables:
    print(f"\nExemple de données dans la table {table[0]} :")
    print(conn.execute(f"SELECT * FROM {table[0]} LIMIT 5").fetchdf())
