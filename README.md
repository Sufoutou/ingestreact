# ingestreact

## Collecte de données
- API REST GitHub : Fournit des points de terminaison pour accéder aux informations sur les dépôts, issues, pull requests, etc. (Documentation : GitHub REST API)

**Bibliothèques Python requises:**
- `requests` : Pour interagir avec l'API REST
- `pandas` et `pyarrow` : Pour transformer et stocker les données au format Parquet
- `duckdb` : Pour charger les données dans une base DuckDB

# Données identifiées

## Dépôt GitHub
- Nom du dépôt
- Description
- Nombre d'étoiles (stars)
- Nombre de forks
- Nombre de clones

## Activité des issues
- Nombre d'issues ouvertes/fermées
- Temps moyen pour résoudre une issue
- Liste des contributeurs ayant interagi sur les issues

## Pull Requests (PR)
- Nombre de PR soumises, fusionnées ou rejetées
- Temps moyen pour fusionner une PR
- Liste des contributeurs ayant soumis des PR

## Trafic GitHub Insights
- Visiteurs uniques
- Pages vues

## Contributeurs
- Nom du contributeur
- Localisation géographique (si disponible)
- Rôle (mainteneur/contributeur)

## Tests et qualité du code
- Couverture des tests (%)
- Nombre de bugs signalés/résolus
- Temps moyen pour corriger un bug critique

## Performances Web Vitals
- Largest Contentful Paint (LCP)
- Time to Interactive (TTI)
- Cumulative Layout Shift (CLS)
