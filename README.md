# Project ETL

## Introduction
Le but est de développer une ETL pour comprendre et suivre la présence des lions (Panthera leo (Linnaeus, 1758)) sur Terre. 

La source de données provient du site web https://www.gbif.org/.


## Environnement de Développement
- Système d'exploitation: `MacOS 14.2.1`
- Environnement `Anaconda 2022.10` (avec `Python 3.9.13`)
- Autres outils : `Airflow`, `Git`
- Les informations de connexion de l'utilisateur pour GBIF sont stockées dans le fichier `bash_profile` local

## Technologie utilisée
Dans ce projet ETL, nous utilisons :
- Pandas pour l'analyse de données.
- Spark SQL pour le traitement des données structurées.
- PySpark pour écrire des scripts ETL.
- Plotly pour la visualisation des données.
- Airflow pour l'automatisation et la planification des scripts.

## Installation des Dépendances
les packages ou bibliothèques tiers nécessaires au projet:
- pyspark
- missingno
- pygbif

## Structure du Projet
```
ETL_Panthera_leo/
│
├── config/ 
│   └── config.yaml       # Fichier de configuration
│
├── logs/ 
│   └── etl.log           # log file
├── data/
│
├── data_temp/
│
├── describe_data.ipynb   # Analyse de données
│
├── src/        
│   ├── main.py           # Programme principal
│   ├── logger.py         # Module de configuration des logs
│   ├── config.py         # Module de chargement de la configuration
│   ├── extract.py        # Module d'extraction de données
│   ├── transform.py      # Module de transformation de données
│   ├── load.py           # Module de chargement de données
│   ├── map_data.py       # Module de visualisation des données
│   └── etl_dag.py        # Module de configuration d'Airflow
│
└── tests/ 
    ├── test_extract.py   # Les tests du module d'extraction
    └── test_transform.py # Les tests du module de transformation
```

## Comment Exécuter
### Exécuter le programme principal ETL
1. Modifiez le path du code ETL dans le fichier `config.yaml`.
2. Ajoutez les variables d'information de compte GBIF dans votre fichier `bash_profile` local.
3. Exécutez le programme `main.py`.
4. Si c'est la première fois que vous exécutez le programme, modifiez la valeur de `occurrences` dans le fichier de configuration en mettant `0`.

### Exécuter le programme de visualisation
1. Modifiez le path vers les données que vous souhaitez consulter dans le fichier `config.yaml`.
2. Exécutez le programme `map_data.py`, la fenêtre de la carte s'ouvrira dans votre navigateur par défaut système.

### Exécutez le DAG d'Airflow
1. Modifiez le path du script.
2. Placez le script DAG dans le répertoire "airflow/dag".
3. Ajouter des variables pour indiquer le path de script ETL dans Airflow Web UI.
4. Démarrer Airflow et activer le script DAG dans Airflow Web UI.


## Étapes du Projet
Les étapes de mise en œuvre de ce projet sont les suivantes :
1. Parcourir le site web et rechercher la source de données.
2. Rechercher le dictionnaire des données et les outils API fournis par le site web.
3. Télécharger manuellement les données, les examiner dans un cahier Jupyter (Jupyter Notebook) pour déterminer la stratégie de nettoyage des données. 
   1. Examiner le degré de manquement des données à l'aide de Missingno et Pandas.
   2. En fonction du dictionnaire des données du site web et des informations fournies par le tableau disponible sur https://www.gbif.org/occurrence/search?taxon_key=5219404, conserver les données valides.
   3. Examiner les statistiques des données après suppression en utilisant la fonction "describe". Identifier des opportunités de nettoyage supplémentaire pour les colonnes "scientificName" et "occurrenceID".
   4. Traiter les valeurs manquantes. Pour la colonne "IndividualCount", remplir les valeurs manquantes avec 1 lorsque des informations de coordonnées sont disponibles.
4. Écrire le programme principal ETL (Extract, Transform, Load).
5. Effectuer les tests.
6. Élaborer une stratégie de mise à jour des données.
   1. À partir de l'analyse des données, il est constaté qu'il n'y a pas de clé primaire incrémentielle ni d'horodatage des téléchargements. Étant donné la petite taille des données, obtenir l'ensemble complet de données à chaque mise à jour.
   2. Stocker les données transformées dans des fichiers CSV et conserver les anciens fichiers plutôt que de les écraser à chaque mise à jour.
   3. Enregistrer le nombre total d'occurrences dans le fichier de configuration. À chaque démarrage du programme, obtenir le nouveau nombre total d'occurrences depuis l'API et quitter automatiquement si les données n'ont pas changé.
   Utiliser Apache Airflow pour exécuter le script ETL de manière périodique. La fréquence actuelle d'exécution est une fois par jour, mais elle peut être ajustée selon les besoins futurs.
   Écrire le programme de visualisation des données en utilisant la bibliothèque Plotly et la fonction density_mapbox pour afficher les données sur une carte interactive. Les utilisateurs peuvent faire glisser la barre de temps pour visualiser l'évolution des données.

## Exécution dans le Cloud AWS
Si vous souhaitez déployer ce programme dans le cloud pour une utilisation à grande échelle, voici quelques étapes générales que vous pouvez suivre :
1. Remplacer le script Python par `Lambda Function`.
2. Utiliser `EventBridge` pour planifier l'exécution de Lambda Function à intervalles réguliers, en remplacement d'Airflow.
3. Stocker les données traitées dans `S3`.
4. Utiliser `Athena` pour interroger les données stockées dans S3 et visualiser les résultats dans `QuickSight`.
