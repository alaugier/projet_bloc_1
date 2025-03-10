## Projet Bloc 1 – Gestion et Analyse de Données Immobilières

Ce projet vise à automatiser l'extraction, le stockage et l'analyse de données immobilières en combinant plusieurs sources et technologies. Il comprend des scripts Python pour la collecte, le traitement et l'exploitation des données, ainsi qu'une API sécurisée pour interagir avec la base de données.

### Description des scripts
#### Extraction et récupération des données

    extraction_donnees.py : Scraping des annonces immobilières depuis le site ParuVendu.
    api_donnees_foncieres.py : Récupération des données foncières depuis l'API de la Cerema.
    wikidata_sparql_query.py : Extraction des données des villes situées dans un rayon de 50 km autour de Rennes via Wikidata SPARQL.

#### Stockage et gestion des données

    insertion_des_donnees_dans_la_bdd_immobilier.py : Insertion des données collectées dans la base de données immobilière.
    lecture_des_donnees_dans_la_bdd_immobilier.py : Lecture et extraction des données stockées dans la base de données immobilière.
    wikidata_api_dvf_mongodb_import_script.py : Importation et fusion des données extraites de l'API de la Cerema et de Wikidata dans MongoDB.

#### API et authentification

    authentification_api.py : Gestion de l'authentification à l'API locale et génération de la documentation interactive de l'API.
    Interroger_API_local.ipynb (Notebook Jupyter) : Interrogation de l'API locale via des requêtes POST, avec manipulation des résultats.

#### Installation et utilisation
##### Prérequis

    Python 3.9+
    Environnement virtuel (optionnel mais recommandé)
    Librairies : requests, beautifulsoup4, pymongo, mysql-connector-python, fastapi, uvicorn
    Base de données MySQL et MongoDB
    WSL (Windows Subsystem for Linux) pour l’automatisation avec crontab

##### Installation

    Cloner le projet

git clone https://github.com/alaugier/projet_bloc_1.git
cd projet_bloc_1

Créer et activer un environnement virtuel

python -m venv env
source env/bin/activate  # Mac/Linux
env\Scripts\activate     # Windows

Installer les dépendances

    pip install -r requirements.txt

##### Exécution des scripts

    Lancer le scraping des annonces immobilières

python extraction_donnees.py

Lancer l’API locale avec FastAPI

    uvicorn main:app --host 127.0.0.1 --port 8000 --reload

##### Automatisation

Les tâches d'extraction et de mise à jour des données sont planifiées via crontab sous WSL. Assurez-vous que le service cron est activé :

sudo service cron start

##### À venir

    Amélioration du modèle d’estimation des biens
    Intégration de nouvelles sources de données
    Déploiement de l’API sur un serveur distant