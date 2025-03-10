## Chargement des librairies et des variables d'environnement

from dotenv import load_dotenv  # Pour charger les variables d'environnement depuis un fichier .env
import os  # Pour interagir avec le système d'exploitation (accès aux variables d'environnement)
import pandas as pd  # Pour manipuler des données sous forme de DataFrame (optionnel ici)
import mysql.connector  # Pour se connecter à une base de données MySQL
from datetime import datetime  # Pour manipuler des dates (optionnel ici)

### Paramètres et fonction de connexion à la base de données

def sql_cnx(list, create_cursor=True):
    """
    Fonction de connexion à une base SQL. Prend une liste en entrée avec :
        [host, database, user, password]
        Fonctionne avec mysql.connector
        
        Si create_cursor = True, renvoie également un curseur.
        
        Args:
            list (list): Liste contenant les informations de connexion [host, database, user, password].
            create_cursor (bool): Si True, retourne également un curseur pour exécuter des requêtes.
        
        Returns:
            cnx: Objet de connexion à la base de données.
            cursor (optionnel): Curseur pour exécuter des requêtes SQL.
    """
    try:
        # Connexion à la base de données MySQL
        cnx = mysql.connector.connect(
            host=list[0],  # Hôte de la base de données
            database=list[1],  # Nom de la base de données
            user=list[2],  # Nom d'utilisateur
            password=list[3]  # Mot de passe
        )
        
        # Vérification de la connexion
        if cnx.is_connected():
            print("Connecté au serveur MySQL. Version : ", cnx.get_server_info())
            cursor = cnx.cursor()
            cursor.execute("SELECT database();")  # Récupère le nom de la base de données actuelle
            print("Connecté à la base de données : ", cursor.fetchone())
        
        return cnx, cursor if create_cursor else cnx
    except Exception as e:
        print("Une erreur est survenue lors de la connexion à la base de données : \n", e)
        return None, None

# Chargement des variables d'environnement depuis le fichier .env
load_dotenv('var.env')

# Récupération des informations de connexion depuis les variables d'environnement
host = os.getenv('host')  # Hôte de la base de données
database = os.getenv('database')  # Nom de la base de données
user = os.getenv('user')  # Nom d'utilisateur
mdp = os.getenv("mdp")  # Mot de passe

# Configuration de la connexion à la base de données
immo_cnx = [host, database, user, mdp]

## Lecture des données dans une table de la base de données immobilier

# Demander à l'utilisateur de spécifier la table à interroger
table = input("Indiquer la table à lire : ")

# Connexion à la base de données
cnx, cursor = sql_cnx(immo_cnx)

if cnx and cursor:  # Vérifier que la connexion et le curseur sont bien initialisés
    try:
        # Construction de la requête SQL pour sélectionner toutes les données de la table spécifiée
        query = f"SELECT * FROM {table}"
        
        # Exécution de la requête
        cursor.execute(query)
        
        # Affichage des résultats
        print(f"\nContenu de la table '{table}' :")
        for row in cursor:
            print(row)  # Affiche chaque ligne de la table
        
    except Exception as e:
        print(f"Une erreur est survenue lors de la lecture de la table '{table}' : \n", e)
    
    finally:
        # Fermeture du curseur et de la connexion
        cursor.close()
        cnx.close()
        print("\nConnexion à la base de données fermée.")
else:
    print("La connexion à la base de données a échoué. Veuillez vérifier les informations de connexion.")