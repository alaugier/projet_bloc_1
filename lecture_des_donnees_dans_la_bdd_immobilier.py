## Chargement des librairies et des variables d'environnement

from dotenv import load_dotenv
import os
import math
import pandas as pd
import numpy as np
import mysql.connector
from datetime import datetime
import csv

### Paramètres et fonction de connexion à la base de données
def sql_cnx(list, create_cursor=True):
    """Fonction de connexion à une base SQL. Prend une liste en entrée avec :
        [host, database, user, password]
        Fonctionne avec mysql.connector
        
        Si create_cursor = True, renvoie également un curseur.
        
        return cnx, cursor(optionnel)"""
    try:
        cnx = mysql.connector.connect(host=list[0],
                                     database=list[1],
                                     user=list[2],
                                     password=list[3])
        if cnx.is_connected():
            print("Connecté au serveur MySQL. Version : ", cnx.get_server_info())
            cursor = cnx.cursor()
            cursor.execute("select database();")
            print("Connecté à la base de donnée : ", cursor.fetchone())
        return cnx, cursor
    except Exception as e:
        print("Une erreur est survenue : \n", e)

load_dotenv('var.env')
host = os.getenv('host')
database = os.getenv('database')
user = os.getenv('user')
mdp = os.getenv("mdp")

immo_cnx = [host, database, user, mdp]

input("Indiquer la table :")
table = input()

## Lecture des données dans une table de la base de données immobilier

# Connexion
cnx, cursor = sql_cnx(immo_cnx)

query = f"SELECT * FROM {table}"
cursor.execute(query)

for (x) in cursor:
    print ("name = " + format(x))
cursor.close()
cnx.close()