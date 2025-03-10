## Chargement des librairies et des variables d'environnement

from dotenv import load_dotenv
import os
import math
import pandas as pd
import numpy as np
import mysql.connector
from datetime import datetime
import csv
from pymongo import MongoClient
from bson import ObjectId

# Paramètres et fonction de connexion à la base de données

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
    
load_dotenv('.env')
host = os.getenv('host')
database = os.getenv('database')
user = os.getenv('user')
mdp = os.getenv("mdp")

immo_cnx = [host, database, user, mdp]

## Aggrégation des données des logements de Rennes dans un rayon de 30km autour du centre-ville

# Dataset des logementsdf
df_immobilier = pd.read_csv("C:\\Users\\Utilisateur\\Projet_Bloc_1\\Archives\\csv_files_of_extracted_datas\\paru_vendu_ventes_logements_rennes_2025-3-2-23-6-21-852774.csv", index_col=0)

for i in range(len(df_immobilier)):
    if df_immobilier.loc[i, 'prix']==np.nan:
        df_immobilier = df_immobilier.drop(i)
df_immobilier = df_immobilier.reset_index(drop=True)

for i in range(len(df_immobilier)):
    if type(df_immobilier.loc[i,'prix'])!=str and math.isnan(df_immobilier.loc[i,'prix']):
       df_immobilier = df_immobilier.drop(i)
df_immobilier = df_immobilier.reset_index(drop=True)

for i in range(len(df_immobilier)):
    if '*' in df_immobilier.loc[i,'prix']:
        df_immobilier = df_immobilier.drop(i)
df_immobilier = df_immobilier.reset_index(drop=True)

for i in range(len(df_immobilier)):
    if df_immobilier.loc[i, 'surface habitable']==np.nan:
        df_immobilier.loc[i,'surface habitable'] = None

for i in range(len(df_immobilier)):
    if type(df_immobilier.loc[i,'surface habitable'])!=str and math.isnan(df_immobilier.loc[i, 'surface habitable']):
        df_immobilier.loc[i,'surface habitable'] = None

for i in range(len(df_immobilier)):
    if df_immobilier.loc[i,'Type de bien']=='maison':
        if df_immobilier.loc[i, 'surface terrain']==np.nan:
            df_immobilier.loc[i,'surface terrain'] = None

for i in range(len(df_immobilier)):
    if df_immobilier.loc[i,'Type de bien']=='maison':
        if type(df_immobilier.loc[i,'surface terrain'])!=str and math.isnan(df_immobilier.loc[i, 'surface terrain']):
            df_immobilier.loc[i,'surface terrain'] = None

for i in range(len(df_immobilier)):
    if math.isnan(df_immobilier.loc[i,'Code postal']):
        df_immobilier = df_immobilier.drop(i)
df_immobilier = df_immobilier.reset_index(drop=True)

for i in range(len(df_immobilier)):
    if math.isnan(df_immobilier.loc[i, 'nombre de pièces']):
        df_immobilier = df_immobilier.drop(i)
df_immobilier = df_immobilier.reset_index(drop=True)  

df_immobilier['Code postal'] = df_immobilier['Code postal'].astype('int')

df_immobilier['prix'] = df_immobilier['prix'].str.replace('€', '').str.replace(' ', '')
df_immobilier['prix'] = df_immobilier['prix'].astype('int')

df_immobilier['surface habitable'] = df_immobilier['surface habitable'].fillna('0').str.replace('m2 environ', '').str.replace('m2 Loi Carrez', '')
df_immobilier['surface habitable'] = df_immobilier['surface habitable'].astype('int')

for i in range(len(df_immobilier)):
    if df_immobilier.loc[i, 'surface habitable']!=0:
        df_immobilier.loc[i, 'prix au m²'] = int(df_immobilier.loc[i, 'prix']/df_immobilier.loc[i, 'surface habitable'])
    else:
        df_immobilier.loc[i, 'prix au m²'] = np.nan

df_immobilier['prix au m²'] = df_immobilier['prix au m²'].fillna('0').replace('0', None)

df_immobilier['surface habitable'] = df_immobilier['surface habitable'].replace('0', None)

df_immobilier['surface terrain'] = df_immobilier['surface terrain'].fillna('0').str.replace('m2environ', '').str.replace(' ', '')
df_immobilier['surface terrain'] = df_immobilier['surface terrain'].replace('0', None)

df_immobilier['nombre de pièces'] = df_immobilier['nombre de pièces'].astype('int')

df_immobilier['consommation énergétique'] = df_immobilier['consommation énergétique'].fillna('0').str.replace('kWh/m².an', '').str.replace(' ', '')
df_immobilier['consommation énergétique'] = df_immobilier['consommation énergétique'].replace('0', None)

df_immobilier['émission GPE'] = df_immobilier['émission GPE'].fillna('0').str.replace('kgCO2/m².an', '').str.replace(' ', '')
df_immobilier['émission GPE'] = df_immobilier['émission GPE'].replace('0', None)

df_immobilier["Mise à jour"] = df_immobilier["Mise à jour"].str.replace(' à ', ' ')
df_immobilier["Mise à jour"] = pd.to_datetime(df_immobilier["Mise à jour"], dayfirst=True)

for i in range(len(df_immobilier)):
    if df_immobilier.loc[i, "ville de l'annonceur"]=='':
        df_immobilier = df_immobilier.drop(i)
    if type(df_immobilier.loc[i, "ville de l'annonceur"])==float:
        df_immobilier = df_immobilier.drop(i)
df_immobilier = df_immobilier.reset_index(drop=True)

for col in df_immobilier.columns:
    if df_immobilier[col].dtype == 'string' or df_immobilier[col].dtype == object:
        df_immobilier[col] = df_immobilier[col].replace('', 'NC')

for c in df_immobilier.columns:
    if df_immobilier[c].dtype == object:
        print("convert ", df_immobilier[c].name, " to string")
        df_immobilier[c] = df_immobilier[c].fillna('').astype('string')

df_immobilier = df_immobilier.drop_duplicates(subset=['Type de bien', 'Ville', 'surface habitable', 'nombre de pièces', "nom_annonceur"])
df_immobilier = df_immobilier.reset_index(drop=True)

lst_nom_agence_contact_associe = ['Capifrance', 'iad FranceAgent Commercial immatricule a CCI SEINE ET MARNENdeg 50367642100020', 'EFFICITY', 'Optimhome', 'SAFTI', 'BSK IMMOBILIER', 'PROPRIETES PRIVEES', '3G IMMO - CONSULTANT RESEAU NATIONAL', 'MEGAGENCE']

df_immobilier['agence'] = df_immobilier['nom_annonceur']
for nom_agence in lst_nom_agence_contact_associe:
    lst_ind_agence = list(df_immobilier['nom_annonceur'].index[df_immobilier['nom_annonceur'].fillna('0').str.contains(nom_agence)])
    for i in lst_ind_agence:
        df_immobilier.loc[i, 'agence'] = nom_agence
        size_nom_agence = len(nom_agence)
        df_immobilier.loc[i, 'nom_contact'] = df_immobilier.loc[i, 'nom_annonceur'][size_nom_agence:]

for i in range(len(df_immobilier)):
    if type(df_immobilier.loc[i,'agence'])==str:
        df_immobilier.loc[i, 'agence'] = df_immobilier.loc[i, 'agence'].lstrip().rstrip()

lst_nom_agence_non_contact_associe = ['Human Immobilier Begard', 'BBII', "DELF'IMMO", 'SELARL COB JURIS', 'Bel Air Homes Agence Immobiliere', "EXPERTIMO", "AXO L'immobilier Actif", "PROMUP", 'AGENCE NEWTON', 'SEXTANT FRANCE', 'MON BIEN A LA MER', 'ADNOV', 'IMMO RESEAU', 'LMD IMMOBILIER', 'SELECTION HABITAT', 'LEGGETT IMMOBILIER', 'MAN IMMO PRO', 'REGM', '36 HEURES IMMO', 'GRIFF IMMOBILIER', 'IMMO RESEAU']

lst_nom_agence = lst_nom_agence_contact_associe.copy()

lst_nom_agence.extend(lst_nom_agence_non_contact_associe)

df_immobilier['nom_contact'] = df_immobilier['nom_contact'].fillna('NC')

for i in range(len(df_immobilier)):
    if type(df_immobilier.loc[i,'agence'])==str and df_immobilier.loc[i, 'agence'] not in lst_nom_agence:
        df_immobilier.loc[i, 'nom_contact'] = df_immobilier.loc[i, 'nom_annonceur']    

for i in range(len(df_immobilier)):
    if type(df_immobilier.loc[i,'nom_contact'])==str:
        df_immobilier.loc[i,'nom_contact'] = df_immobilier.loc[i,'nom_contact'].replace("*", '').lstrip().rstrip()

for i in range(len(df_immobilier)):
    if type(df_immobilier.loc[i,'agence'])!=str:
        if math.isnan(df_immobilier.loc[i,'agence']) or df_immobilier.loc[i, 'agence']==np.nan:
            df_immobilier.loc[i,'nom_annonceur'] = None

## Remplissage de la collection description dand la base de données immobilier dans MongoDB

# Connexion à MongoDB en local
connection_string="mongodb://localhost:27017/"
client = MongoClient(connection_string)  # Si le serveur tourne sur le port par défaut (27017)
db_name = "projet_bloc_1"  # Nom de ta base
collection_name = "description_logements"  # Nom de la collection

db = client[db_name]
collection = db[collection_name]

print(f"Connexion établie à la base de données '{db_name}'")

# Récupération des documents de MongoDB
documents = list(collection.find())  # Convertir le curseur en liste

# Vérifier qu'on a bien récupéré des documents
if documents:
    # Convertir les _id en str pour Pandas
    for doc in documents:
        doc["_id"] = str(doc["_id"])  

    # Transformer en DataFrame
    df_mongo = pd.DataFrame(documents)

    print("Données récupérées avec succès depuis MongoDB !")
    print(df_mongo.head())  # Afficher un aperçu
else:
    print("Aucun document trouvé dans MongoDB.")


# Création d'une copie explicite du DataFrame + filtrage
colonnes_a_garder = ["Type de bien", "Ville", "title", "short_description", "long_description", "Chauffage", "Exposition", "Parking / Garage", "Jardin / Terrain", "Balcon / Terrasse", "Accès Ascenseur", "Dépendance", "Proximité MétroBusGare SNCFGare RERCommercesEcolesEspaces verts", "Réf. annonce", "Mise à jour"]
df_filtre = df_immobilier[colonnes_a_garder].copy()  # Ajout de .copy() ici

# Remplacement des chaînes vides par 'NC' pour les colonnes textuelles
colonnes_textuelles = ['Chauffage', 'Exposition', 'Dépendance', 'Parking / Garage', 
                       'Jardin / Terrain', 'Balcon / Terrasse', 'Accès Ascenseur',
                       'Proximité MétroBusGare SNCFGare RERCommercesEcolesEspaces verts']

for col in colonnes_textuelles:
    if col in df_filtre.columns:
        # Utiliser .loc pour éviter l'avertissement
        df_filtre.loc[:, col] = df_filtre[col].apply(lambda x: 'NC' if str(x).strip() == '' else x)

# Vérification que toutes les chaînes vides ont été remplacées
for col in colonnes_textuelles:
    if col in df_filtre.columns:
        empty_count = (df_filtre[col] == '').sum()
        print(f"La colonne {col} contient {empty_count} chaînes vides.")

# Merge + insertion dans MongoDB
df_avec_id = df_filtre.merge(df_mongo[["_id", "Réf. annonce"]], on="Réf. annonce", how="left")

# Insertion dans MongoDB
for _, row in df_avec_id.iterrows():
    # Convertir la ligne en dictionnaire
    row_dict = row.to_dict()
    
    if pd.notna(row["_id"]):
        # Supprimer la clé "_id" du dictionnaire avant la mise à jour
        if "_id" in row_dict:
            del row_dict["_id"]
            
        # Mise à jour du document existant
        collection.update_one(
            {"_id": ObjectId(row["_id"])},  # Recherche par _id
            {"$set": row_dict},  # Mise à jour des valeurs sans le champ _id
            upsert=True
        )
    else:
        # Pour une nouvelle insertion, supprimer le champ _id s'il existe et est None/NaN
        if "_id" in row_dict:
            del row_dict["_id"]
            
        # Insérer le nouveau document
        collection.insert_one(row_dict)