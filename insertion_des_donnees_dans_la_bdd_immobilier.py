## Chargement des librairies et des variables d'environnement

from dotenv import load_dotenv
import os
import math
import pandas as pd
import numpy as np
import mysql.connector
from datetime import datetime
import csv

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
    
load_dotenv('var.env')
host = os.getenv('host')
database = os.getenv('database')
user = os.getenv('user')
mdp = os.getenv("mdp")

immo_cnx = [host, database, user, mdp]

## Aggrégation des données des logements de Rennes dans un rayon de 30km autour du centre-ville

# Dataset des logementsdf
df_immobilier = pd.read_csv("paru_vendu_ventes_logements_rennes_2025-3-2-23-6-21-852774.csv", index_col=0)

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

## Remplissage de la base de données

### Remplissage de la table ville

#### Villes des agences non répertoriées dans l'ensemble des villes des logements
set_ville_logements = set(df_immobilier['Ville'].unique())
set_ville_agences = set(df_immobilier["ville de l'annonceur"].unique())
set_ville_agences_dif_logements = set_ville_agences.difference(set_ville_logements)
lst_ville_agences_dif_logements = list(set_ville_agences_dif_logements)
if 'crioult' in lst_ville_agences_dif_logements:
    ind_zip_code = lst_ville_agences_dif_logements.index('crioult')
    lst_ville_agences_dif_logements[ind_zip_code] = lst_ville_agences_dif_logements[ind_zip_code].replace('crioult', 'st germain du crioult')
if 'gregoire' in lst_ville_agences_dif_logements:
    ind_zip_code = lst_ville_agences_dif_logements.index('gregoire')
    lst_ville_agences_dif_logements[ind_zip_code] = lst_ville_agences_dif_logements[ind_zip_code].replace('gregoire', 'st gregoire')
if 'pompadour' in lst_ville_agences_dif_logements:
    ind_zip_code = lst_ville_agences_dif_logements.index('pompadour')
    lst_ville_agences_dif_logements[ind_zip_code] = lst_ville_agences_dif_logements[ind_zip_code].replace('pompadour', 'arnac pompadour')
print(lst_ville_agences_dif_logements, len(lst_ville_agences_dif_logements))

# Récupération des codes postaux des villes
# Obtenir les données du CSV des codes INSEE pour les villes
def charger_codes_insee_depuis_url(url):
    """
    Charge les données du CSV des codes INSEE depuis une URL spécifique.
    """
    try:
        # Télécharger le fichier CSV à l'URL donnée
        response = requests.get(url, timeout=10)  # Timeout de 10 secondes
        if response.status_code == 200:
            data = response.text
            
            # Utiliser io.StringIO pour lire le CSV à partir du texte
            df_insee = pd.read_csv(io.StringIO(data), sep=';', encoding='latin-1')
            return df_insee
        else:
            print(f"Erreur lors du téléchargement: code {response.status_code}")
            return None
    except Exception as e:
        print(f"Erreur lors du chargement depuis {url}: {e}")
        return None

# URL par défaut pour les codes INSEE des villes (à mettre à jour si besoin)
url_par_defaut = "https://www.data.gouv.fr/fr/datasets/r/9713f203-7703-4f86-8b95-64446d7132ee"

# Charger les données directement avec l'URL par défaut
print(f"Tentative de chargement des codes INSEE depuis: {url_par_defaut}")
df_zip_codes = charger_codes_insee_depuis_url(url_par_defaut)

# Vérifier si le chargement a réussi
if df_zip_codes is not None:
    print(f"Chargement des codes INSEE réussi: {len(df_zip_codes)} entrées trouvées")
    # Continuer avec le reste de votre code...
else:
    print("Impossible de charger les codes INSEE. Arrêt du script.")
    import sys
    sys.exit(1)

code_postal_ville_agences_dif_logements = []
for i in range(len(lst_ville_agences_dif_logements)):
    try:
        idn_i = df_zip_codes.index[df_zip_codes['Libellé_d_acheminement'] == lst_ville_agences_dif_logements[i].upper()].tolist()[0]
        code_postal_ville_agences_dif_logements.append(df_zip_codes.loc[idn_i, 'Code_postal'])
    except:
        if lst_ville_agences_dif_logements[i]=='st germain du crioult':
            code_postal_ville_agences_dif_logements.append(np.int64(14110))
        elif lst_ville_agences_dif_logements[i]=='lez':
            code_postal_ville_agences_dif_logements.append(np.int64(31440))
        elif lst_ville_agences_dif_logements[i]=='guipry':
            code_postal_ville_agences_dif_logements.append(np.int64(35480))
        elif lst_ville_agences_dif_logements[i]=='lamballe':
            code_postal_ville_agences_dif_logements.append(np.int64(22400))
        elif lst_ville_agences_dif_logements[i]=='lamballe':
            code_postal_ville_agences_dif_logements.append(np.int64(22400))
        elif lst_ville_agences_dif_logements[i]=='perret':
            code_postal_ville_agences_dif_logements.append(np.int64(22570))
        else:
            code_postal_ville_agences_dif_logements.append('')

# insert data from dataframe
df_ville = df_immobilier.loc[:,['Ville', 'Code postal']]
df_ville['id_locale'] = df_immobilier.index
df_ville = df_ville.loc[:,['id_locale', 'Ville', 'Code postal']].rename(columns={'Ville': 'nom', 'Code postal':'code_postal'})

for i in range(len(lst_ville_agences_dif_logements)):
    df_ville.loc[len(df_ville)] = [len(df_ville), lst_ville_agences_dif_logements[i], code_postal_ville_agences_dif_logements[i]]

df_ville = df_ville.drop_duplicates(subset=['nom', 'code_postal'])
df_ville = df_ville.reset_index(drop=True)

df_ville['id_locale'] = df_ville.index

#### Insertion des données des villes dans la table des villes de la base de données immobilier
cnx, cursor = sql_cnx(immo_cnx)

try:
    # requète SQL
    q = "INSERT IGNORE INTO ville (id_locale, nom, code_postal) VALUES (%s, %s, %s)"
    
    # Insert DataFrame records one by one. 
    for i,row in df_ville.iterrows():
        print(f"Insertion de {tuple(row)}...")
        cursor.execute(q, tuple(row))
        print("Insertion réussie.")
    
    # valider la transaction
    cnx.commit()
    print("Enregistrement des modifications.\n")

except Exception as e:
    print("Une erreur est survenue :\n",e,"\nUn rollback de la base de donnée va être effectué.")
    q="ROLLBACK;"
    cursor.execute(q)
    print("ROLLBACK effectué !")

# fermer la conection
cursor.close()
cnx.close()
print("Déconnexion de la base de donnée.")

### Remplissage de la table appartement

#### Création du dataframe des appartements
df_appartement = df_immobilier[df_immobilier['Type de bien']=='appartement']

df_appartement = df_appartement.reset_index(drop=True)

dict_ref_appart_idn_ville = {}
for ref in df_appartement['Réf. annonce']:
    idn_ref = df_appartement[df_appartement['Réf. annonce']==ref].index.tolist()[0]
    nom_ville = df_appartement.loc[idn_ref, 'Ville']
    idn_ville = df_ville[df_ville['nom']==nom_ville].index.tolist()[0]
    dict_ref_appart_idn_ville.update({ref : idn_ville})

df_appartement = df_appartement.loc[:,["Ville", "Réf. annonce", 'prix', 'prix au m²', 'surface habitable', 'nombre de pièces', 'nombre de chambres', 'Etage', 'lien du logement', 'étiquette DPE', 'consommation énergétique', 'étiquette GPE', 'émission GPE', "Mise à jour"]]
df_appartement['nombre de chambres'] = df_appartement['nombre de chambres'].fillna(0)
df_appartement['nombre de chambres'] = df_appartement['nombre de chambres'].astype('int')

df_appartement['id_locale'] = df_appartement['Réf. annonce']
df_appartement['id_locale'] = df_appartement['id_locale'].map(dict_ref_appart_idn_ville)

df_appartement['id_appartement'] = df_appartement.index
df_appartement = df_appartement.loc[:,['id_appartement', "Réf. annonce", 'prix', 'prix au m²', 'surface habitable', 'nombre de pièces', 'nombre de chambres', 'Etage', 'étiquette DPE', 'consommation énergétique', 'étiquette GPE', 'émission GPE', 'lien du logement', "Mise à jour", 'id_locale']].rename(columns={"Réf. annonce": 'ref_annonce', 'surface habitable':'surf_hab_m2', 'nombre de pièces':'nb_piece', 'nombre de chambres':'nb_chambre', 'Etage':'num_etage', 'lien du logement':'lien_appartement', 'étiquette DPE':'lab_dpe', 'consommation énergétique':'conso_elec', 'étiquette GPE':'lab_gpe', 'émission GPE':'emis_gpe', "Mise à jour":'date_maj'})

#### Insertion des données des appartements dans la table des appartements de la base de données immobilier
cnx, cursor = sql_cnx(immo_cnx)

try:
    # requète SQL
    q = "INSERT IGNORE INTO appartement (id_appartement, ref_annonce, prix, prix_au_m2, surf_hab_m2, nb_piece, nb_chambre, num_etage, lab_dpe, conso_elec, lab_gpe, emis_gpe, lien_appartement, date_maj, id_locale) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    # Insert DataFrame records one by one. 
    for i,row in df_appartement.iterrows():
        print(f"Insertion de {tuple(row)}...")
        cursor.execute(q, tuple(row))
        print("Insertion réussie.")
    
    # valider la transaction
    cnx.commit()
    print("Enregistrement des modifications.\n")

except Exception as e:
    print("Une erreur est survenue :\n",e,"\nUn rollback de la base de donnée va être effectué.")
    q="ROLLBACK;"
    cursor.execute(q)
    print("ROLLBACK effectué !")

# fermer la connection
cursor.close()
cnx.close()
print("Déconnexion de la base de donnée.")

## Remplissage de la table maison

#### Création du dataframe des maisons

df_maison = df_immobilier[df_immobilier['Type de bien']=='maison']

df_maison = df_maison.reset_index(drop=True)

dict_ref_maison_idn_ville = {}
for ref in df_maison['Réf. annonce']:
    idn_ref = df_maison[df_maison['Réf. annonce']==ref].index.tolist()[0]
    nom_ville = df_maison.loc[idn_ref, 'Ville']
    idn_ville = df_ville[df_ville['nom']==nom_ville].index.tolist()[0]
    dict_ref_maison_idn_ville.update({ref : idn_ville})
dict_ref_maison_idn_ville

df_maison = df_maison.loc[:,["Ville", "Réf. annonce", 'prix', 'prix au m²', 'surface habitable', 'nombre de pièces', 'nombre de chambres', 'Etage', 'lien du logement', 'étiquette DPE', 'consommation énergétique', 'étiquette GPE', 'émission GPE', "Mise à jour"]]
df_maison['nombre de pièces'] = df_maison['nombre de pièces'].fillna(0)
df_maison['nombre de pièces'] = df_maison['nombre de pièces'].astype('int')
df_maison['nombre de chambres'] = df_maison['nombre de chambres'].fillna(0)
df_maison['nombre de chambres'] = df_maison['nombre de chambres'].astype('int')

df_maison['id_locale'] = df_maison['Réf. annonce']
df_maison['id_locale'] = df_maison['id_locale'].map(dict_ref_maison_idn_ville)

df_maison['id_maison'] = df_maison.index
df_maison = df_maison.loc[:,['id_maison', "Réf. annonce", 'prix', 'prix au m²', 'surface habitable', 'nombre de pièces', 'nombre de chambres', 'Etage', 'étiquette DPE', 'consommation énergétique', 'étiquette GPE', 'émission GPE', 'lien du logement', "Mise à jour", 'id_locale']].rename(columns={"Réf. annonce": 'ref_annonce', 'surface habitable':'surf_hab_m2', 'nombre de pièces':'nb_piece', 'nombre de chambres':'nb_chambre', 'Etage':'num_etage', 'lien du logement':'lien_appartement', 'étiquette DPE':'lab_dpe', 'consommation énergétique':'conso_elec', 'étiquette GPE':'lab_gpe', 'émission GPE':'emis_gpe', "Mise à jour":'date_maj'})

#### Insertion des données des maisons dans la table des maisons de la base de données immobilier
cnx, cursor = sql_cnx(immo_cnx)

try:
    # requète SQL
    q = "INSERT IGNORE INTO maison (id_maison, ref_annonce, prix, prix_au_m2, surf_hab_m2, surf_terrain_m2, nb_piece, nb_chambre, lab_dpe, conso_elec, emis_gpe, lab_gpe, lien_maison, date_maj, id_locale) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    # Insert DataFrame records one by one. 
    for i,row in df_maison.iterrows():
        print(f"Insertion de {tuple(row)}...")
        cursor.execute(q, tuple(row))
        print("Insertion réussie.")
    
    # valider la transaction
    cnx.commit()
    print("Enregistrement des modifications.\n")

except Exception as e:
    print("Une erreur est survenue :\n",e,"\nUn rollback de la base de donnée va être effectué.")
    q="ROLLBACK;"
    cursor.execute(q)
    print("ROLLBACK effectué !")

# fermer la conection
cursor.close()
cnx.close()
print("Déconnexion de la base de donnée.")

## Remplissage de la table Agence

#### Création du dataframe des agences
df_agence = df_immobilier.loc[:,["agence", "nom_contact", "ville de l'annonceur", "lien de l'annonceur"]]

df_agence = df_agence.drop_duplicates(subset=['agence', 'nom_contact'])
df_agence = df_agence.reset_index(drop=True)

df_agence["ville de l'annonceur"] = df_agence["ville de l'annonceur"].str.replace('crioult', 'st germain du crioult').str.replace('gregoire', 'st gregoire').str.replace('pompadour', 'arnac pompadour')

dict_agence_idn_ville = {}
for agence,contact in list(zip(df_agence.agence, df_agence.nom_contact)):
    idn_ref = df_agence[(df_agence['agence']==agence) & (df_agence['nom_contact']==contact)].index.tolist()[0]
    nom_ville = df_agence.loc[idn_ref, "ville de l'annonceur"]
    idn_ville = df_ville[df_ville['nom']==nom_ville].index.tolist()[0]
    dict_agence_idn_ville.update({(agence, contact) : idn_ville})

df_agence['id_locale'] = list(zip(df_agence.agence, df_agence.nom_contact))
df_agence['id_locale'] = df_agence['id_locale'].map(dict_agence_idn_ville)

df_agence['id_agence'] = df_agence.index
df_agence = df_agence.loc[:,['id_agence', "agence", "nom_contact", "ville de l'annonceur", "lien de l'annonceur", 'id_locale']].rename(columns={"agence":"nom_agence", "ville de l'annonceur":'ville_annonceur', "lien de l'annonceur":'lien_annonceur'})

#### Insertion des données des agences dans la table des agences de la base de données immobilier
cnx, cursor = sql_cnx(immo_cnx)

try:
    # requète SQL
    q = "INSERT IGNORE INTO agence (id_agence, nom_agence, nom_contact, ville_annonceur, lien_annonceur, id_locale) VALUES (%s, %s, %s, %s, %s, %s)"

    # Insert DataFrame records one by one. 
    for i,row in df_agence.iterrows():
        print(f"Insertion de {tuple(row)}...")
        cursor.execute(q, tuple(row))
        print("Insertion réussie.")
    
    # valider la transaction
    cnx.commit()
    print("Enregistrement des modifications.\n")

except Exception as e:
    print("Une erreur est survenue :\n",e,"\nUn rollback de la base de donnée va être effectué.")
    q="ROLLBACK;"
    cursor.execute(q)
    print("ROLLBACK effectué !")

# fermer la conection
cursor.close()
cnx.close()
print("Déconnexion de la base de donnée.")

## Remplissage de la table appartement_agence

#### Création du dataframe appartement_agence
df_appartement_agence = pd.DataFrame()
df_appartement_agence['id_appartement'] = df_appartement['id_appartement']

lst_agence_appartement = []
for i in range(len(df_immobilier)):
    if df_immobilier.loc[i, 'Type de bien']=='appartement':
        lst_agence_appartement.append(df_immobilier.loc[i, "agence"])

dict_id_agence_appartement = {}
for agence in df_agence['nom_agence']:
    if agence in lst_agence_appartement:
        dict_id_agence_appartement.update({agence : list(df_agence[df_agence['nom_agence']==agence].loc[:,'id_agence'])[0]})

df_appartement_agence['id_agence'] = lst_agence_appartement
df_appartement_agence['id_agence'] = df_appartement_agence['id_agence'].map(dict_id_agence_appartement)

#### Insertion des données des appartement_agence dans la table de jonction appartement_agence de la base de données immobilier
cnx, cursor = sql_cnx(immo_cnx)

try:
    # requète SQL
    q = "INSERT IGNORE INTO appartement_agence (id_appartement, id_agence) VALUES (%s, %s)"

    # Insert DataFrame records one by one. 
    for i,row in df_appartement_agence.iterrows():
        print(f"Insertion de {tuple(row)}...")
        cursor.execute(q, tuple(row))
        print("Insertion réussie.")
    
    # valider la transaction
    cnx.commit()
    print("Enregistrement des modifications.\n")

except Exception as e:
    print("Une erreur est survenue :\n",e,"\nUn rollback de la base de donnée va être effectué.")
    q="ROLLBACK;"
    cursor.execute(q)
    print("ROLLBACK effectué !")

# fermer la connection
cursor.close()
cnx.close()
print("Déconnexion de la base de donnée.")

## Remplissage de la table maison_agence

#### Création du dataframe maison_agence
df_maison_agence = pd.DataFrame()
df_maison_agence['id_maison'] = df_maison['id_maison']

lst_agence_maison = []
for i in range(len(df_immobilier)):
    if df_immobilier.loc[i, 'Type de bien']=='maison':
        lst_agence_maison.append(df_immobilier.loc[i, "agence"])

dict_id_agence_maison = {}
for agence in df_agence['nom_agence']:
    if agence in lst_agence_maison:
        dict_id_agence_maison.update({agence : list(df_agence[df_agence['nom_agence']==agence].loc[:,'id_agence'])[0]})

df_maison_agence['id_agence'] = lst_agence_maison
df_maison_agence['id_agence'] = df_maison_agence['id_agence'].map(dict_id_agence_maison)

# Insertion des données des maison_agence dans la table de jonction des maison_agence de la base de données immobilier
cnx, cursor = sql_cnx(immo_cnx)

try:
    # requète SQL
    q = "INSERT IGNORE INTO maison_agence (id_maison, id_agence) VALUES (%s, %s)"

    # Insert DataFrame records one by one. 
    for i,row in df_maison_agence.iterrows():
        print(f"Insertion de {tuple(row)}...")
        cursor.execute(q, tuple(row))
        print("Insertion réussie.")
    
    # valider la transaction
    cnx.commit()
    print("Enregistrement des modifications.\n")

except Exception as e:
    print("Une erreur est survenue :\n",e,"\nUn rollback de la base de donnée va être effectué.")
    q="ROLLBACK;"
    cursor.execute(q)
    print("ROLLBACK effectué !")

# fermer la connection
cursor.close()
cnx.close()
print("Déconnexion de la base de donnée.")


