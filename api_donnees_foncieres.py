import requests
import pandas as pd
import time
import os
from datetime import datetime
import io
from geopy.distance import geodesic

session = requests.Session()  # Création d'une session

def apidf(url_endpoint, token=None, max_retries=3, timeout=10):
    HEADERS = {"Content-Type": "application/json"}
    if token:
        HEADERS["Authorization"] = "Token " + token

    retries = 0
    while retries < max_retries:
        try:
            print(f"Tentative {retries + 1}: Requête à {url_endpoint}")
            response = session.get(url_endpoint, headers=HEADERS, timeout=timeout)
            
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Erreur {response.status_code}: {response.text}")
                return None
        
        except requests.exceptions.Timeout:
            print("Timeout! Nouvelle tentative...")
            retries += 1
        except requests.exceptions.RequestException as e:
            print(f"Erreur lors de la requête: {e}")
            break

    print("Échec après plusieurs tentatives.")
    return None

# Charger les données du fichier CSV des villes autour de Rennes
def charger_villes_rennes(fichier_csv):
    df_villes = pd.read_csv(fichier_csv)
    return df_villes

# Charger les données du CSV des codes INSEE pour les villes
def charger_codes_insee(url):
    # Télécharger le fichier CSV à l'URL donnée
    response = requests.get(url)
    data = response.text
    
    # Utiliser io.StringIO pour lire le CSV à partir du texte
    df_insee = pd.read_csv(io.StringIO(data), sep=';', encoding='latin-1')
    return df_insee

# Extraire les coordonnées de la colonne 'coordinates'
def extraire_coordonnees(row):
    # Extraire les coordonnées comme tuple (latitude, longitude)
    return tuple(map(float, row['coordinates'][1:-1].split(',')))  # Conversion en tuple

# Fonction pour calculer la distance entre Rennes et une autre ville
def calculer_distance(rennes_coords, ville_coords):
    """
    Calcul la distance entre Rennes (rennes_coords) et une ville (ville_coords)
    en utilisant la méthode de Haversine.
    """
    return geodesic(rennes_coords, ville_coords).km

# Fonction pour associer les codes INSEE aux villes proches de Rennes
def associer_codes_insee(df_villes, df_insee, rennes_coords, rayon=100):
    """
    Associe les codes INSEE aux villes autour de Rennes en comparant les noms
    et les coordonnées, puis filtre celles qui sont dans le rayon spécifié.
    """
    villes_avec_insee = []
    
    for _, row in df_villes.iterrows():
        # Extraire les coordonnées du champ 'coordinates' (tuple)
        ville_coords = extraire_coordonnees(row)
        distance = calculer_distance(rennes_coords, ville_coords)
        
        if distance <= rayon:
            # Trouver le code INSEE correspondant à la ville
            ville_nom = row['name'].strip().lower()
            ville_insee = df_insee[df_insee['Nom_de_la_commune'].str.lower() == ville_nom]
            
            if not ville_insee.empty:
                code_insee = ville_insee.iloc[0]['#Code_commune_INSEE']
                villes_avec_insee.append(code_insee)
    
    return villes_avec_insee

# Fonction pour extraire les données foncières
def extract_donnees_foncieres_communes(codes_insee, annee_debut=2018, annee_fin=2023, token=None):
    base_url = "https://apidf-preprod.cerema.fr/indicateurs/dv3f/communes/annuel/"
    
    all_data = []
    
    for code_insee in codes_insee:
        print(f"Extraction des données pour la commune {code_insee}...")
        
        url = f"{base_url}{code_insee}"
        
        try:
            # Spécifier un timeout plus long (ex : 10 secondes)
            response = requests.get(url, timeout=10)  # Timeout en secondes
            data = response.json()
        except requests.exceptions.Timeout:
            print(f"Erreur de délai d'attente pour la commune {code_insee}. Tentative suivante.")
            continue  # Si timeout, passe à la suivante
        except requests.exceptions.RequestException as e:
            print(f"Erreur lors de la requête pour {code_insee}: {e}")
            continue  # Si une autre erreur, passe à la suivante
        
        if data:
            if isinstance(data, list):
                items_to_process = data
            else:
                items_to_process = [data]
            
            for item in items_to_process:
                # Vérification des différents champs d'année
                annee = item.get('annee') or item.get('anneemut') or item.get('annee_mutation')
                
                if annee is not None:
                    try:
                        annee = int(annee)  # Conversion en entier
                    except ValueError:
                        print(f"Erreur de conversion pour l'année: {annee}")
                        continue
                    
                    if annee_debut <= annee <= annee_fin:
                        if 'code_insee' not in item and 'codinsee' not in item:
                            item['code_insee'] = code_insee
                        all_data.append(item)
                        print(f"  Données récupérées pour {code_insee} - {annee}")
                else:
                    if 'code_insee' not in item and 'codinsee' not in item:
                        item['code_insee'] = code_insee
                    all_data.append(item)
                    print(f"  Données récupérées pour {code_insee} (année non spécifiée)")
        
        else:
            print(f"  Aucune donnée disponible pour {code_insee}")
        
        time.sleep(1)
    
    if all_data:
        df = pd.json_normalize(all_data)
        
        for year_col in ['annee', 'anneemut', 'annee_mutation']:
            if year_col in df.columns:
                df[year_col] = pd.to_numeric(df[year_col], errors='coerce')
                df = df[(df[year_col] >= annee_debut) & (df[year_col] <= annee_fin)]
                break
        
        print(f"Extraction terminée pour {len(df)} enregistrements.")
        return df
    else:
        print("Aucune donnée récupérée.")
        return pd.DataFrame()

# Charger les données des villes autour de Rennes
fichier_villes_rennes = 'cities_around_rennes.csv'  # Remplace par ton fichier
df_villes = charger_villes_rennes(fichier_villes_rennes)

# Charger les codes INSEE des villes
url_codes_insee = 'https://www.data.gouv.fr/fr/datasets/r/170ec28c-cd4a-4ce4-bac5-f1d8243cd7bb'
df_insee = charger_codes_insee(url_codes_insee)

# Coordonner de Rennes (latitude, longitude)
rennes_coords = (48.1173, -1.6778)  # Exemple pour Rennes

# Extraire les codes INSEE des villes dans un rayon de 100 km autour de Rennes
codes_insee_proches = associer_codes_insee(df_villes, df_insee, rennes_coords, rayon=100)

# Exemple d'extraction des données foncières pour ces communes
df_rennes = extract_donnees_foncieres_communes(codes_insee_proches, annee_debut=2018, annee_fin=2023, token="ton_token")

def process_rennes_data(df_rennes, colonnes_mapping):
    # Liste pour collecter les données sous forme de lignes
    all_data = []
    
    # Parcourir chaque ligne du DataFrame principal
    for index, row in df_rennes.iterrows():
        code_insee = row['code_insee']
        
        # Vérifier si la colonne 'results' existe et contient des données
        if 'results' not in row or not row['results']:
            print(f"Pas de données 'results' pour le code INSEE {code_insee}")
            continue
        
        # La cellule 'results' contient une liste avec un seul dictionnaire
        for year_data in row['results']:  # Il y a une seule entrée dans la liste
            # Ajouter le code_insee aux données
            year_data['code_insee'] = code_insee
            
            # Appliquer le mappage des colonnes en ne conservant que celles qui existent
            year_data_mapped = {colonnes_mapping.get(k, k): v for k, v in year_data.items()}
            
            # Ajouter cette ligne à la liste
            all_data.append(year_data_mapped)
    
    # Vérifier si des données ont été collectées
    if not all_data:
        print("Aucune donnée collectée dans all_data.")
    
    # Créer un DataFrame avec les données collectées
    df_cleaned = pd.DataFrame(all_data)
    
    # Supprimer la colonne 'année' si elle existe et contient des valeurs inutiles
    if 'année' in df_cleaned.columns:
        df_cleaned = df_cleaned.drop(columns=['année'])

    df_cleaned = df_cleaned.rename(columns=colonne_mapping)

    # Garder uniquement les colonnes présentes dans le mappage et dans le DataFrame
    colonnes_presentes = [col for col in colonne_mapping.values() if col in df_cleaned.columns]

    df_cleaned = df_cleaned[colonnes_presentes]
    
    return df_cleaned



# Exemple de colonnes_mapping
colonne_mapping = {
    "annee": "annee_mutation",  # Année de la mutation
    "codgeo": "code_geo",  # Code géographique
    "libgeo": "libelle_geo",  # Libellé géographique
    "nbtrans_cod1": "nombre_transactions_cod1",  # Nombre de transactions, cod1
    "valeurfonc_sum_cod1": "valeur_fonciere_sum_cod1",  # Somme des valeurs foncières, cod1
    "nbtrans_cod2": "nombre_transactions_cod2",  # Nombre de transactions, cod2
    "valeurfonc_sum_cod2": "valeur_fonciere_sum_cod2",  # Somme des valeurs foncières, cod2
    "nbtrans_cod11": "nombre_transactions_cod11",  # Nombre de transactions, cod11
    "valeurfonc_sum_cod11": "valeur_fonciere_sum_cod11",  # Somme des valeurs foncières, cod11
    "nbtrans_cod111": "nombre_transactions_cod111",  # Nombre de transactions, cod111
    "valeurfonc_sum_cod111": "valeur_fonciere_sum_cod111",  # Somme des valeurs foncières, cod111
    "valeurfonc_q25_cod111": "valeur_fonciere_q25_cod111",  # 25e percentile de la valeur foncière, cod111
    "valeurfonc_median_cod111": "valeur_fonciere_median_cod111",  # Médiane de la valeur foncière, cod111
    "valeurfonc_q75_cod111": "valeur_fonciere_q75_cod111",  # 75e percentile de la valeur foncière, cod111
    "pxm2_q25_cod111": "prix_m2_q25_cod111",  # 25e percentile du prix au m², cod111
    "pxm2_median_cod111": "prix_m2_median_cod111",  # Médiane du prix au m², cod111
    "pxm2_q75_cod111": "prix_m2_q75_cod111",  # 75e percentile du prix au m², cod111
    "sbati_sum_cod111": "surface_batie_sum_cod111",  # Somme des surfaces bâties, cod111
    "sbati_median_cod111": "surface_batie_median_cod111",  # Médiane de la surface bâtie, cod111
    "nbtrans_cod1111": "nombre_transactions_cod1111",  # Nombre de transactions, cod1111
    "valeurfonc_sum_cod1111": "valeur_fonciere_sum_cod1111",  # Somme des valeurs foncières, cod1111
    "valeurfonc_q25_cod1111": "valeur_fonciere_q25_cod1111",  # 25e percentile de la valeur foncière, cod1111
    "valeurfonc_median_cod1111": "valeur_fonciere_median_cod1111",  # Médiane de la valeur foncière, cod1111
    "valeurfonc_q75_cod1111": "valeur_fonciere_q75_cod1111",  # 75e percentile de la valeur foncière, cod1111
    "pxm2_q25_cod1111": "prix_m2_q25_cod1111",  # 25e percentile du prix au m², cod1111
    "pxm2_median_cod1111": "prix_m2_median_cod1111",  # Médiane du prix au m², cod1111
    "pxm2_q75_cod1111": "prix_m2_q75_cod1111",  # 75e percentile du prix au m², cod1111
    "sbati_sum_cod1111": "surface_batie_sum_cod1111",  # Somme des surfaces bâties, cod1111
    "sbati_median_cod1111": "surface_batie_median_cod1111",  # Médiane de la surface bâtie, cod1111
    "nbtrans_cod1112": "nombre_transactions_cod1112",  # Nombre de transactions, cod1112
    "valeurfonc_sum_cod1112": "valeur_fonciere_sum_cod1112",  # Somme des valeurs foncières, cod1112
    "valeurfonc_q25_cod1112": "valeur_fonciere_q25_cod1112",  # 25e percentile de la valeur foncière, cod1112
    "valeurfonc_median_cod1112": "valeur_fonciere_median_cod1112",  # Médiane de la valeur foncière, cod1112
    "valeurfonc_q75_cod1112": "valeur_fonciere_q75_cod1112",  # 75e percentile de la valeur foncière, cod1112
    "pxm2_q25_cod1112": "prix_m2_q25_cod1112",  # 25e percentile du prix au m², cod1112
    "pxm2_median_cod1112": "prix_m2_median_cod1112",  # Médiane du prix au m², cod1112
    "pxm2_q75_cod1112": "prix_m2_q75_cod1112",  # 75e percentile du prix au m², cod1112
    "sbati_sum_cod1112": "surface_batie_sum_cod1112",  # Somme des surfaces bâties, cod1112
    "sbati_median_cod1112": "surface_batie_median_cod1112",  # Médiane de la surface bâtie, cod1112
    "nbtrans_cod1113": "nombre_transactions_cod1113",  # Nombre de transactions, cod1113
    "valeurfonc_sum_cod1113": "valeur_fonciere_sum_cod1113",  # Somme des valeurs foncières, cod1113
    "valeurfonc_q25_cod1113": "valeur_fonciere_q25_cod1113",  # 25e percentile de la valeur foncière, cod1113
    "valeurfonc_median_cod1113": "valeur_fonciere_median_cod1113",  # Médiane de la valeur foncière, cod1113
    "valeurfonc_q75_cod1113": "valeur_fonciere_q75_cod1113",  # 75e percentile de la valeur foncière, cod1113
    "pxm2_q25_cod1113": "prix_m2_q25_cod1113",  # 25e percentile du prix au m², cod1113
    "pxm2_median_cod1113": "prix_m2_median_cod1113",  # Médiane du prix au m², cod1113
    "pxm2_q75_cod1113": "prix_m2_q75_cod1113",  # 75e percentile du prix au m², cod1113
    "sbati_sum_cod1113": "surface_batie_sum_cod1113",  # Somme des surfaces bâties, cod1113
    "sbati_median_cod1113": "surface_batie_median_cod1113",  # Médiane de la surface bâtie, cod1113
    "nbtrans_mp1": "nombre_transactions_mp1"  # Nombre de transactions, mp1
}


# Nettoyage, filtrage et grégation des données foncières relatives à la ville de Rennes dans un dataset
df_rennes_cleaned = process_rennes_data(df_rennes, colonne_mapping)

# Sauvegarde des données obtenues dans un csv
df_rennes_cleaned.to_csv('/mnt/c/Users/Utilisateur/Projet_Bloc_1/api_indicateurs_donnees_foncieres.csv')

def extract_geomutations_data(url_endpoint, token=None):
    """
    Fonction pour récupérer les données de géométrie depuis l'API
    """
    data = apidf(url_endpoint, token)
    
    if not data:
        print("Aucune donnée récupérée.")
        return pd.DataFrame()
    
    # Vérifier si 'features' est présent dans les données
    if 'features' in data:
        features_data = data['features']
    else:
        print("Aucune colonne 'features' trouvée dans les données.")
        return pd.DataFrame()
    
    # Transformer la liste de dictionnaires en DataFrame
    df_features = pd.json_normalize(features_data)
    
    return df_features

# Extraction de ces données de géométrie
url = "https://apidf-preprod.cerema.fr/dvf_opendata/geomutations/?code_insee=35238&in_bbox=-1.69%2C%2048.07%2C-1.67%2C48.09"
df_geomutations = extract_geomutations_data(url)

colonnes_a_supprimer = ['type', 'geometry.type', 'properties.idmutinvar', 'properties.idopendata']

# Vérifier que les colonnes existent avant de les supprimer
colonnes_presentes = [col for col in colonnes_a_supprimer if col in df_geomutations.columns]

df_geomutations = df_geomutations.drop(columns=colonnes_presentes)

# Sauvegarde des données obtenues dans un csv
df_geomutations.to_csv('/mnt/c/Users/Utilisateur/Projet_Bloc_1/api_geometries_donnees_foncieres.csv')