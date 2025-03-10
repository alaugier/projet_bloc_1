import requests
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON
import time
from geopy.distance import geodesic
import logging
import sys
import os

# Configuration du logging avec encodage UTF-8
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("mongodb_import.log", encoding='utf-8'),
        logging.StreamHandler(sys.stdout if hasattr(sys.stdout, 'encoding') else None)
    ]
)
logger = logging.getLogger(__name__)

def windows_to_wsl_path(windows_path):
    """
    Convertit un chemin Windows en chemin WSL.
    
    Args:
        windows_path (str): Chemin au format Windows (ex: C:\\Users\\...)
        
    Returns:
        str: Chemin au format WSL (ex: /mnt/c/Users/...)
    """
    # Supprimer les doubles backslashes et extraire la lettre du lecteur
    clean_path = windows_path.replace('\\\\', '\\')
    if ':' in clean_path:
        drive, rest = clean_path.split(':', 1)
        # Convertir au format WSL: /mnt/c/...
        return f"/mnt/{drive.lower()}{rest.replace('\\', '/')}"
    return windows_path

def query_wikidata(query):
    """Exécute une requête SPARQL sur l'endpoint Wikidata et renvoie les résultats."""
    sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    
    # Ajout d'un délai pour respecter les limites de l'API
    time.sleep(1)
    
    try:
        results = sparql.query().convert()
        return results["results"]["bindings"]
    except Exception as e:
        print(f"Erreur lors de la requête: {e}")
        return []

def get_cities_around_rennes(radius_km=50):
    """Récupère les villes dans un rayon donné autour de Rennes."""
    # Coordonnées de Rennes (centre-ville)
    rennes_lat = 48.117266
    rennes_lon = -1.6777926
    
    # Requête pour obtenir les villes françaises (communes)
    query = """
    SELECT ?city ?cityLabel ?population ?area ?coordinates WHERE {
      # Sélection des communes françaises
      ?city wdt:P31 wd:Q484170.
      
      # Dans le département d'Ille-et-Vilaine et départements voisins
      {?city wdt:P131/wdt:P131 wd:Q12543.} UNION # Ille-et-Vilaine
      {?city wdt:P131/wdt:P131 wd:Q12538.} UNION # Morbihan
      {?city wdt:P131/wdt:P131 wd:Q12549.} UNION # Côtes-d'Armor
      {?city wdt:P131/wdt:P131 wd:Q12553.} UNION # Mayenne
      {?city wdt:P131/wdt:P131 wd:Q12741.}       # Maine-et-Loire
      
      # Récupération des coordonnées
      ?city wdt:P625 ?coordinates.
      
      # Population (optionnel)
      OPTIONAL { ?city wdt:P1082 ?population. }
      
      # Surface (optionnel)
      OPTIONAL { ?city wdt:P2046 ?area. }
      
      # Service pour obtenir les labels
      SERVICE wikibase:label { bd:serviceParam wikibase:language "fr". }
    }
    """
    
    results = query_wikidata(query)
    cities_data = []
    
    for result in results:
        # Extraction des coordonnées
        coord_str = result.get('coordinates', {}).get('value', '')
        if coord_str:
            # Format: Point(longitude latitude)
            coord_str = coord_str.replace('Point(', '').replace(')', '')
            parts = coord_str.split()
            if len(parts) >= 2:
                lon, lat = float(parts[0]), float(parts[1])
                
                # Calcul de la distance avec Rennes
                distance = geodesic((rennes_lat, rennes_lon), (lat, lon)).kilometers
                
                # Filtre par distance
                if distance <= radius_km:
                    # Extraction des autres données
                    city_data = {
                        'id': result.get('city', {}).get('value', ''),
                        'name': result.get('cityLabel', {}).get('value', ''),
                        'coordinates': (lat, lon),
                        'distance_to_rennes': round(distance, 2),
                        'population': int(result.get('population', {}).get('value', 0)) if 'population' in result else None,
                        'area_km2': float(result.get('area', {}).get('value', 0)) if 'area' in result else None
                    }
                    
                    # Calcul de la densité si possible
                    if city_data['population'] and city_data['area_km2']:
                        city_data['density'] = round(city_data['population'] / city_data['area_km2'], 2)
                    else:
                        city_data['density'] = None
                    
                    cities_data.append(city_data)
    
    return cities_data

def get_city_public_facilities(city_id):
    """Récupère les établissements publics pour une ville donnée."""
    query = f"""
    SELECT ?facility ?facilityLabel ?facilityType ?facilityTypeLabel WHERE {{
      # Établissements dans cette ville
      ?facility wdt:P131 <{city_id}>.
      
      # Type d'établissement
      ?facility wdt:P31 ?facilityType.
      
      # Filtrer par types d'établissements publics
      VALUES ?facilityType {{
        wd:Q24354 # école
        wd:Q3914 # école primaire
        wd:Q159334 # collège
        wd:Q2385804 # lycée
        wd:Q3918 # université
        wd:Q27686 # bibliothèque
        wd:Q24699794 # mairie
        wd:Q16917 # hôpital
        wd:Q41176 # bâtiment
        wd:Q570116 # office de tourisme
        wd:Q57660343 # centre des impôts
      }}
      
      # Service pour obtenir les labels
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "fr". }}
    }}
    """
    
    results = query_wikidata(query)
    facilities = []
    
    for result in results:
        facility = {
            'id': result.get('facility', {}).get('value', ''),
            'name': result.get('facilityLabel', {}).get('value', ''),
            'type_id': result.get('facilityType', {}).get('value', ''),
            'type_name': result.get('facilityTypeLabel', {}).get('value', '')
        }
        facilities.append(facility)
    
    return facilities

def determine_zone_type(city_data, facilities):
    """Détermine le type de zone (urbaine, périurbaine, rurale) en fonction de critères simples."""
    if city_data['population'] is None:
        return "Indéterminé"
    
    if city_data['population'] > 50000 or (city_data['density'] and city_data['density'] > 1000):
        return "Urbaine"
    elif city_data['population'] > 5000 or (city_data['density'] and city_data['density'] > 300) or len(facilities) > 10:
        return "Périurbaine"
    else:
        return "Rurale"

def main():
    # Récupération des villes autour de Rennes
    print("Récupération des villes autour de Rennes...")
    cities = get_cities_around_rennes(50)
    
    # Enrichissement des données avec les établissements publics et le type de zone
    for city in cities:
        print(f"Traitement de {city['name']}...")
        facilities = get_city_public_facilities(city['id'])
        
        # Compter les types d'établissements
        facility_counts = {}
        for facility in facilities:
            facility_type = facility['type_name']
            facility_counts[facility_type] = facility_counts.get(facility_type, 0) + 1
        
        city['public_facilities'] = facility_counts
        city['total_public_facilities'] = len(facilities)
        city['zone_type'] = determine_zone_type(city, facilities)
    
    # Création d'un DataFrame pandas
    df = pd.DataFrame(cities)
    
    # Sauvegarde en CSV
    csv_filepath='C:\\Users\\Utilisateur\\Projet_Bloc_1\\cities_around_rennes.csv'
    client = None
    try:
        # Conversion du chemin Windows en chemin WSL si nécessaire
        wsl_filepath = windows_to_wsl_path(csv_filepath)
        
        # Vérifier si le chemin Windows existe d'abord
        win_exists = os.path.exists(csv_filepath)
        wsl_exists = os.path.exists(wsl_filepath)
        
        # Utiliser le chemin qui existe
        if win_exists:
            actual_path = csv_filepath
            logger.info(f"Utilisation du chemin Windows: {csv_filepath}")
            df.to_csv(actual_path)
            print(f"Données sauvegardées pour {len(cities)} villes.")
        elif wsl_exists:
            actual_path = wsl_filepath
            logger.info(f"Utilisation du chemin WSL: {wsl_filepath}")
            df.to_csv(actual_path)
            print(f"Données sauvegardées pour {len(cities)} villes.")
        else:
            logger.error(f"Le fichier n'existe ni sous Windows ({csv_filepath}) ni sous WSL ({wsl_filepath})")
            print("0")
    except Exception as e:
        logger.error(f"Erreur lors de l'importation des données: {str(e)}", exc_info=True)
        print("0")
    finally:
        # Fermeture de la connexion
        if client:
            client.close()
    return df

if __name__ == "__main__":
    df = main()
    print(df.head())
