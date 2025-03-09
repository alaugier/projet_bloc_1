import pandas as pd
from pymongo import MongoClient
import json
from datetime import datetime
import os
import logging
import sys

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


def csv_to_mongodb(csv_filepath, db_name, collection_name, connection_string="mongodb://localhost:27017/", replace_existing=False):
    """
    Importe les données d'un fichier CSV dans une collection MongoDB.
    
    Args:
        csv_filepath (str): Chemin vers le fichier CSV à importer
        db_name (str): Nom de la base de données MongoDB
        collection_name (str): Nom de la collection MongoDB
        connection_string (str): Chaîne de connexion à MongoDB
        replace_existing (bool): Si True, remplace la collection existante; sinon ajoute à la collection existante
    
    Returns:
        int: Nombre de documents insérés
    """
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
        elif wsl_exists:
            actual_path = wsl_filepath
            logger.info(f"Utilisation du chemin WSL: {wsl_filepath}")
        else:
            logger.error(f"Le fichier n'existe ni sous Windows ({csv_filepath}) ni sous WSL ({wsl_filepath})")
            return 0
    
        # Connexion à MongoDB
        client = MongoClient(connection_string)
        db = client[db_name]
        collection = db[collection_name]
        
        logger.info(f"Connexion établie à la base de données '{db_name}'")
        
        # Lecture du fichier CSV avec encodage UTF-8
        logger.info(f"Lecture du fichier CSV: {actual_path}")
        df = pd.read_csv(actual_path, encoding='utf-8')
        
        # Log des informations sur le DataFrame
        logger.info(f"Dimensions du DataFrame après nettoyage: {df.shape}")
        logger.info(f"Colonnes: {df.columns.tolist()}")
        
        # Conversion des coordonnées du format texte au format liste de nombres
        def parse_coordinates(coord_str):
            if isinstance(coord_str, str) and coord_str.strip():
                try:
                    # Format attendu: "(lat, lon)"
                    coord_str = coord_str.strip("()").split(",")
                    return [float(coord_str[0].strip()), float(coord_str[1].strip())]
                except Exception as e:
                    logger.warning(f"Erreur de conversion des coordonnées: {str(coord_str)[:30]}. Erreur: {str(e)}")
                    return None
            return None
        
        # Appliquer la conversion sur la colonne des coordonnées
        if 'coordinates' in df.columns:
            df['coordinates'] = df['coordinates'].apply(parse_coordinates)
            # Log du nombre de coordonnées valides
            valid_coords = df['coordinates'].notnull().sum()
            logger.info(f"Coordonnées valides: {valid_coords}/{len(df)}")
        
        # Conversion des dictionnaires stockés sous forme de chaînes
        def parse_dict(dict_str):
            if isinstance(dict_str, str) and dict_str.strip():
                try:
                    # Remplacer les quotes simples par des doubles pour la compatibilité JSON
                    cleaned_str = dict_str.replace("'", "\"")
                    return json.loads(cleaned_str)
                except Exception as e:
                    logger.warning(f"Erreur de conversion du dictionnaire: {str(dict_str)[:30]}... Erreur: {str(e)}")
                    return {}
            return {} if pd.isna(dict_str) else dict_str
        
        # Appliquer la conversion sur toutes les colonnes qui pourraient contenir des dictionnaires
        dict_columns = ['public_facilities']
        for col in dict_columns:
            if col in df.columns:
                logger.info(f"Conversion de la colonne {col} en dictionnaires")
                df[col] = df[col].apply(parse_dict)
        
        def parse_multipolygon_coordinates(polygon_str):
            """
            Convertit une chaîne représentant un MultiPolygon en structure de données GeoJSON.
            
            Format attendu en entrée: une chaîne représentant un tableau de polygones GeoJSON
            Format de sortie: structure GeoJSON pour un MultiPolygon
            
            Args:
                polygon_str (str): Chaîne représentant les coordonnées du MultiPolygon
                
            Returns:
                list: Liste de polygones au format GeoJSON, ou None en cas d'erreur
            """
            if not isinstance(polygon_str, str) or not polygon_str.strip():
                return None
                
            try:
                # Convertir la chaîne en liste Python
                import ast
                coords_list = ast.literal_eval(polygon_str)
                
                # Vérifier si nous avons un MultiPolygon (liste de polygones)
                if isinstance(coords_list, list) and all(isinstance(polygon, list) for polygon in coords_list):
                    
                    # Pour chaque polygone, vérifier que le premier et le dernier point coïncident
                    for i, polygon in enumerate(coords_list):
                        if isinstance(polygon, list) and len(polygon) > 0 and isinstance(polygon[0], list):
                            ring = polygon[0]
                            if ring and ring[0] != ring[-1]:
                                # Fermer le polygone en ajoutant le premier point à la fin
                                logger.info(f"Fermeture automatique du polygone {i}")
                                ring.append(ring[0])
                        
                    return coords_list
                    
                # Si c'est un seul polygone, le transformer en MultiPolygon
                elif isinstance(coords_list, list) and len(coords_list) > 0 and all(isinstance(point, list) and len(point) == 2 for point in coords_list[0]):
                    # Vérifier si le polygone est fermé
                    if coords_list[0][0] != coords_list[0][-1]:
                        coords_list[0].append(coords_list[0][0])
                    
                    # Format MultiPolygon: [[[x1,y1], [x2,y2], ...]]
                    return [coords_list]
                    
                else:
                    logger.warning(f"Format de MultiPolygon invalide")
                    return None
                        
            except Exception as e:
                logger.warning(f"Erreur lors de la conversion du MultiPolygon: {str(e)}")
                logger.debug(f"Chaîne problématique: {str(polygon_str)[:100]}...")
                return None
        
        # Fonction pour déterminer le type de géométrie
        def determine_geometry_type(coords):
            if coords is None:
                return None
                
            # Si nous avons une liste de polygones où chaque polygone est une liste de listes de points
            if all(isinstance(polygon, list) and all(isinstance(ring, list) for ring in polygon) 
                for polygon in coords):
                # Si plus d'un polygone, c'est un MultiPolygon
                if len(coords) > 1:
                    return {
                        "type": "MultiPolygon",
                        "coordinates": coords
                    }
                # Si un seul polygone, c'est un Polygon
                else:
                    return {
                        "type": "Polygon",
                        "coordinates": coords[0]
                    }
            
            return None
    
        # Ajout d'un timestamp d'importation
        current_time = datetime.now()

        # Traitement des coordonnées polygonales
        if 'geometry.coordinates' in df.columns:
            logger.info("Conversion de la colonne geometry.coordinates en format GeoJSON MultiPolygon")
            try:
                # Afficher quelques exemples de valeurs brutes
                logger.info("Exemple de valeur brute dans geometry.coordinates:")
                sample_value = df['geometry.coordinates'].iloc[0] if not df['geometry.coordinates'].empty else "Aucune valeur"
                logger.info(str(sample_value)[:500] + "..." if len(str(sample_value)) > 500 else str(sample_value))
                
                df['geometry.coordinates'] = df['geometry.coordinates'].apply(parse_multipolygon_coordinates)
        
                # Log du nombre de polygones valides
                valid_polygons = df['geometry.coordinates'].notnull().sum()
                logger.info(f"MultiPolygones valides: {valid_polygons}/{len(df)}")
                
                # Créer un champ geometry complet au format GeoJSON
                df['geometry'] = df['geometry.coordinates'].apply(determine_geometry_type)
                
            except Exception as e:
                logger.error(f"Erreur lors du traitement des polygones: {str(e)}", exc_info=True)

        # Conversion du DataFrame en liste de dictionnaires
        data_to_insert = df.to_dict('records')
        
        # Ajout du timestamp et d'autres métadonnées à chaque document
        for doc in data_to_insert:
            doc['import_timestamp'] = current_time
            doc['source_file'] = os.path.basename(actual_path)
        
        # Gestion de la collection existante - MODIFIÉ POUR SUPPRIMER LE TIMESTAMP
        if collection_name in db.list_collection_names():
            if replace_existing:
                # Supprimer les documents existants
                logger.info(f"Suppression des documents existants dans la collection {collection_name}")
                collection.delete_many({})
            else:
                # Avertir que la collection existe déjà
                logger.info(f"La collection {collection_name} existe déjà. Les données seront ajoutées à cette collection.")

        # Insertion des données dans MongoDB avec gestion des erreurs
        if not data_to_insert:
            logger.warning("Aucune donnée à insérer")
            return 0
            
        logger.info(f"Insertion de {len(data_to_insert)} documents dans la collection '{collection.name}'")
        
        # Utilisation de bulk_write pour de meilleures performances
        batch_size = 1000  # Ajuster selon la taille des documents
        num_inserted = 0
        
        for i in range(0, len(data_to_insert), batch_size):
            batch = data_to_insert[i:i+batch_size]
            result = collection.insert_many(batch)
            num_inserted += len(result.inserted_ids)
            logger.info(f"Batch {i//batch_size + 1}: {len(result.inserted_ids)} documents insérés")
        
        # Création d'index
        if 'coordinates' in df.columns:
            logger.info("Création d'un index géospatial sur le champ 'coordinates'")
            collection.create_index([("coordinates", "2d")])
        
        # Création d'autres index utiles
        index_fields = ['name', 'code_insee', 'postal_code']
        for field in index_fields:
            if field in df.columns:
                logger.info(f"Création d'un index sur le champ '{field}'")
                collection.create_index(field)
        
        logger.info(f"Importation terminée avec succès! {num_inserted} documents insérés.")
        return num_inserted
    
    except Exception as e:
        logger.error(f"Erreur lors de l'importation des données: {str(e)}", exc_info=True)
        return 0
    finally:
        # Fermeture de la connexion
        if client:
            client.close()
            logger.info("Connexion à MongoDB fermée")


def main():
    # Configuration des imports à réaliser
    imports = [
        {
            "csv_file": "C:\\Users\\Utilisateur\\Projet_Bloc_1\\cities_around_rennes.csv",
            "collection_name": "wikidata_cities_around_rennes",
            "replace_existing": False
        },
        {
            "csv_file": "C:\\Users\\Utilisateur\\Projet_Bloc_1\\api_indicateurs_donnees_foncieres.csv",
            "collection_name": "indicateurs_donnees_foncieres_around_rennes",
            "replace_existing": False
        },
        {
            "csv_file": "C:\\Users\\Utilisateur\\Projet_Bloc_1\\api_geometries_donnees_foncieres.csv",
            "collection_name": "geometries_donnees_foncieres_around_rennes",
            "replace_existing": False
        }
    ]
    
    database_name = "projet_bloc_1"
    connection_string = "mongodb://localhost:27017/"
    
    # Exécution des imports
    logger.info(f"Démarrage de l'importation de {len(imports)} fichiers CSV")
    
    results = []
    for import_config in imports:
        logger.info(f"Traitement du fichier: {import_config['csv_file']}")
        num_imported = csv_to_mongodb(
            import_config["csv_file"],
            database_name,
            import_config["collection_name"],
            connection_string,
            import_config["replace_existing"]
        )
        results.append({
            "file": import_config["csv_file"],
            "collection": import_config["collection_name"],
            "documents_imported": num_imported
        })
    
    # Affichage du résumé
    logger.info("\n=== RÉSUMÉ DE L'IMPORTATION ===")
    for result in results:
        if result["documents_imported"] > 0:
            logger.info(f"{result['file']} -> {result['collection']}: {result['documents_imported']} documents importés")
        else:
            logger.info(f"{result['file']} -> {result['collection']}: échec de l'importation")
    
    logger.info(f"Importation terminée. Base de données: {database_name}")


if __name__ == "__main__":
    main()