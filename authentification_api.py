import os
import jwt
import datetime
from fastapi import FastAPI, Query, Depends, HTTPException, Header
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import mysql.connector
from typing import Optional, List, Dict, Any
from pydantic import BaseModel
from dotenv import load_dotenv
import pymongo
from bson.json_util import dumps
import json

# Chargement des variables d'environnement
load_dotenv('var.env')
SECRET_KEY = os.getenv("SECRET_KEY")
API_PASSWORD = os.getenv("API_PASSWORD")
host = os.getenv('host')
database = os.getenv('database')
user = os.getenv('user')
mdp = os.getenv("mdp")

# Configuration MongoDB
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = "projet_bloc_1"

app = FastAPI()

# Sécurité avec JWT
security = HTTPBearer()

class TokenRequest(BaseModel):
    """
    Modèle de requête pour obtenir un token JWT.
    
    - password : Mot de passe requis pour obtenir un token.
    - duration : Durée de validité du token en secondes (par défaut : 3600s / 1h).
    """
    password: str
    duration: Optional[int] = 3600

def create_jwt(duration: int) -> str:
    """
    Génère un token JWT valide pour une durée spécifiée.
    
    :param duration: Durée de validité du token en secondes.
    :return: Token JWT encodé.
    """
    expiration = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=duration)
    return jwt.encode({"exp": expiration}, SECRET_KEY, algorithm="HS256")

@app.post("/token")
def generate_token(request: TokenRequest):
    """
    Endpoint pour générer un token JWT après authentification par mot de passe.
    
    :param request: Objet contenant le mot de passe et la durée du token.
    :return: Dictionnaire contenant le token généré.
    """
    print("🔹 Requête reçue pour /token")
    print("Mot de passe reçu :", request.password)  # Ajout du print pour débogage
    print("Mot de passe attendu :", API_PASSWORD)  # Vérifie ce qui est stocké
    if request.password != API_PASSWORD:
        raise HTTPException(status_code=401, detail="Invalid password")
    return {"token": create_jwt(request.duration)}

async def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """
    Vérifie la validité du token JWT fourni par l'utilisateur.
    
    :param credentials: Token JWT inclus dans la requête.
    :raises HTTPException: Si le token est expiré ou invalide.
    """
    try:
        jwt.decode(credentials.credentials, SECRET_KEY, algorithms=["HS256"])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

def get_db_connection():
    """
    Établit une connexion à la base de données MySQL.
    
    :return: Objet de connexion MySQL.
    """
    return mysql.connector.connect(
        host=host,
        user=user,
        password=mdp,
        database=database
    )

def get_mongo_connection():
    """
    Établit une connexion à la base de données MongoDB.
    
    :return: Client MongoDB connecté à la base de données du projet.
    """
    client = pymongo.MongoClient(MONGO_URI)
    return client[MONGO_DB]

@app.get("/biens/agence/{id_agence}")
async def get_biens_by_agence(
    id_agence: int,
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """
    Récupère tous les biens immobiliers gérés par une agence spécifique.
    
    :param id_agence: ID unique de l'agence dont on veut lister les biens.
    :param credentials: Token JWT pour l'authentification.
    :return: Liste des biens immobiliers de l'agence.
    """
    await verify_token(credentials)
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)
    query = """
        SELECT * FROM appartement WHERE id_appartement IN (SELECT id_appartement FROM appartement_agence WHERE id_agence = %s)
        UNION ALL
        SELECT * FROM maison WHERE id_maison IN (SELECT id_maison FROM maison_agence WHERE id_agence = %s)
    """
    cursor.execute(query, (id_agence, id_agence))
    results = cursor.fetchall()
    cursor.close()
    connection.close()
    return results

@app.get("/biens/filtre-prix")
async def get_biens_by_price_range(
    min_price: int = Query(0, alias="prix_min", description="Prix minimum du bien"),
    max_price: int = Query(1000000, alias="prix_max", description="Prix maximum du bien"),
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """
    Récupère les biens immobiliers dont le prix est compris entre deux valeurs données.
    
    :param min_price: Prix minimum du bien (par défaut 0€).
    :param max_price: Prix maximum du bien (par défaut 1 000 000€).
    :param credentials: Token JWT pour l'authentification.
    :return: Liste des biens correspondant aux critères de prix.
    """
    await verify_token(credentials)
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)
    query = """
        SELECT * FROM appartement WHERE prix BETWEEN %s AND %s
        UNION ALL
        SELECT * FROM maison WHERE prix BETWEEN %s AND %s
    """
    cursor.execute(query, (min_price, max_price, min_price, max_price))
    results = cursor.fetchall()
    cursor.close()
    connection.close()
    return results

@app.get("/biens/filtre-surface")
async def get_biens_by_surface_range(
    min_surface: int = Query(0, alias="surface_min", description="Surface habitable minimum en m²"),
    max_surface: int = Query(500, alias="surface_max", description="Surface habitable maximum en m²"),
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """
    Récupère les biens immobiliers dont la surface habitable est comprise entre deux valeurs données.
    
    :param min_surface: Surface minimum du bien en m² (par défaut 0 m²).
    :param max_surface: Surface maximum du bien en m² (par défaut 500 m²).
    :param credentials: Token JWT pour l'authentification.
    :return: Liste des biens correspondant aux critères de surface.
    """
    await verify_token(credentials)
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)
    
    query = """
        SELECT * FROM appartement WHERE surf_hab_m2 BETWEEN %s AND %s
        UNION ALL
        SELECT * FROM maison WHERE surf_hab_m2 BETWEEN %s AND %s
    """
    cursor.execute(query, (min_surface, max_surface, min_surface, max_surface))
    results = cursor.fetchall()
    
    cursor.close()
    connection.close()
    return results

@app.get("/mongo/descriptions-logements")
async def get_descriptions_logements(
    type_bien: Optional[str] = Query(None, description="Type de bien immobilier"),
    ville: Optional[str] = Query(None, description="Ville où se situe le bien"),
    parking: Optional[bool] = Query(None, description="Présence d'un parking ou garage"),
    jardin: Optional[bool] = Query(None, description="Présence d'un jardin ou terrain"),
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """
    Récupère les descriptions de logements depuis MongoDB avec des filtres optionnels.
    
    :param type_bien: Filtre par type de bien (appartement, maison, etc.).
    :param ville: Filtre par ville.
    :param parking: Filtre par présence de parking/garage.
    :param jardin: Filtre par présence de jardin/terrain.
    :param credentials: Token JWT pour l'authentification.
    :return: Liste des descriptions de logements correspondant aux critères.
    """
    await verify_token(credentials)
    
    # Construction du filtre de requête
    query_filter = {}
    if type_bien:
        query_filter["Type de bien"] = {"$regex": type_bien, "$options": "i"}
    if ville:
        # Rendre la recherche plus flexible pour les noms de ville
        ville_cleaned = ville.replace("-", " ").replace("  ", " ")
        query_filter["Ville"] = {"$regex": f".*{ville_cleaned}.*", "$options": "i"}
    
    # Pour les champs qui sont des valeurs numériques 0 ou 1
    if parking is not None:
        query_filter["Parking / Garage"] = 1 if parking else 0
    
    if jardin is not None:
        query_filter["Jardin / Terrain"] = 1 if jardin else 0
    
    # Sélection des champs à retourner
    projection = {
        "Type de bien": 1,
        "Ville": 1,
        "title": 1,
        "short_description": 1,
        "long_description": 1,
        "chauffage": 1,
        "Exposition": 1,
        "Parking / Garage": 1,
        "Jardin / Terrain": 1,
        "Proximité MétroBusGare SNCFGare RERCommercesEcolesEspaces verts": 1,
        "_id": 0
    }
    
    mongo_db = get_mongo_connection()
    
    # Debug: afficher le query_filter
    print("Filtre de recherche:", query_filter)
    
    # Exécution de la requête et récupération des résultats
    results = list(mongo_db["description_logements"].find(query_filter, projection))
    
    # Debug: nombre de résultats
    print(f"Nombre de résultats trouvés: {len(results)}")
    
    # Conversion des résultats BSON en JSON
    return json.loads(dumps(results))

@app.get("/mongo/indicateurs-foncieres")
async def get_indicateurs_foncieres(
    annee_mutation: Optional[int] = Query(None, description="Année de mutation"),
    code_geo: Optional[int] = Query(None, description="Code géographique"),
    libelle_geo: Optional[str] = Query(None, description="Libellé géographique (nom de commune/département)"),
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """
    Récupère les indicateurs fonciers depuis MongoDB avec des filtres optionnels.
    
    :param annee_mutation: Filtre par année de mutation.
    :param code_geo: Filtre par code géographique.
    :param libelle_geo: Filtre par libellé géographique (nom de commune ou département).
    :param credentials: Token JWT pour l'authentification.
    :return: Liste des indicateurs fonciers correspondant aux critères.
    """
    await verify_token(credentials)
    
    # Construction du filtre de requête
    query_filter = {}
    if annee_mutation:
        query_filter["annee_mutation"] = annee_mutation
    if code_geo:
        query_filter["code_geo"] = code_geo
    if libelle_geo:
        query_filter["libelle_geo"] = {"$regex": libelle_geo, "$options": "i"}  # Recherche insensible à la casse
    
    # Sélection des champs à retourner
    projection = {
        "annee_mutation": 1,
        "code_geo": 1,
        "libelle_geo": 1,
        "valeur_fonciere_median_cod111": 1,
        "prix_m2_median_cod111": 1,
        "surface_batie_median_cod111": 1,
        "_id": 0
    }
    
    mongo_db = get_mongo_connection()
    
    # Debug: afficher le query_filter
    print("Filtre de recherche indicateurs:", query_filter)
    
    results = list(mongo_db["indicateurs_donnees_foncieres_around_rennes"].find(query_filter, projection))
    
    # Debug: nombre de résultats
    print(f"Nombre de résultats indicateurs trouvés: {len(results)}")
    
    # Conversion des résultats BSON en JSON
    return json.loads(dumps(results))

@app.get("/mongo/villes-autour-rennes")
async def get_villes_autour_rennes(
    distance_max: Optional[float] = Query(None, description="Distance maximale de Rennes en km"),
    population_min: Optional[float] = Query(None, description="Population minimale en milliers d'habitants"),
    zone_type: Optional[str] = Query(None, description="Type de zone (urbaine, rurale, etc.)"),
    services_requis: Optional[List[str]] = Query(None, description="Liste des services publics requis"),
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """
    Récupère les données des villes autour de Rennes depuis MongoDB avec des filtres optionnels.
    
    :param distance_max: Filtre par distance maximale de Rennes en km.
    :param population_min: Filtre par population minimale en milliers d'habitants.
    :param zone_type: Filtre par type de zone (urbaine, rurale, etc.).
    :param services_requis: Filtre par présence de certains services publics.
    :param credentials: Token JWT pour l'authentification.
    :return: Liste des villes correspondant aux critères.
    """
    await verify_token(credentials)
    
    # Construction du filtre de requête
    query_filter = {}
    if distance_max is not None:
        query_filter["distance_to_rennes"] = {"$lte": distance_max}
    if population_min is not None:
        query_filter["population"] = {"$gte": population_min}
    if zone_type:
        query_filter["zone_type"] = {"$regex": zone_type, "$options": "i"}
    if services_requis:
        # Utilisation de $all pour vérifier que tous les services requis sont présents
        query_filter["public_facilities"] = {"$all": services_requis}
    
    # Sélection des champs à retourner
    projection = {
        "name": 1,
        "coordinates": 1,
        "distance_to_rennes": 1,
        "population": 1,
        "density": 1,
        "public_facilities": 1,
        "zone_type": 1,
        "_id": 0
    }
    
    mongo_db = get_mongo_connection()
    
    # Debug: afficher le query_filter
    print("Filtre de recherche villes:", query_filter)
    
    results = list(mongo_db["wikidata_cities_around_rennes"].find(query_filter, projection))
    
    # Debug: nombre de résultats
    print(f"Nombre de résultats villes trouvés: {len(results)}")
    
    # Conversion des résultats BSON en JSON
    return json.loads(dumps(results))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host='127.0.0.1', port=8000)