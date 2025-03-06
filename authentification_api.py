import os
import jwt
import datetime
from fastapi import FastAPI, Query, Depends, HTTPException, Header
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import mysql.connector
from typing import Optional
from pydantic import BaseModel
from dotenv import load_dotenv

# Chargement des variables d'environnement
load_dotenv('var.env')
SECRET_KEY = os.getenv("SECRET_KEY")
API_PASSWORD = os.getenv("API_PASSWORD")
host = os.getenv('host')
database = os.getenv('database')
user = os.getenv('user')
mdp = os.getenv("mdp")

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
    min_surface: int = Query(0, alias="surface_min"),
    max_surface: int = Query(500, alias="surface_max"),
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Cette fonction permet de récupérer les biens immobiliers dont la surface habitable est comprise entre deux valeurs."""
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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host='127.0.0.1', port=8000)