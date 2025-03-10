import os
import sys
import platform
import unidecode
# import pyperclip as clip
import pickle
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import json
import csv
import datetime
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
import time
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
import warnings
warnings.filterwarnings('ignore')
pd.set_option('future.no_silent_downcasting', True)

# Détection si la sortie est redirigée vers un fichier (comme dans crontab)
is_redirected = not sys.stdout.isatty()

# Configuration des barres de progression tqdm
if is_redirected:
    # Désactiver tqdm et utiliser uniquement les prints pour les fichiers log
    from tqdm import tqdm as original_tqdm
    def tqdm_disabled(iterable, *args, **kwargs):
        return iterable
    tqdm = tqdm_disabled
else:
    # Utiliser tqdm normalement dans un environnement interactif
    from tqdm import tqdm

# Détection automatique de l'environnement
is_windows = platform.system() == 'Windows'

# Configuration des chemins selon l'environnement
if is_windows:
    # Chemins pour Windows
    base_path = 'C:\\Users\\Utilisateur\\Projet_Bloc_1'
    archives_path = f'{base_path}\\Archives'
    chromedriver_path = f'{base_path}/chromedriver.exe'
else:
    # Chemins pour WSL
    base_path = '/mnt/c/Users/Utilisateur/Projet_Bloc_1'
    archives_path = f'{base_path}/Archives'
    chromedriver_path = f'{base_path}/chromedriver.exe'

# Fonction pour obtenir le chemin complet d'un fichier
def get_path(filename):
    return os.path.join(base_path, filename)

def get_archives_path(filename):
    return os.path.join(archives_path, filename)

chrome_options = webdriver.ChromeOptions()

service = Service(chromedriver_path)

ville = 'rennes'
nb_page = 1

links_pages = []
def find_links_pages():
    """
    Recherche et collecte les URLs des pages de résultats sur Paru Vendu.
    
    Parcourt tous les éléments d'ancrage (liens) de la page courante et filtre 
    ceux qui correspondent aux URLs des pages de résultats pour la ville spécifiée.
    
    Returns:
        int: Nombre de liens de pages trouvés
    """
    n=0
    print(f"[INFO] Recherche des liens de pages pour la ville: {ville}")
    elems = driver.find_elements(By.XPATH, "//a[@href]") 
    for elem in tqdm(elems):
        link = elem.get_attribute("href")
        if not link in links_pages:
            if link.startswith(f'https://www.paruvendu.fr/immobilier/vente/{ville}/?rechpv=1&tt=1&tbApp=1&tbDup=1&tbChb=1&tbLof=1&tbAtl=1&tbPla=1&tbMai=1&tbVil=1&tbCha=1&tbPro=1&tbHot=1&tbMou=1&tbFer=1&at=1&nbp0=99&pa=FR&lol=30&ray=50&codeINSEE=35XX0,'):
                # print(link)
                links_pages.append(link)
                n+=1
    print(f"[INFO] Total des liens de pages trouvés: {n}")
    return n

links_ads = []
def find_links_ads():
    """
    Recherche et collecte les URLs des annonces immobilières sur la page courante.
    
    Parcourt tous les éléments d'ancrage (liens) de la page courante et filtre
    ceux qui correspondent aux URLs des annonces immobilières.
    
    Returns:
        int: Nombre de liens d'annonces trouvés
    """
    n_i=0
    print(f"[INFO] Recherche des liens d'annonces sur la page courante")
    elems = driver.find_elements(By.XPATH, "//a[@href]") 
    for elem in tqdm(elems):
        link = elem.get_attribute("href")
        if not link in links_ads:
            if link.startswith('https://www.paruvendu.fr/immobilier/vente/'):
                # print(link)
                links_ads.append(link)
                n_i+=1
    print(f"[INFO] Nombre d'annonces trouvées sur cette page: {n_i}")
    return n_i

def find_last_index(s, char):
    """
    Trouve l'index de la dernière occurrence d'un caractère dans une chaîne.
    
    Args:
        s (str): Chaîne de caractères à analyser
        char (str): Caractère à rechercher
        
    Returns:
        int: Index de la dernière occurrence du caractère, -1 si non trouvé
    """
    return max([i for i, letter in enumerate(s) if letter == char])

print("[INFO] Initialisation du scraping Paru Vendu")
time.sleep(5)
driver = webdriver.Chrome(service=service, options=chrome_options)
driver.execute_script("document.body.style.zoom='100%'")
driver.get(f'https://www.paruvendu.fr/immobilier/vente/{ville}/?rechpv=1&tt=1&tbApp=1&tbDup=1&tbChb=1&tbLof=1&tbAtl=1&tbPla=1&tbMai=1&tbVil=1&tbCha=1&tbPro=1&tbHot=1&tbMou=1&tbFer=1&at=1&nbp0=99&pa=FR&lol=30&ray=50&codeINSEE=35XX0,')

n = find_links_pages()
print(f"[INFO] Début de la collecte des liens d'annonces")
while n>0:
    time.sleep(5)
    print(f'[INFO] Traitement des {n} pages trouvées - page principale {nb_page}')
    for i in range(n):
        print(f'[PROGRESSION] Page {i+1}/{n} en cours de traitement')
        time.sleep(5)
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.execute_script("document.body.style.zoom='100%'")
        driver.get(links_pages[i+nb_page-1])
        page_source_i = driver.page_source
        n_i = find_links_ads()
        print(f"[RÉSULTAT] Page {i+1}/{n}: {n_i} annonces trouvées")
        time.sleep(3)
        driver.close()
    time.sleep(5)
    nb_page+=n
    print(f"[INFO] Passage à la page principale suivante: {nb_page}")
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.execute_script("document.body.style.zoom='100%'")
    driver.get(f'https://www.paruvendu.fr/immobilier/vente/{ville}/?rechpv=1&tt=1&tbApp=1&tbDup=1&tbChb=1&tbLof=1&tbAtl=1&tbPla=1&tbMai=1&tbVil=1&tbCha=1&tbPro=1&tbHot=1&tbMou=1&tbFer=1&at=1&nbp0=99&pa=FR&lol=30&ray=50&codeINSEE=35XX0,&p={nb_page}')
    n = find_links_pages()
driver.quit()
print(f"[INFO] Fin de la collecte des liens - Total des annonces trouvées: {len(links_ads)}")

# Save the raw flattened_listing_ads list into a file

# ct stores current time (ct)
ct = datetime.datetime.now()

# Convert the ct result into a string
st = str(ct.year)+'-'+str(ct.month)+'-'+str(ct.day)+'-'+str(ct.hour)+'-'+str(ct.minute)+'-'+str(ct.second)+'-'+str(ct.microsecond)

paru_vendu_listing_ads_path_filename = get_archives_path(f'paru_vendu_links_ads_rennes_{st}.txt')

# Write file
with open(paru_vendu_listing_ads_path_filename, 'w+', encoding="utf-8") as f:
    
    for item in links_ads:
        # Write elements of list
        f.write('%s;\n' %item) # We put a semicolon (;) to separate the elements of this list 
                               # (it is assumed that the elements don't contain a semicolon. 
                               # If not, replace semicolon by another character)
    
    print("[INFO] Fichier des liens d'annonces enregistré avec succès")

# Close the file
f.close()

# Get the date of latest updates from archives
l = len('paru_vendu_links_ads_rennes_')
lst_datetime = []
for dirpath, dirnames, filenames in os.walk(archives_path):
    for filename in filenames:
        if 'paru_vendu_links_ads_rennes_' in filename:
            # print(filename)
            n_f = len(filename)
            # datetime in string format
            input = filename[l:n_f-4]
            # print(input)
            format = '%Y-%m-%d-%H-%M-%S-%f'
        
            # convert from string format to datetime format
            lst_datetime.append(datetime.datetime.strptime(input, format))
print(f"[INFO] Recherche du dernier fichier d'archives")    
latest_recorded_time = max(lst_datetime)
print(f"[INFO] Date du dernier fichier trouvé: {latest_recorded_time}")

# Recover the latest recorded datas which contains links of lodgements and put them into a list
# Don't forget to run the cell above whose output is the latest recorded time

# Date of latest recording without zero padding in string format
dt_parts = latest_recorded_time.strftime('%Y-%m-%d-%H-%M-%S-%f').split('-')
# Ne pas toucher aux microsecondes (dernier élément)
for i in range(len(dt_parts) - 1):  # Ne pas traiter le dernier élément (microsecondes)
    if dt_parts[i].startswith('0') and len(dt_parts[i]) > 1:
        dt_parts[i] = dt_parts[i][1:]  # Enlever le zéro initial si présent
latest_recorded_time_to_string = '-'.join(dt_parts)
print(f"[INFO] Récupération des données du fichier: {latest_recorded_time_to_string}")

latest_paru_vendu_ads_links_path_filename = get_archives_path(f'paru_vendu_links_ads_rennes_{latest_recorded_time_to_string}.txt')
print(f"[INFO] Chargement du fichier: {latest_paru_vendu_ads_links_path_filename}")

# Store the appartment links into a list
latest_recorded_links = []
with open(latest_paru_vendu_ads_links_path_filename, 'r', encoding="utf-8") as f:
    # Display content of the file
    for x in f.readlines():
        latest_recorded_links.append(x.replace('\n', ''))

# Close the file
f.close()

# Make a copy of the latest recorded links list
links_ads = latest_recorded_links.copy()
print(f"[INFO] {len(links_ads)} liens chargés depuis les archives")

lst_temp = []
for i in range(len(links_ads)):
    if '?' in links_ads[i]:
        lst_temp.append(links_ads[i])
set_ads_links = set(links_ads).difference(set(lst_temp))
links_ads = list(set_ads_links)
print(f"[INFO] Après nettoyage: {len(links_ads)} liens valides")

start = 1+find_last_index(links_ads[0],'/')
links_ads[0][start:].replace(';', '')

lst_num_ads = []
for i in range(len(links_ads)):
    start = 1+find_last_index(links_ads[i],'/')
    lst_num_ads.append(links_ads[i][start:].replace(';', ''))

lst_type_de_biens = []
for i in range(len(links_ads)):
    if 'maison' in links_ads[i]:
        lst_type_de_biens.append('maison')
    else:
        lst_type_de_biens.append('appartement')

# Nombre total d'annonces
N = len(links_ads)
print(f"[INFO] Nombre total d'annonces à traiter: {N}")

n = 50
k = 1
lst_title = []
lst_short_description = []
lst_long_description = []
lst_data_fields = []
lst_annonceurs = []
lst_dpe =[]

# Extraction des données
print(f"[INFO] Début de l'extraction des données par lots de {n} annonces")
while k<=N//n:
    print(f"[PROGRESSION] Traitement du lot {k}/{N//n + (1 if N%n > 0 else 0)} ({(k-1)*n+1}-{min(k*n, N)}/{N})")
    time.sleep(10)
    for i in tqdm(range((k-1)*n, k*n)):
        try:
            print(f"[PROGRESSION] Traitement de l'annonce {i+1}/{N} ({round((i+1)/N*100, 2)}%)")
            time.sleep(5)
            driver = webdriver.Chrome(service=service, options=chrome_options)
            driver.execute_script("document.body.style.zoom='100%'")
            time.sleep(np.random.randint(1, 2))
            driver.get(links_ads[i])
            page_source_i = driver.page_source
            soup_i = BeautifulSoup(page_source_i, "html.parser")
            try:
                lst_title.append(soup_i.find('meta', property="og:title").get('content'))
            except Exception:
                lst_title.append('')
            try:
                lst_short_description.append(soup_i.find('meta', property="og:description").get('content').replace('&nbsp;', ' '))
            except Exception:
                lst_short_description.append('')
            try:
                lst_long_description.append(soup_i.find('div', class_='txt_annonceauto txt_annoncetrunc').text.replace('\n','').replace('  ',''))
            except Exception:
                lst_long_description.append('')
            try:
                dict_data_fields_i = {'prix': soup_i.find('div', class_=['prixactionalerte-box','im12_hd_prix']).find('div').text.replace('\n', '')}
                for data in soup_i.find_all('ul', class_='crit-alignbloc'):
                    for subdata in data.find_all('li'):
                        if subdata.has_attr("class"):
                            if subdata['class']==["nbp"]:
                                dict_data_fields_i['nombre de pièces'] = subdata.text[:2].replace('\n', '').replace('\t','')
                            else:
                                dict_data_fields_i['surface habitable'] = subdata.text.replace('\n', '').replace('\t','')
                        elif 'chambres' in subdata.text and 'Agencement' not in subdata.text:
                            dict_data_fields_i['nombre de chambres'] = subdata.text[:2].replace('\n', '').replace('\t','')
                        elif 'Extérieur' in subdata.text:
                            ind_surft=1+find_last_index(subdata.find('span').text,':')
                            dict_data_fields_i['surface terrain'] = subdata.find('span').text[ind_surft:].replace('\n', '').replace(' ', '').replace('\t','')
                        elif 'Annexes' in subdata.text:
                            dict_data_fields_i['Annexes'] = subdata.find('span').text.replace('\n', '').replace('\t','')
                        elif 'Dépendance' in subdata.text:
                            dict_data_fields_i['Dépendance'] = subdata.find('span').text.replace('\n', '').replace('\t','')
                        elif 'Général' in subdata.text:
                            for elem in subdata.find_all('span'):
                                ind_elem = 1+find_last_index(elem.text, ':')
                                dict_data_fields_i[elem.text[:ind_elem-1].replace('\n', '').replace(' ', '')] = elem.text[ind_elem:].replace('\n', '').lstrip().rstrip()
                        elif 'Réf. annonce' in subdata.text:
                            dict_data_fields_i['Réf. annonce'] = subdata.find('span').text.replace('\n', '').replace('\t','')
                        elif 'Mise à jour' in subdata.text:
                            dict_data_fields_i['Mise à jour'] = subdata.find('span').text.replace('\n', '').replace('\t','')
                        else:
                            if 'Agencement' not in subdata.text:
                                dict_data_fields_i[subdata.text.replace('\n', '').replace('\t','')] = 1
            except Exception:
                dict_data_fields_i ={}
            lst_data_fields.append(dict_data_fields_i)
            try:
                try:
                    dict_annonceur_i = {'nom_annonceur':unidecode.unidecode(soup_i.find('p', class_='ba-nameannonceur').text.replace('\n', '').replace('  ', ''))}
                except Exception:
                    dict_annonceur_i = {'nom_annonceur':''}
                try:
                    dict_annonceur_i.update({"lien de l'annonceur" : soup_i.find('div', class_='blocannonceur_linklist').find('a').get('href')})
                    ind_0_i = find_last_index(dict_annonceur_i["lien de l'annonceur"],'-')
                    ind_1_i = find_last_index(dict_annonceur_i["lien de l'annonceur"][:ind_0_i],'-')
                    ind_2_i = 1+find_last_index(dict_annonceur_i["lien de l'annonceur"][:ind_0_i][:ind_1_i],'-')
                    dict_annonceur_i.update({"ville de l'annonceur" : dict_annonceur_i["lien de l'annonceur"][:ind_0_i][:ind_1_i][ind_2_i:]})  
                except Exception:
                    dict_annonceur_i.update({"lien de l'annonceur" : '', "ville de l'annonceur" :''})
                lst_annonceurs.append(dict_annonceur_i)   
            except Exception:
                lst_annonceurs.append({'nom_annonceur':'', "lien de l'annonceur" : '', "ville de l'annonceur" :''})
            try:
                dict_dpe_i = {} 
                for data in soup_i.find_all('div', class_="DPE_greyPadd"):
                    for subdata in data.find_all('div', class_='DPE_ng_flex'):
                        for elem in subdata.find_all('span'):
                            if 'NoteActive' in elem['class']:
                                dict_dpe_i['étiquette DPE'] = elem.text
                            else:
                                dict_dpe_i['étiquette DPE'] = soup_i.find('div', attrs={'class': lambda x: x.startswith('DPE_consEnerNote NoteEnerg') if x else ''}).text
                try:
                    dict_dpe_i.update({'consommation énergétique':soup_i.find('div', class_='DPE_consEnerTxt newDPE').find('span').text})
                except Exception:
                    dict_dpe_i.update({'consommation énergétique':''})
                try:
                    dict_dpe_i.update({'étiquette GPE':soup_i.find('div', class_='DPE_effSerreGlob newDPE_glob').find('div').text})
                except Exception:
                    dict_dpe_i.update({'étiquette GPE':''})
                try:
                    dict_dpe_i.update({'émission GPE':soup_i.find('div', class_='DPE_effSerreTxt').find('span').text})
                except Exception:
                    dict_dpe_i.update({'émission GPE':''})
                try:
                    dict_dpe_i.update({'date du bilan DPE':soup_i.find('p', class_='mentions_detailimmo m-0 text-center').text.replace('Fait le :', '').lstrip()})
                except Exception:
                    dict_dpe_i.update({'date du bilan DPE':''})
            except Exception:
                dict_dpe_i = {'étiquette DPE':'', 'consommation énergétique':'', 'étiquette GPE':'', 'émission GPE':'', 'date du bilan DPE':''} 
            lst_dpe.append(dict_dpe_i)
        except Exception as ex:
            time.sleep(3)
            print(f"[ERREUR] Annonce {i+1}: {ex}")
        finally:
            driver.quit()
            time.sleep(2)
    k+=1
    print(f"[INFO] Lot {k-1} terminé. {min((k-1)*n, N)}/{N} annonces traitées ({round(min((k-1)*n, N)/N*100, 2)}%)")

m = (k-1)*n

# Extraction des donn{\'e}es residuelles
if m < N:
    print(f"[INFO] Traitement des annonces restantes: {m+1}-{N}")
    for i in tqdm(range(m,N)):
        try:
            print(f"[PROGRESSION] Traitement de l'annonce {i+1}/{N} ({round((i+1)/N*100, 2)}%)")
            time.sleep(5)
            driver = webdriver.Chrome(service=service, options=chrome_options)
            driver.execute_script("document.body.style.zoom='100%'")
            time.sleep(np.random.randint(1, 2))
            driver.get(links_ads[i])
            page_source_i = driver.page_source
            soup_i = BeautifulSoup(page_source_i, "html.parser")
            try:
                lst_title.append(soup_i.find('meta', property="og:title").get('content'))
            except Exception:
                lst_title.append('')
            try:
                lst_short_description.append(soup_i.find('meta', property="og:description").get('content').replace('&nbsp;', ' '))
            except Exception:
                lst_short_description.append('')
            try:
                lst_long_description.append(soup_i.find('div', class_='txt_annonceauto txt_annoncetrunc').text.replace('\n','').replace('  ',''))
            except Exception:
                lst_long_description.append('')
            try:
                dict_data_fields_i = {'prix': soup_i.find('div', class_=['prixactionalerte-box','im12_hd_prix']).find('div').text.replace('\n', '')}
                for data in soup_i.find_all('ul', class_='crit-alignbloc'):
                    for subdata in data.find_all('li'):
                        if subdata.has_attr("class"):
                            if subdata['class']==["nbp"]:
                                dict_data_fields_i['nombre de pièces'] = subdata.text[:2].replace('\n', '').replace('\t','')
                            else:
                                dict_data_fields_i['surface habitable'] = subdata.text.replace('\n', '').replace('\t','')
                        elif 'chambres' in subdata.text and 'Agencement' not in subdata.text:
                            dict_data_fields_i['nombre de chambres'] = subdata.text[:2].replace('\n', '').replace('\t','')
                        elif 'Extérieur' in subdata.text:
                            ind_surft=1+find_last_index(subdata.find('span').text,':')
                            dict_data_fields_i['surface terrain'] = subdata.find('span').text[ind_surft:].replace('\n', '').replace(' ', '').replace('\t','')
                        elif 'Annexes' in subdata.text:
                            dict_data_fields_i['Annexes'] = subdata.find('span').text.replace('\n', '').replace('\t','')
                        elif 'Dépendance' in subdata.text:
                            dict_data_fields_i['Dépendance'] = subdata.find('span').text.replace('\n', '').replace('\t','')
                        elif 'Général' in subdata.text:
                            for elem in subdata.find_all('span'):
                                ind_elem = 1+find_last_index(elem.text, ':')
                                dict_data_fields_i[elem.text[:ind_elem-1].replace('\n', '').replace(' ', '')] = elem.text[ind_elem:].replace('\n', '').lstrip().rstrip()
                        elif 'Réf. annonce' in subdata.text:
                            dict_data_fields_i['Réf. annonce'] = subdata.find('span').text.replace('\n', '').replace('\t','')
                        elif 'Mise à jour' in subdata.text:
                            dict_data_fields_i['Mise à jour'] = subdata.find('span').text.replace('\n', '').replace('\t','')
                        else:
                            if 'Agencement' not in subdata.text:
                                dict_data_fields_i[subdata.text.replace('\n', '').replace('\t','')] = 1
            except Exception:
                dict_data_fields_i ={}
            lst_data_fields.append(dict_data_fields_i)
            try:
                try:
                    dict_annonceur_i = {'nom_annonceur':unidecode.unidecode(soup_i.find('p', class_='ba-nameannonceur').text.replace('\n', '').replace('  ', ''))}
                except Exception:
                    dict_annonceur_i = {'nom_annonceur':''}
                try:
                    dict_annonceur_i.update({"lien de l'annonceur" : soup_i.find('div', class_='blocannonceur_linklist').find('a', {'target':'_blank'}).get('href')})
                    ind_0_i = find_last_index(dict_annonceur_i["lien de l'annonceur"],'-')
                    ind_1_i = find_last_index(dict_annonceur_i["lien de l'annonceur"][:ind_0_i],'-')
                    ind_2_i = 1+find_last_index(dict_annonceur_i["lien de l'annonceur"][:ind_0_i][:ind_1_i],'-')
                    dict_annonceur_i.update({"ville de l'annonceur" : dict_annonceur_i["lien de l'annonceur"][:ind_0_i][:ind_1_i][ind_2_i:]})  
                except Exception:
                    dict_annonceur_i.update({"lien de l'annonceur" : '', "ville de l'annonceur" :''})
                lst_annonceurs.append(dict_annonceur_i)   
            except Exception:
                lst_annonceurs.append({'nom_annonceur':'', "lien de l'annonceur" : '', "ville de l'annonceur" :''})
            try:
                dict_dpe_i = {} 
                for data in soup_i.find_all('div', class_="DPE_greyPadd"):
                    for subdata in data.find_all('div', class_='DPE_ng_flex'):
                        for elem in subdata.find_all('span'):
                            if 'NoteActive' in elem['class']:
                                dict_dpe_i['étiquette DPE'] = elem.text
                            else:
                                dict_dpe_i['étiquette DPE'] = soup_i.find('div', attrs={'class': lambda x: x.startswith('DPE_consEnerNote NoteEnerg') if x else ''}).text
                try:
                    dict_dpe_i.update({'consommation énergétique':soup_i.find('div', class_='DPE_consEnerTxt newDPE').find('span').text})
                except Exception:
                    dict_dpe_i.update({'consommation énergétique':''})
                try:
                    dict_dpe_i.update({'étiquette GPE':soup_i.find('div', class_='DPE_effSerreGlob newDPE_glob').find('div').text})
                except Exception:
                    dict_dpe_i.update({'étiquette GPE':''})
                try:
                    dict_dpe_i.update({'émission GPE':soup_i.find('div', class_='DPE_effSerreTxt').find('span').text})
                except Exception:
                    dict_dpe_i.update({'émission GPE':''})
                try:
                    dict_dpe_i.update({'date du bilan DPE':soup_i.find('p', class_='mentions_detailimmo m-0 text-center').text.replace('Fait le :', '').lstrip()})
                except Exception:
                    dict_dpe_i.update({'date du bilan DPE':''})
            except Exception:
                dict_dpe_i = {'étiquette DPE':'', 'consommation énergétique':'', 'étiquette GPE':'', 'émission GPE':'', 'date du bilan DPE':''}
            lst_dpe.append(dict_dpe_i)
        except Exception as ex:
            time.sleep(3)
            print(f"[ERREUR] Annonce {i+1}: {ex}")
        finally:
            driver.quit()
            time.sleep(2)
    print(f"[INFO] Toutes les annonces ont été traitées: {N}/{N} (100%)")

print(f"[INFO] Préparation des données pour la sauvegarde")
# Codes postaux des logements
lst_zip_codes = []
for title in lst_title:
    try:
        ind_lp = title.index('(') # index of left parentheses in title
        ind_rp = title.index(')') # index of right parentheses in title
        lst_zip_codes.append(title[ind_lp+1:ind_rp])
    except Exception:
        lst_zip_codes.append('')

# Villes des logements
lst_cities = []
for title in lst_title:
    try:
        ind_lp = title.index('(') # index of left parentheses in title
        ind_rp = title.index(')') # index of right parentheses in title
        ind_prep_a = title[1:ind_lp].index('à')
        lst_cities.append(title[1:ind_lp][ind_prep_a+1:].lstrip().rstrip())
    except Exception:
        lst_cities.append('')

# Get all the boolean fields
lst_bool_fields_names = []
for d in lst_data_fields:
    for field in list(d.keys()):
        if field not in ['nombre de pièces', 'nombre de chambres', 'chambre']:
            if d[field]==1:
                if field not in lst_bool_fields_names:
                    lst_bool_fields_names.append(field)

# Add boolean keys (fields) with 0 value into dictionnary fields which doesn't contain them
# in order to have the same length for all the dictionary fields
# so that all the dictionary fields share the same keys
for d in lst_data_fields:
    for bool_field in lst_bool_fields_names:
        if bool_field not in list(d.keys()):
            d[bool_field]=0

df_ads = pd.DataFrame.from_dict({**{'Type de bien':lst_type_de_biens},  **{'Ville':lst_cities}, **{'Code postal':lst_zip_codes},
                                 **{'title' : lst_title, 'short_description': lst_short_description, 'long_description': lst_long_description},
                                 **{'lien du logement':links_ads}
                                 })

df_data_fields = pd.DataFrame(lst_data_fields)

df_annonceurs = pd.DataFrame(lst_annonceurs)

df_dpe = pd.DataFrame(lst_dpe)

df = pd.concat([df_ads, df_data_fields, df_dpe, df_annonceurs], axis=1)

# Save the dataframe df into a csv file

# ct stores current time (ct)
ct = datetime.datetime.now()

# Convert the ct result into a string
st = str(ct.year)+'-'+str(ct.month)+'-'+str(ct.day)+'-'+str(ct.hour)+'-'+str(ct.minute)+'-'+str(ct.second)+'-'+str(ct.microsecond)

# Créer le dossier Archives s'il n'existe pas
os.makedirs(archives_path, exist_ok=True)

# Chemin du fichier CSV
csv_path = os.path.join(base_path, f'paru_vendu_ventes_logements_rennes_{st}.csv')
df.to_csv(csv_path)

# Save lists of data
pickle_files = {
    'lst_title_rennes.pkl': lst_title,
    'lst_short_description_rennes.pkl': lst_short_description,
    'lst_long_description_rennes.pkl': lst_long_description,
    'lst_cities_rennes.pkl': lst_cities,
    'lst_zip_codes_rennes.pkl': lst_zip_codes,
    'lst_links_ads_rennes.pkl': links_ads,
    'lst_data_fields_rennes.pkl': lst_data_fields,
    'lst_annonceurs_rennes.pkl': lst_annonceurs,
    'lst_data_dpe_rennes.pkl': lst_dpe
}

# Sauvegarde de tous les fichiers pickle
for filename, data in pickle_files.items():
    file_path = os.path.join(base_path, filename)
    with open(file_path, 'wb') as f:
        pickle.dump(data, f)

print(f"[INFO] Données sauvegardées avec succès dans {base_path}")