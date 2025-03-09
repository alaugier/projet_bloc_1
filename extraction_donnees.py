import os
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
from tqdm import tqdm
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

chrome_options = webdriver.ChromeOptions()

service = Service('/mnt/c/Users/Utilisateur/Projet_Bloc_1/chromedriver.exe')

ville = 'rennes'
nb_page = 1

links_pages = []
def find_links_pages():
    """Build a list of link found pages. Return the number of pages"""
    n=0
    elems = driver.find_elements(By.XPATH, "//a[@href]") 
    for elem in tqdm(elems):
        link = elem.get_attribute("href")
        if not link in links_pages:
            if link.startswith(f'https://www.paruvendu.fr/immobilier/vente/{ville}/?rechpv=1&tt=1&tbApp=1&tbDup=1&tbChb=1&tbLof=1&tbAtl=1&tbPla=1&tbMai=1&tbVil=1&tbCha=1&tbPro=1&tbHot=1&tbMou=1&tbFer=1&at=1&nbp0=99&pa=FR&lol=30&ray=50&codeINSEE=35XX0,'):
                # print(link)
                links_pages.append(link)
                n+=1
    return n

links_ads = []
def find_links_ads():
    """Build a list of found links of results. Return the number of found links of results."""
    n_i=0
    elems = driver.find_elements(By.XPATH, "//a[@href]") 
    for elem in tqdm(elems):
        link = elem.get_attribute("href")
        if not link in links_ads:
            if link.startswith('https://www.paruvendu.fr/immobilier/vente/'):
                # print(link)
                links_ads.append(link)
                n_i+=1
    return n_i

time.sleep(5)
driver = webdriver.Chrome(service=service, options=chrome_options)
driver.execute_script("document.body.style.zoom='100%'")
driver.get(f'https://www.paruvendu.fr/immobilier/vente/{ville}/?rechpv=1&tt=1&tbApp=1&tbDup=1&tbChb=1&tbLof=1&tbAtl=1&tbPla=1&tbMai=1&tbVil=1&tbCha=1&tbPro=1&tbHot=1&tbMou=1&tbFer=1&at=1&nbp0=99&pa=FR&lol=30&ray=50&codeINSEE=35XX0,')

n = find_links_pages()
while n>0:
    time.sleep(5)
    print('Nombre de pages trouvées :', n)
    for i in range(n):
        print(i+1,'/', n)
        time.sleep(5)
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.execute_script("document.body.style.zoom='100%'")
        driver.get(links_pages[i+nb_page-1])
        page_source_i = driver.page_source
        n_i = find_links_ads()
        print("nombre de résultats trouvés :", n_i)
        time.sleep(3)
        driver.close()
    time.sleep(5)
    nb_page+=n
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.execute_script("document.body.style.zoom='100%'")
    driver.get(f'https://www.paruvendu.fr/immobilier/vente/{ville}/?rechpv=1&tt=1&tbApp=1&tbDup=1&tbChb=1&tbLof=1&tbAtl=1&tbPla=1&tbMai=1&tbVil=1&tbCha=1&tbPro=1&tbHot=1&tbMou=1&tbFer=1&at=1&nbp0=99&pa=FR&lol=30&ray=50&codeINSEE=35XX0,&p={nb_page}')
    n = find_links_pages()
driver.quit()

# Save the raw flattened_listing_ads list into a file

# ct stores current time (ct)
ct = datetime.datetime.now()

# Convert the ct result into a string
st = str(ct.year)+'-'+str(ct.month)+'-'+str(ct.day)+'-'+str(ct.hour)+'-'+str(ct.minute)+'-'+str(ct.second)+'-'+str(ct.microsecond)

paru_vendu_listing_ads_path_filename = '/mnt/c/Users/Utilisateur/Projet_Bloc_1/Archives/paru_vendu_links_ads_rennes'+'_'+st+'.txt'

# Write file
with open(paru_vendu_listing_ads_path_filename, 'w+', encoding="utf-8") as f:
    
    for item in links_ads:
        # Write elements of list
        f.write('%s;\n' %item) # We put a semicolon (;) to separate the elements of this list 
                               # (it is assumed that the elements don't contain a semicolon. 
                               # If not, replace semicolon by another character)
    
    print("File written successfully")

# Close the file
f.close()

# Get the date of latest updates from archives
l = len('paru_vendu_links_ads_rennes_')
lst_datetime = []
for dirpath, dirnames, filenames in os.walk('/mnt/c/Users/Utilisateur/Projet_Bloc_1/Archives'):
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
    
latest_recorded_time = max(lst_datetime)
print(latest_recorded_time)

# Recover the latest recorded datas which contains links of lodgements and put them into a list
# Don't forget to run the cell above whose output is the latest recorded time

# Date of latest recording without zero padding in string format
latest_recorded_time_to_string = latest_recorded_time.strftime('%Y-%m-%d-%H-%M-%S-%f').replace("-0", "-")
print(latest_recorded_time_to_string)

latest_paru_vendu_ads_links_path_filename = '/mnt/c/Users/Utilisateur/Projet_Bloc_1/Archives/paru_vendu_links_ads_rennes'+'_'+latest_recorded_time_to_string+'.txt'
print(latest_paru_vendu_ads_links_path_filename)

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

lst_temp = []
for i in range(len(links_ads)):
    if '?' in links_ads[i]:
        lst_temp.append(links_ads[i])
set_ads_links = set(links_ads).difference(set(lst_temp))
links_ads = list(set_ads_links)

def find_last_index(s, char):
    return max([i for i, letter in enumerate(s) if letter == char])
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

n = 50
k = 1
lst_title = []
lst_short_description = []
lst_long_description = []
lst_data_fields = []
lst_annonceurs = []
lst_dpe =[]

# Extraction des données
while k<=N//n:
    time.sleep(10)
    for i in tqdm(range((k-1)*n, k*n)):
        try:
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
            print(ex)
        finally:
            driver.quit()
            time.sleep(2)
    k+=1


m = (k-1)*n

# Extraction des donn{\'e}es residuelles
for i in tqdm(range(m,N)):
    try:
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
        print(ex)
    finally:
        driver.quit()
        time.sleep(2)


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

df.to_csv('/mnt/c/Users/Utilisateur/Projet_Bloc_1/paru_vendu_ventes_logements_rennes_'+st+'.csv')

# Save lists of data
with open('/mnt/c/Users/Utilisateur/Projet_Bloc_1/lst_title_rennes.pkl', 'wb') as f:
    pickle.dump(lst_title, f)

with open('/mnt/c/Users/Utilisateur/Projet_Bloc_1/lst_short_description_rennes.pkl', 'wb') as f:
    pickle.dump(lst_short_description, f)

with open('/mnt/c/Users/Utilisateur/Projet_Bloc_1/lst_long_description_rennes.pkl', 'wb') as f:
    pickle.dump(lst_long_description, f)

with open('/mnt/c/Users/Utilisateur/Projet_Bloc_1/lst_short_description_rennes.pkl', 'wb') as f:
    pickle.dump(lst_short_description, f)

with open('/mnt/c/Users/Utilisateur/Projet_Bloc_1/lst_cities_rennes.pkl', 'wb') as f:
    pickle.dump(lst_cities, f)

with open('/mnt/c/Users/Utilisateur/Projet_Bloc_1/lst_zip_codes_rennes.pkl', 'wb') as f:
    pickle.dump(lst_zip_codes, f)

with open('/mnt/c/Users/Utilisateur/Projet_Bloc_1/lst_links_ads_rennes.pkl', 'wb') as f:
    pickle.dump(links_ads, f)

with open('/mnt/c/Users/Utilisateur/Projet_Bloc_1/lst_data_fields_rennes.pkl', 'wb') as f:
    pickle.dump(lst_data_fields, f)

with open('/mnt/c/Users/Utilisateur/Projet_Bloc_1/lst_annonceurs_rennes.pkl', 'wb') as f:
    pickle.dump(lst_annonceurs, f)

with open('/mnt/c/Users/Utilisateur/Projet_Bloc_1/lst_data_dpe_rennes.pkl', 'wb') as f:
    pickle.dump(lst_dpe, f)