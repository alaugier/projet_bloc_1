-- Suppression de la base de données si elle existe déjà
DROP DATABASE IF EXISTS immobilier;

-- Création de la base de données "immobilier"
CREATE DATABASE IF NOT EXISTS immobilier;

-- Utilisation de la base de données "immobilier"
USE immobilier;

-- Message d'information pour indiquer le début de la création de la structure de la base de données
SELECT 'CREATING DATABASE STRUCTURE' as 'INFO';

-- Création de la table `appartement`
CREATE TABLE `appartement` (
    `id_appartement` INTEGER NOT NULL AUTO_INCREMENT UNIQUE,  -- Identifiant unique de l'appartement (clé primaire)
    `ref_annonce` TEXT(65535),  -- Référence de l'annonce (texte libre)
    `prix` INTEGER,  -- Prix de l'appartement (en euros)
    `prix_au_m2` INTEGER,  -- Prix au mètre carré (en euros)
    `surf_hab_m2` INTEGER,  -- Surface habitable en m²
    `nb_piece` INTEGER,  -- Nombre de pièces
    `nb_chambre` INTEGER,  -- Nombre de chambres
    `num_etage` TEXT(65535),  -- Numéro d'étage (texte libre)
    `lab_dpe` TEXT(65535),  -- Label DPE (Diagnostic de Performance Énergétique)
    `conso_elec` INTEGER,  -- Consommation électrique (en kWh/m².an)
    `lab_gpe` TEXT(65535),  -- Label GPE (Gaz à Effet de Serre)
    `emis_gpe` INTEGER,  -- Émissions de GES (en kgCO2/m².an)
    `lien_appartement` TEXT(65535),  -- Lien vers l'annonce en ligne
    `date_maj` DATETIME,  -- Date de mise à jour de l'annonce
    `id_locale` INTEGER,  -- Clé étrangère vers la table `ville` (localisation de l'appartement)
    PRIMARY KEY(`id_appartement`)  -- Définition de la clé primaire
);

-- Création de la table `ville`
CREATE TABLE `ville` (
    `id_locale` INTEGER NOT NULL AUTO_INCREMENT UNIQUE,  -- Identifiant unique de la ville (clé primaire)
    `nom` TEXT(65535),  -- Nom de la ville
    `code_postal` INTEGER,  -- Code postal de la ville
    PRIMARY KEY(`id_locale`)  -- Définition de la clé primaire
);

-- Création de la table `agence`
CREATE TABLE `agence` (
    `id_agence` INTEGER NOT NULL AUTO_INCREMENT UNIQUE,  -- Identifiant unique de l'agence (clé primaire)
    `nom_agence` TEXT(65535),  -- Nom de l'agence immobilière
    `nom_contact` TEXT(65535),  -- Nom du contact dans l'agence
    `ville_annonceur` TEXT(65535),  -- Ville de l'annonceur
    `lien_annonceur` TEXT(65535),  -- Lien vers l'annonceur
    `id_locale` INTEGER,  -- Clé étrangère vers la table `ville` (localisation de l'agence)
    PRIMARY KEY(`id_agence`)  -- Définition de la clé primaire
);

-- Création de la table `appartement_agence` (table de jonction entre `appartement` et `agence`)
CREATE TABLE `appartement_agence` (
    `id_appartement` INTEGER NOT NULL AUTO_INCREMENT UNIQUE,  -- Clé étrangère vers la table `appartement`
    `id_agence` INTEGER NOT NULL,  -- Clé étrangère vers la table `agence`
    PRIMARY KEY(`id_appartement`, `id_agence`)  -- Clé primaire composée
);

-- Création de la table `maison`
CREATE TABLE `maison` (
    `id_maison` INTEGER NOT NULL AUTO_INCREMENT UNIQUE,  -- Identifiant unique de la maison (clé primaire)
    `ref_annonce` TEXT(65535),  -- Référence de l'annonce (texte libre)
    `prix` INTEGER,  -- Prix de la maison (en euros)
    `prix_au_m2` INTEGER,  -- Prix au mètre carré (en euros)
    `surf_hab_m2` INTEGER,  -- Surface habitable en m²
    `surf_terrain_m2` INTEGER,  -- Surface du terrain en m² (spécifique aux maisons)
    `nb_piece` INTEGER,  -- Nombre de pièces
    `nb_chambre` INTEGER,  -- Nombre de chambres
    `lab_dpe` TEXT(65535),  -- Label DPE (Diagnostic de Performance Énergétique)
    `conso_elec` INTEGER,  -- Consommation électrique (en kWh/m².an)
    `lab_gpe` TEXT(65535),  -- Label GPE (Gaz à Effet de Serre)
    `emis_gpe` INTEGER,  -- Émissions de GES (en kgCO2/m².an)
    `lien_maison` TEXT(65535),  -- Lien vers l'annonce en ligne
    `date_maj` DATETIME,  -- Date de mise à jour de l'annonce
    `id_locale` INTEGER NOT NULL,  -- Clé étrangère vers la table `ville` (localisation de la maison)
    PRIMARY KEY(`id_maison`)  -- Définition de la clé primaire
);

-- Création de la table `maison_agence` (table de jonction entre `maison` et `agence`)
CREATE TABLE `maison_agence` (
    `id_maison` INTEGER NOT NULL AUTO_INCREMENT UNIQUE,  -- Clé étrangère vers la table `maison`
    `id_agence` INTEGER NOT NULL,  -- Clé étrangère vers la table `agence`
    PRIMARY KEY(`id_maison`, `id_agence`)  -- Clé primaire composée
);

-- Ajout des contraintes de clés étrangères

-- Clé étrangère de `appartement` vers `ville`
ALTER TABLE `appartement`
ADD FOREIGN KEY(`id_locale`) REFERENCES `ville`(`id_locale`)
ON UPDATE NO ACTION ON DELETE NO ACTION;

-- Clé étrangère de `maison` vers `ville`
ALTER TABLE `maison`
ADD FOREIGN KEY(`id_locale`) REFERENCES `ville`(`id_locale`)
ON UPDATE NO ACTION ON DELETE NO ACTION;

-- Clé étrangère de `agence` vers `ville`
ALTER TABLE `agence`
ADD FOREIGN KEY(`id_locale`) REFERENCES `ville`(`id_locale`)
ON UPDATE NO ACTION ON DELETE NO ACTION;

-- Clé étrangère de `appartement_agence` vers `appartement`
ALTER TABLE `appartement_agence`
ADD FOREIGN KEY(`id_appartement`) REFERENCES `appartement`(`id_appartement`)
ON UPDATE NO ACTION ON DELETE NO ACTION;

-- Clé étrangère de `appartement_agence` vers `agence`
ALTER TABLE `appartement_agence`
ADD FOREIGN KEY(`id_agence`) REFERENCES `agence`(`id_agence`)
ON UPDATE NO ACTION ON DELETE NO ACTION;

-- Clé étrangère de `maison_agence` vers `maison`
ALTER TABLE `maison_agence`
ADD FOREIGN KEY(`id_maison`) REFERENCES `maison`(`id_maison`)
ON UPDATE NO ACTION ON DELETE NO ACTION;

-- Clé étrangère de `maison_agence` vers `agence`
ALTER TABLE `maison_agence`
ADD FOREIGN KEY(`id_agence`) REFERENCES `agence`(`id_agence`)
ON UPDATE NO ACTION ON DELETE NO ACTION;