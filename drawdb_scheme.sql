DROP DATABASE IF EXISTS immobilier;
CREATE DATABASE IF NOT EXISTS immobilier;
USE immobilier;

SELECT 'CREATING DATABASE STRUCTURE' as 'INFO';

CREATE TABLE `appartement` (
	`id_appartement` INTEGER NOT NULL AUTO_INCREMENT UNIQUE,
	`ref_annonce` TEXT(65535),
	`prix` INTEGER,
	`prix_au_m2` INTEGER,
	`surf_hab_m2` INTEGER,
	`nb_piece` INTEGER,
	`nb_chambre` INTEGER,
	`num_etage` TEXT(65535),
	`lab_dpe` TEXT(65535),
	`conso_elec` INTEGER,
	`lab_gpe` TEXT(65535),
	`emis_gpe` INTEGER,
	`lien_appartement` TEXT(65535),
	`date_maj` DATETIME,
	`id_locale` INTEGER,
	PRIMARY KEY(`id_appartement`)
);


CREATE TABLE `ville` (
	`id_locale` INTEGER NOT NULL AUTO_INCREMENT UNIQUE,
	`nom` TEXT(65535),
	`code_postal` INTEGER,
	PRIMARY KEY(`id_locale`)
);


CREATE TABLE `agence` (
	`id_agence` INTEGER NOT NULL AUTO_INCREMENT UNIQUE,
	`nom_agence` TEXT(65535),
	`nom_contact` TEXT(65535),
	`ville_annonceur` TEXT(65535),
	`lien_annonceur` TEXT(65535),
	`id_locale` INTEGER,
	PRIMARY KEY(`id_agence`)
);


CREATE TABLE `appartement_agence` (
	`id_appartement` INTEGER NOT NULL AUTO_INCREMENT UNIQUE,
	`id_agence` INTEGER NOT NULL,
	PRIMARY KEY(`id_appartement`, `id_agence`)
);


CREATE TABLE `maison` (
	`id_maison` INTEGER NOT NULL AUTO_INCREMENT UNIQUE,
	`ref_annonce` TEXT(65535),
	`prix` INTEGER,
	`prix_au_m2` INTEGER,
	`surf_hab_m2` INTEGER,
	`surf_terrain_m2` INTEGER,
	`nb_piece` INTEGER,
	`nb_chambre` INTEGER,
	`lab_dpe` TEXT(65535),
	`conso_elec` INTEGER,
	`lab_gpe` TEXT(65535),
	`emis_gpe` INTEGER,
	`lien_maison` TEXT(65535),
	`date_maj` DATETIME,
	`id_locale` INTEGER NOT NULL,
	PRIMARY KEY(`id_maison`)
);


CREATE TABLE `maison_agence` (
	`id_maison` INTEGER NOT NULL AUTO_INCREMENT UNIQUE,
	`id_agence` INTEGER NOT NULL,
	PRIMARY KEY(`id_maison`, `id_agence`)
);


ALTER TABLE `appartement`
ADD FOREIGN KEY(`id_locale`) REFERENCES `ville`(`id_locale`)
ON UPDATE NO ACTION ON DELETE NO ACTION;
ALTER TABLE `maison`
ADD FOREIGN KEY(`id_locale`) REFERENCES `ville`(`id_locale`)
ON UPDATE NO ACTION ON DELETE NO ACTION;
ALTER TABLE `agence`
ADD FOREIGN KEY(`id_locale`) REFERENCES `ville`(`id_locale`)
ON UPDATE NO ACTION ON DELETE NO ACTION;
ALTER TABLE `appartement_agence`
ADD FOREIGN KEY(`id_appartement`) REFERENCES `appartement`(`id_appartement`)
ON UPDATE NO ACTION ON DELETE NO ACTION;
ALTER TABLE `appartement_agence`
ADD FOREIGN KEY(`id_agence`) REFERENCES `agence`(`id_agence`)
ON UPDATE NO ACTION ON DELETE NO ACTION;
ALTER TABLE `maison_agence`
ADD FOREIGN KEY(`id_maison`) REFERENCES `maison`(`id_maison`)
ON UPDATE NO ACTION ON DELETE NO ACTION;
ALTER TABLE `maison_agence`
ADD FOREIGN KEY(`id_agence`) REFERENCES `agence`(`id_agence`)
ON UPDATE NO ACTION ON DELETE NO ACTION;