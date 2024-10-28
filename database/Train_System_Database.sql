CREATE DATABASE HD_lab_db;
GO

USE HD_lab_db
GO

CREATE TABLE Maszynista (
	ID INT,
    PESEL CHAR(11) PRIMARY KEY,
	Imie VARCHAR(20) NOT NULL,
	Nazwisko VARCHAR(20) NOT NULL
);

CREATE TABLE Stacja (
    ID_Stacja INT PRIMARY KEY,
	Nazwa VARCHAR(40) NOT NULL,

	Miasto VARCHAR(40) NOT NULL,
	Adres VARCHAR(60) NOT NULL,
	Liczba_Peronow INT NOT NULL,

	Liczba_Torow INT NOT NULL
);

CREATE TABLE Pociag (
    ID_Pociag INT PRIMARY KEY,
	Model VARCHAR(60) NOT NULL, 
	Ostatni_Przeglad DATETIME NOT NULL,
	Czy_Awaria BIT NOT NULL
);

CREATE TABLE Odcinek (
    ID_Odcinek INT PRIMARY KEY,
	ID_Stacji_Start INT,
	ID_Stacji_Koniec INT,
	FOREIGN KEY (ID_Stacji_Start) REFERENCES Stacja(ID_Stacja),
    FOREIGN KEY (ID_Stacji_Koniec) REFERENCES Stacja(ID_Stacja)
);

CREATE TABLE Kurs (
    ID_Kurs CHAR(36) PRIMARY KEY,
	PESEL_Maszynisty CHAR(11) NOT NULL,
	ID_Pociag INT NOT NULL,
	FOREIGN KEY (ID_Pociag) REFERENCES Pociag(ID_Pociag),
    FOREIGN KEY (PESEL_Maszynisty) REFERENCES Maszynista(PESEL)
);

CREATE TABLE Realizacja (
    ID_Realizacji INT PRIMARY KEY,
	ID_Kurs CHAR(36) NOT NULL,
	ID_Odcinek INT,

	Planowany_Odjazd DATETIME,
	Planowany_Przyjazd DATETIME,
	Rzeczywisty_Odjazd DATETIME,
	Rzeczywisty_Przyjazd DATETIME,

    FOREIGN KEY (ID_Odcinek) REFERENCES Odcinek(ID_Odcinek),
    FOREIGN KEY (ID_Kurs) REFERENCES Kurs(ID_Kurs)

);


CREATE TABLE Awaria (
    ID_Awaria INT PRIMARY KEY,
	Typ_Awarii VARCHAR(60) NOT NULL,
	Opis TEXT,
	Powod_Awarii VARCHAR(60) NOT NULL,
	ID_Realizacji INT NOT NULL,
	FOREIGN KEY (ID_Realizacji) REFERENCES Realizacja(ID_Realizacji)

);
