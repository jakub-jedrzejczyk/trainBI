USE HD_lab_db
GO

BULK INSERT dbo.Maszynista 
FROM 'C:\Users\annap\Downloads\output\output\driver_data.csv' 
WITH (FIELDTERMINATOR=',', ROWTERMINATOR = '\n', FIRSTROW = 2)

EXEC xp_fileexist 'C:\Users\annap\Downloads\output\output\driver_data.csv';

DROP TABLE Awaria;
DROP TABLE Realizacja;

DROP TABLE Odcinek;


DROP TABLE Stacja;




DROP TABLE Kurs;


DROP TABLE Pociag;
