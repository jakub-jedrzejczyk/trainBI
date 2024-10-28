USE HD_lab_db
GO



EXEC xp_fileexist 'path\output';


BULK INSERT dbo.Maszynista 
FROM 'path\driver_data.csv' 
WITH (FIELDTERMINATOR=',', ROWTERMINATOR = '\n', FIRSTROW = 2)

BULK INSERT dbo.Pociag 
FROM 'path\train_data.csv' 
WITH (FIELDTERMINATOR=',', ROWTERMINATOR = '\n', FIRSTROW = 2)

BULK INSERT dbo.Stacja 
FROM 'path\stop_data.csv' 
WITH (FIELDTERMINATOR=',', ROWTERMINATOR = '\n', FIRSTROW = 2)



BULK INSERT dbo.Odcinek 
FROM 'path\section_data.csv' 
WITH (FIELDTERMINATOR=',', ROWTERMINATOR = '\n', FIRSTROW = 2)



BULK INSERT dbo.Kurs 
FROM 'path\relation_data.csv' 
WITH (FIELDTERMINATOR=',', ROWTERMINATOR = '\n', FIRSTROW = 2)




BULK INSERT dbo.Realizacja 
FROM 'path\realisation_data.csv' 
WITH (FIELDTERMINATOR=',', ROWTERMINATOR = '\n', FIRSTROW = 2)

BULK INSERT dbo.Awaria 
FROM 'path\fault_data.csv' 
WITH (FIELDTERMINATOR=',', ROWTERMINATOR = '\n', FIRSTROW = 2)






