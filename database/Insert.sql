USE HD_lab_db
GO


BULK INSERT dbo.Maszynista 
FROM 'path\output\1000000\driver_data.csv' 
WITH (FIELDTERMINATOR=',', ROWTERMINATOR = '\n', FIRSTROW = 2)

BULK INSERT dbo.Pociag 
FROM 'path\output\1000000\train_data.csv' 
WITH (FIELDTERMINATOR=',', ROWTERMINATOR = '\n', FIRSTROW = 2)

BULK INSERT dbo.Stacja 
FROM 'path\output\1000000\stop_data.csv' 
WITH (FIELDTERMINATOR=',', ROWTERMINATOR = '\n', FIRSTROW = 2)



BULK INSERT dbo.Odcinek 
FROM 'path\output\1000000\section_data.csv' 
WITH (FIELDTERMINATOR=',', ROWTERMINATOR = '\n', FIRSTROW = 2)



BULK INSERT dbo.Kurs 
FROM 'path\output\1000000\relation_data.csv' 
WITH (FIELDTERMINATOR=',', ROWTERMINATOR = '\n', FIRSTROW = 2)




BULK INSERT dbo.Realizacja 
FROM 'path\output\1000000\realisation_data.csv' 
WITH (FIELDTERMINATOR=',', ROWTERMINATOR = '\n', FIRSTROW = 2)

BULK INSERT dbo.Awaria 
FROM 'path\output\1000000\fault_data.csv' 
WITH (FIELDTERMINATOR=',', ROWTERMINATOR = '\n', FIRSTROW = 2)






