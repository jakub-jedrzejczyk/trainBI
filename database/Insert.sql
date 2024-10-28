USE HD_lab_db
GO



EXEC xp_fileexist 'C:\Users\annap\OneDrive\Dokumenty\SQL Server Management Studio\output';


BULK INSERT dbo.Maszynista 
FROM 'C:\Users\annap\Downloads\output\output\driver_data.csv' 
WITH (FIELDTERMINATOR=',', ROWTERMINATOR = '\n', FIRSTROW = 2)

BULK INSERT dbo.Pociag 
FROM 'C:\Users\annap\Downloads\output\output\train_data.csv' 
WITH (FIELDTERMINATOR=',', ROWTERMINATOR = '\n', FIRSTROW = 2)

BULK INSERT dbo.Stacja 
FROM 'C:\Users\annap\Downloads\output\output\stop_data.csv' 
WITH (FIELDTERMINATOR=',', ROWTERMINATOR = '\n', FIRSTROW = 2)



BULK INSERT dbo.Odcinek 
FROM 'C:\Users\annap\Downloads\output\output\section_data.csv' 
WITH (FIELDTERMINATOR=',', ROWTERMINATOR = '\n', FIRSTROW = 2)



BULK INSERT dbo.Kurs 
FROM 'C:\Users\annap\Downloads\output\output\relation_data.csv' 
WITH (FIELDTERMINATOR=',', ROWTERMINATOR = '\n', FIRSTROW = 2)




BULK INSERT dbo.Realizacja 
FROM 'C:\Users\annap\Downloads\output\output\realisation_data.csv' 
WITH (FIELDTERMINATOR=',', ROWTERMINATOR = '\n', FIRSTROW = 2)

BULK INSERT dbo.Awaria 
FROM 'C:\Users\annap\Downloads\output\output\fault_data.csv' 
WITH (FIELDTERMINATOR=',', ROWTERMINATOR = '\n', FIRSTROW = 2)






