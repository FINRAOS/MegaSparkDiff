DROP TABLE IF EXISTS Persons1;

CREATE TABLE Persons1 (
  PersonID INT,
  LastName VARCHAR(255),
  FirstName VARCHAR(255),
  Address VARCHAR(255),
  City VARCHAR(255)
);

INSERT INTO Persons1 VALUES(1,'Garcia', 'Carlos', 'lives somewhere', 'Rockville');
INSERT INTO Persons1 VALUES(2,'Patel', 'Shraddha', 'lives somewhere', 'Maryland');