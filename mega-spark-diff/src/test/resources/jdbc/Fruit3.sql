DROP TABLE IF EXISTS Fruit3;

CREATE TABLE Fruit3 (
  Fruit VARCHAR(255),
  Price INT,
  Ripeness INT,
  Color VARCHAR(255)
);

INSERT INTO Fruit3 VALUES('Apple', 5, 10, 'Red');
INSERT INTO Fruit3 VALUES('Banana', 4, 8, 'Yellow');
INSERT INTO Fruit3 VALUES('Orange', 2, -9, 'Blue'); --diff
INSERT INTO Fruit3 VALUES('Kiwi', 8, 7, 'Fuzzy-Green');
INSERT INTO Fruit3 VALUES('Watermelon', 3, 11, 'Green');
INSERT INTO Fruit3 VALUES('Mango', 6, 12, 'Yellow');
INSERT INTO Fruit3 VALUES('Papaya', 190534, 4, 'I remember now'); --diff
INSERT INTO Fruit3 VALUES('Strawberry', 5, 10, 'Acne');
INSERT INTO Fruit3 VALUES('Plum', 8261, 6, 'Purple');
INSERT INTO Fruit3 VALUES('Tomato', 0, 0, 'Red');