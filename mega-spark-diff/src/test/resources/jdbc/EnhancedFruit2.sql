DROP TABLE IF EXISTS EnhancedFruit2;

CREATE TABLE EnhancedFruit2 (
  Fruit VARCHAR(255),
  Price DOUBLE,
  Ripeness INT,
  Color VARCHAR(255),
  ImportDate DATE,
  ImportTimeStamp TIMESTAMP,
  Status BOOLEAN,
  BValues BLOB,
  CValues CLOB
);

INSERT INTO EnhancedFruit2 VALUES('Mango', 6.11, 12, '', '2017-05-20', '2017-05-20 10:22:10', TRUE, X'01FF', 'clob');
INSERT INTO EnhancedFruit2 VALUES('Papaya', 190534.12, 4, 'I forget', '2017-05-20', '2017-05-20 10:22:10', TRUE, X'01FF', 'clob');
INSERT INTO EnhancedFruit2 VALUES('Strawberry', 5.89, 10, 'Acne', '2017-05-20', '2017-05-20 10:22:10', TRUE, X'01FF', 'clob');
INSERT INTO EnhancedFruit2 VALUES('Plum', 8261.05, 6, 'Purple', '2017-05-20', '2017-05-20 10:22:10', FALSE, X'01FF', 'clob');
INSERT INTO EnhancedFruit2 VALUES('Tomato', 0.9, 0, 'Red', '2017-05-20', '2017-05-20 10:22:10', TRUE, X'01FF', 'clob');
INSERT INTO EnhancedFruit2 VALUES('Watermelon', null, 11, null, '2017-05-21', '2017-05-21 8:10:18', FALSE, X'01FF', 'clob');