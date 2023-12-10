DROP TABLE IF EXISTS EnhancedFruit1;

CREATE TABLE EnhancedFruit1 (
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

INSERT INTO EnhancedFruit1 VALUES('Mango', 6.45, 12, 'Yellow', '2017-05-20', '2017-05-20 10:22:10', FALSE, X'01FF', 'clob');
INSERT INTO EnhancedFruit1 VALUES('Papaya', 190534.12, 4, 'I forget', '2017-05-20', '2017-05-20 10:22:10', TRUE, X'01FF', 'clob');
INSERT INTO EnhancedFruit1 VALUES('Kiwi', 8.83, 7, 'Fuzzy-Green', '2017-05-20', '2017-05-20 10:22:10', TRUE, X'01FF', 'clob');
INSERT INTO EnhancedFruit1 VALUES('Watermelon', null, 11, null, '2017-05-20', '2017-05-20 10:22:10', TRUE, X'01FF', 'clob');