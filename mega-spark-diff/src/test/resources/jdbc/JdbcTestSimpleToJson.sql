DROP TABLE IF EXISTS JdbcTestSimpleToJson;

CREATE TABLE JdbcTestSimpleToJson (
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

INSERT INTO JdbcTestSimpleToJson VALUES('Mango', 6.45, 12, 'Yellow', '2017-05-20', '2017-05-20 10:22:10', FALSE, X'01FF', 'clob');
INSERT INTO JdbcTestSimpleToJson VALUES('Papaya', 190534.12, 4, '["I forget","I do not remember"]', '2017-05-20', '2017-05-20 10:22:10', TRUE, X'01FF', 'clob');
INSERT INTO JdbcTestSimpleToJson VALUES('Kiwi', 8.83, 7, '{"inside":"Fuzzy-Green","outside":"Brown"}', '2017-05-20', '2017-05-20 10:22:10', TRUE, X'01FF', 'clob');
INSERT INTO JdbcTestSimpleToJson VALUES('Watermelon', null, 11, null, '2017-05-20', '2017-05-20 10:22:10', TRUE, X'01FF', 'clob');