DROP TABLE IF EXISTS compare_JsonTestMapList;

CREATE TABLE compare_JsonTestMapList (
  key1 VARCHAR(255),
  key2 INT,
  attribute1 VARCHAR(255),
  attribute2 VARCHAR(255),
  attribute3 VARCHAR(255)
);

INSERT INTO compare_JsonTestMapList VALUES('TEST1', 1, 'test number 1', '{"element1":"1"}', '["1"]');
INSERT INTO compare_JsonTestMapList VALUES('TEST2', 1, 'test number 2', '{"element1":"2"}', '["2"]');
INSERT INTO compare_JsonTestMapList VALUES('TEST3', 3, 'test number 3', '{"element1":"3","element2":"4"}', '["3","4"]');