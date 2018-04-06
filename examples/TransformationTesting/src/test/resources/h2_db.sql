DROP TABLE IF EXISTS APPLIANCE;

CREATE TABLE APPLIANCE
(
	NAME varchar(50),
	TYPE varchar(20),
	SALES_AMOUNT decimal(10,2),
	PRICE decimal(10,2),
	DATE_ADDED date
);

INSERT INTO APPLIANCE VALUES
('some refrigerator,some brand 1', 'refrigerator', 1000.00, 250.00, '2017-06-01'),
('some washer,some brand 2', 'washer', 5000.00, 500.00, '2017-01-17'),
('some dryer,some brand 3', 'dryer', 2500.00, 500.00, '2017-04-23');