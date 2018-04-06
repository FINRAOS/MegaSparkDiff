DROP TABLE IF EXISTS appliance;

CREATE TABLE appliance
(
	name varchar(25),
	brand varchar(25),
	type bigint,
	units_sold integer,
	price decimal(10,2),
	date_added date
);

INSERT INTO appliance VALUES
('Some Refrigerator', 'Some Brand 1', 1, 4, 250.00, '2017-06-01'),
('Some Washer', 'Some Brand 2', 2, 10, 500.00, '2017-01-17'),
('Some Dryer', 'Some Brand 3', 3, 5, 500.00, '2017-04-23');

DROP TABLE IF EXISTS appliance_type;

CREATE TABLE appliance_type
(
	type_id bigint,
	type_name varchar(20)
);

INSERT INTO appliance_type VALUES
(1, 'Refrigerator'),
(2, 'Washer'),
(3, 'Dryer');