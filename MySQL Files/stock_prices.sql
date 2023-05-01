CREATE DATABASE IF NOT EXISTS nasdaq_db;
use nasdaq_db;

DROP TABLE IF EXISTS stocks;
CREATE TABLE stocks(
idx				INT PRIMARY KEY,
Symbol			VARCHAR(5),
date			DATE,
Open			FLOAT,
High 			FLOAT,
Low				FLOAT,
Close			FLOAT,
FOREIGN KEY (Symbol) REFERENCES nasdaq(symbol));

select COUNT(*) FROM stocks;