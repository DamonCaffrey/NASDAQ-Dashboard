use nasdaq_db;

show tables;

SELECT * FROM nasdaq;

SELECT * FROM stocks;

-- As we can see that there are about 114 lakhs of rows in this table which could take a toll on our 
-- performance so to solve this issue, we are going to make a new table which will have monthly data  
-- of all the stocks instead of daily which in turn will reduce the number of rows significantly and 
-- in return will increase performance

DROP TABLE IF EXISTS stocks_monthly;
CREATE TABLE IF NOT EXISTS stocks_monthly AS 
(WITH cte AS(
SELECT symbol, YEAR(date) as year, MONTH(date) as month, open, close, high, low,
RANK() OVER(PARTITION BY symbol, YEAR(date), MONTH(date) ORDER BY date ASC) AS date_asc,
RANK() OVER(PARTITION BY symbol, YEAR(date), MONTH(date) ORDER BY date DESC) AS date_desc
FROM stocks)

SELECT symbol, year, month AS num_month, CASE
WHEN month = 1 THEN "January"
WHEN month = 2 THEN "February"
WHEN month = 3 THEN "March"
WHEN month = 4 THEN "April"
WHEN month = 5 THEN "May"
WHEN month = 6 THEN "June"
WHEN month = 7 THEN "July"
WHEN month = 8 THEN "August"
WHEN month = 9 THEN "September"
WHEN month = 10 THEN "October"
WHEN month = 11 THEN "November"
WHEN month = 12 THEN "December" END AS Months, 
CONVERT(CONCAT(year,"-",month,"-01"), DATE) AS date, MAX(high) AS high, MIN(low) AS low,
SUM(CASE WHEN date_asc = 1 THEN open ELSE NULL END) AS open,
SUM(CASE WHEN date_desc = 1 THEN close ELSE NULL END) AS close 
FROM cte WHERE date_asc = 1 OR date_desc = 1
GROUP BY symbol, year, month);

SELECT * FROM stocks_monthly;


SELECT Country, Sector, Industry, market_cap, company_size, Symbol FROM nasdaq
ORDER BY 1, 2, 3, 4;



-- EDA

SELECT COUNT(*), COUNT(symbol), COUNT(DISTINCT symbol), COUNT(industry), COUNT(DISTINCT industry),
COUNT(market_cap), COUNT(DISTINCT market_cap), COUNT(country), COUNT(DISTINCT country), 
COUNT(ipo_year), COUNT(DISTINCT ipo_year), COUNT(sector), COUNT(DISTINCT sector), COUNT(industry), 
COUNT(DISTINCT industry), COUNT(company_size), COUNT(DISTINCT company_size) FROM nasdaq;



SELECT DISTINCT sector, COUNT(*) OVER(PARTITION BY sector) AS count_sec,
industry, COUNT(*) OVER(PARTITION BY sector, industry) AS count_ind FROM nasdaq
ORDER BY sector, count_ind;



SELECT 
CASE 
WHEN market_cap > 1000000000000 THEN "Trillion_Dollar_Company"
WHEN market_cap > 100000000000 THEN "100_Billion_Dollar_Company"
WHEN market_cap > 10000000000 THEN "10_Billion_Dollar_Company"
WHEN market_cap > 1000000000 THEN "Billion_Dollar_Company"
WHEN market_cap > 100000000 THEN "100_Million_Dollar_Company"
WHEN market_cap > 10000000 THEN "10_Million_Dollar_Company" 
WHEN market_cap > 1000000 THEN "Million_Dollar_Company" 
ELSE "Less Than a Million" END AS Market_Cap_,
COUNT(*) AS total_company, MAX(company_size), MIN(company_size) FROM nasdaq
GROUP BY 1 ORDER BY market_cap;



SELECT country, COUNT(*) AS total_company,
SUM(CASE WHEN market_cap > 1000000000000 THEN 1 ELSE 0 END) AS "Trillion Dollar",
SUM(CASE WHEN market_cap BETWEEN 1000000000 AND 1000000000000 THEN 1 ELSE 0 END) AS "Billion Dollar",
SUM(CASE WHEN market_cap BETWEEN 1000000 AND 1000000000 THEN 1 ELSE 0 END) AS "Million Dollar",
SUM(CASE WHEN market_cap BETWEEN 1000 AND 1000000 THEN 1 ELSE 0 END) AS "Thousand Dollar" FROM nasdaq
GROUP BY country;



SELECT country, COUNT(*) AS total_company,
SUM(CASE WHEN company_size > 100000 THEN 1 ELSE 0 END) AS "More than Lakh",
SUM(CASE WHEN company_size BETWEEN 10000 AND 100000 THEN 1 ELSE 0 END) AS "10 Thousand - Lakh",
SUM(CASE WHEN company_size BETWEEN 1000 AND 10000 THEN 1 ELSE 0 END) AS "Thousand - 10 Thousand",
SUM(CASE WHEN company_size BETWEEN 100 AND 1000 THEN 1 ELSE 0 END) AS "Hundred - Thousand",
SUM(CASE WHEN company_size BETWEEN 10 AND 100 THEN 1 ELSE 0 END) AS "Ten - Hundred",
SUM(CASE WHEN company_size < 10 THEN 1 ELSE 0 END) AS "Less than 10" FROM nasdaq
GROUP BY country;



DROP PROCEDURE IF EXISTS country_details;
DELIMITER $$
CREATE PROCEDURE country_details(IN cntry VARCHAR(20))
BEGIN
	SELECT DISTINCT sector, COUNT(*) OVER(PARTITION BY sector) AS sector_Count, 
	industry, COUNT(1) OVER(PARTITION BY sector, industry) AS industry_Count,
	SUM(CASE WHEN market_cap > 1000000000000 THEN 1 ELSE 0 END) OVER(PARTITION BY sector, industry) AS "Trillion Dollar +",
	SUM(CASE WHEN market_cap BETWEEN 1000000000 AND 1000000000000 THEN 1 ELSE 0 END) OVER(PARTITION BY sector, industry) AS "Billion Dollar +",
	SUM(CASE WHEN market_cap BETWEEN 1000000 AND 1000000000 THEN 1 ELSE 0 END) OVER(PARTITION BY sector, industry) AS "Million Dollar +",
	SUM(CASE WHEN market_cap < 1000000 THEN 1 ELSE 0 END) OVER(PARTITION BY sector, industry) AS "Million Dollar -"
	FROM nasdaq WHERE country = cntry;
END $$
DELIMITER ;

CALL country_details("China");



DROP PROCEDURE IF EXISTS country_stock_prices;
DELIMITER $$
CREATE PROCEDURE country_stock_prices(IN cntry VARCHAR(20))
BEGIN
	SELECT DISTINCT country, n.symbol, name, year, 
	ROUND(MAX(high) OVER(PARTITION BY name, year), 2) AS high, 
	ROUND(MIN(low) OVER(PARTITION BY name, year), 2) AS low,
	ROUND(FIRST_VALUE(open) OVER(PARTITION BY name, year), 2) AS open,
	ROUND(LAST_VALUE(close) OVER(PARTITION BY name, year), 2) AS close
	FROM nasdaq n JOIN stocks_monthly s ON n.symbol = s.symbol
	WHERE country = cntry
	ORDER BY 1, 2, 4;
END $$
DELIMITER ;

CALL country_stock_prices("Ireland");



DROP VIEW IF EXISTS stocks_year_summary;
CREATE VIEW stocks_year_summary AS
WITH cte1 AS(
SELECT symbol,
FIRST_VALUE(year) OVER(PARTITION BY symbol) AS first,
CASE WHEN high = MAX(high) OVER(PARTITION BY symbol) THEN year ELSE NULL END AS high_year,
CASE WHEN low = MIN(low) OVER(PARTITION BY symbol) THEN year ELSE NULL END AS low_year,
LAST_VALUE(year) OVER(PARTITION BY symbol) AS last
FROM stocks_monthly),

cte2 AS(
SELECT symbol, first, MAX(high_year) AS high, MIN(low_year) AS low, last 
FROM cte1 GROUP BY symbol),

cte3 AS(
SELECT symbol, 
FIRST_VALUE(open) OVER(PARTITION BY symbol) AS first,
CASE WHEN high = MAX(high) OVER(PARTITION BY symbol) THEN ROUND(high, 1) ELSE NULL END AS high,
CASE WHEN low = MIN(low) OVER(PARTITION BY symbol) THEN ROUND(low, 1) ELSE NULL END AS low,
LAST_VALUE(open) OVER(PARTITION BY symbol) AS last
FROM stocks_monthly),

cte4 AS(
SELECT symbol, ROUND(first, 2) AS first, ROUND(MAX(high), 2) AS high, 
ROUND(MIN(low), 2) AS low, ROUND(last, 2) AS last
FROM cte3 GROUP BY symbol)

SELECT * FROM cte2
UNION ALL
SELECT * FROM cte4
ORDER BY symbol;

SELECT * FROM stocks_year_summary;



DROP PROCEDURE IF EXISTS stocks_pivot;
DELIMITER $$
CREATE PROCEDURE stocks_pivot(IN stock VARCHAR(5))
BEGIN
	SELECT year, 
    ROUND(SUM(CASE WHEN num_month = 1 THEN open ELSE NULL END), 2) AS January,
    ROUND(SUM(CASE WHEN num_month = 2 THEN open ELSE NULL END), 2) AS February,
    ROUND(SUM(CASE WHEN num_month = 3 THEN open ELSE NULL END), 2) AS March,
    ROUND(SUM(CASE WHEN num_month = 4 THEN open ELSE NULL END), 2) AS April,
    ROUND(SUM(CASE WHEN num_month = 5 THEN open ELSE NULL END), 2) AS May,
    ROUND(SUM(CASE WHEN num_month = 6 THEN open ELSE NULL END), 2) AS June,
    ROUND(SUM(CASE WHEN num_month = 7 THEN open ELSE NULL END), 2) AS July,
    ROUND(SUM(CASE WHEN num_month = 8 THEN open ELSE NULL END), 2) AS August,
    ROUND(SUM(CASE WHEN num_month = 9 THEN open ELSE NULL END), 2) AS September,
    ROUND(SUM(CASE WHEN num_month = 10 THEN open ELSE NULL END), 2) AS October,
    ROUND(SUM(CASE WHEN num_month = 11 THEN open ELSE NULL END), 2) AS November,
    ROUND(SUM(CASE WHEN num_month = 12 THEN open ELSE NULL END), 2) AS December
    FROM stocks_monthly
    WHERE symbol = stock
    GROUP BY year;
END $$
DELIMITER ;

CALL stocks_pivot("AACG");



WITH cte1 AS (
SELECT DISTINCT sector, industry, market_cap,
CASE 
WHEN market_cap > 1000000000000 THEN "Trillion_Dollar_Company"
WHEN market_cap > 100000000000 THEN "100_Billion_Dollar_Company"
WHEN market_cap > 10000000000 THEN "10_Billion_Dollar_Company"
WHEN market_cap > 1000000000 THEN "Billion_Dollar_Company"
WHEN market_cap > 100000000 THEN "100_Million_Dollar_Company"
WHEN market_cap > 10000000 THEN "10_Million_Dollar_Company" 
WHEN market_cap > 1000000 THEN "Million_Dollar_Company" 
ELSE "Less Than a Million" END AS Market_Cap_,
company_size, 
CASE
WHEN company_size > 1000000 THEN "Million +"
WHEN company_size > 100000 THEN "Lakh +"
WHEN company_size > 10000 THEN "10 Thousand +"
WHEN company_size > 1000 THEN "Thousand +"
WHEN company_size > 100 THEN "Hundred +"
ELSE "Less Than a Hundred" END AS company_size_ FROM nasdaq
),

cte2 AS(
SELECT DISTINCT sector, industry, company_size_, market_cap_,
COUNT(*) OVER(PARTITION BY sector, industry, company_size_, market_cap_) AS Number_of_Companies,
MAX(market_cap) OVER(PARTITION BY sector, industry, company_size_, market_cap_) AS MAX,
MIN(market_cap) OVER(PARTITION BY sector, industry, company_size_, market_cap_) AS MIN FROM cte1
ORDER BY 1, 2, 4)

SELECT * FROM cte2;