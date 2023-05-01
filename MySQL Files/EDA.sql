use nasdaq_db;

SELECT COUNT(*) FROM stocks;


-- As we can see that there are about 114 lakhs of rows in this table which could take a toll on our performance so to solve this issue 
-- we are going to make a new table which will have monthly data  of all the stocks instead of daily 
-- which in turn will reduce the number of rows significantly and in return will increase performance
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








SELECT * FROM nasdaq;

-- Now taking a look how many companies have directly listed their stocks 
-- (or in simpler terms listed their stocks without releasing ann IPO)
SELECT s.symbol, n.name, MIN(year(date)) AS listed_year, ipo_year, 
CASE 
WHEN MIN(year(date)) = ipo_year THEN CONCAT("Listed through IPO on ", ipo_year)
WHEN ipo_year IS NULL THEN CONCAT("Directly  Listed on ", MIN(year(date)))
ELSE CONCAT("Directly Listed on ", MIN(year(date)), ", And Later on released an IPO on ", ipo_year) END AS Listing 
FROM stocks_monthly s JOIN nasdaq n ON s.symbol = n.symbol
GROUP BY s.symbol;







SELECT sector, industry, COUNT(1) FROM nasdaq
GROUP BY 1, 2
ORDER BY 1, COUNT(1) DESC;




DROP PROCEDURE IF EXISTS stocks_year_month_pivot;
DELIMITER $$
CREATE PROCEDURE stocks_year_month_pivot(IN p_symbol VARCHAR(5))
BEGIN
	SELECT year, 
    SUM(CASE WHEN months = "January" THEN high ELSE NULL END) AS "January",
    SUM(CASE WHEN months = "February" THEN high ELSE NULL END) AS "February",
    SUM(CASE WHEN months = "March" THEN high ELSE NULL END) AS "March",
    SUM(CASE WHEN months = "April" THEN high ELSE NULL END) AS "April",
    SUM(CASE WHEN months = "May" THEN high ELSE NULL END) AS "May",
    SUM(CASE WHEN months = "June" THEN high ELSE NULL END) AS "June",
    SUM(CASE WHEN months = "July" THEN high ELSE NULL END) AS "July",
    SUM(CASE WHEN months = "August" THEN high ELSE NULL END) AS "August",
    SUM(CASE WHEN months = "September" THEN high ELSE NULL END) AS "September",
    SUM(CASE WHEN months = "October" THEN high ELSE NULL END) AS "October",
    SUM(CASE WHEN months = "November" THEN high ELSE NULL END) AS "November",
    SUM(CASE WHEN months = "December" THEN high ELSE NULL END) AS "December"
    FROM stocks_monthly 
    WHERE symbol = p_symbol
    GROUP BY year;
END$$
DELIMITER ;
CALL stocks_year_month_pivot("AACG");




DROP PROCEDURE IF EXISTS highest_earning_month_year;
DELIMITER $$
CREATE PROCEDURE highest_earning_month_year(IN p_symbol VARCHAR(5))
BEGIN
	SELECT year, months, high - low, ROW_NUMBER() OVER(ORDER BY (high - low) DESC, year, num_month) 
    FROM stocks_monthly WHERE symbol = p_symbol;
END$$
DELIMITER ;
CALL highest_earning_month_year("AAPL");




-- FILTERS
SELECT symbol, sector, industry,
CASE 
WHEN market_cap > 1000000000000 THEN "Trillion_Dollar_Company"
WHEN market_cap > 100000000000 THEN "100_Billion_Dollar_Company"
WHEN market_cap > 10000000000 THEN "10_Billion_Dollar_Company"
WHEN market_cap > 1000000000 THEN "Billion_Dollar_Company"
WHEN market_cap > 100000000 THEN "100_Million_Dollar_Company"
WHEN market_cap > 10000000 THEN "10_Million_Dollar_Company" 
WHEN market_cap > 1000000 THEN "Million_Dollar_Company" 
ELSE NUll END AS "Market Cap",
IF(ipo_year IS NULL, "Directly Listed", "Listed Through IPO") AS "Listed Type"
FROM nasdaq
ORDER BY 5, 4, 2 DESC, 3 DESC;