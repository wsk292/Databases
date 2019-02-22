--This parses the Date column to create Date and Time objects with columns named as such
--This transform applied to all the data tables, simply by interchanging the name of the coin

#standardSQL
SELECT id, Symbol,
  DATE(PARSE_DATETIME('%Y-%m-%d %H-%p', a.date)) AS `date`,
  TIME(PARSE_DATETIME('%Y-%m-%d %H-%p', a.date)) AS time,
  Open, High, Low, Close, Volume_BTC, Volume_USD
FROM `crypto_market_data.BTC_1H`  a


-- This query returns a table of duplicates in the selected data
-- If the returned table is empty, the list has no dupilcates
-- Like the above sql, this was applied to all the tables by simply interchanging the
-- coin name in the selected variables

SELECT
    distinct ID, DATE, Open, High, Low, Close, Volume_BTC, Volume_USD, COUNT(*)
FROM
    `crypto_market_data.BTC_1H`
GROUP BY
    ID, DATE, Open, High, Low, Close, Volume_BTC, Volume_USD
HAVING
    COUNT(*) > 1
