-- Lists months during 2018 where BTC's net change from open to close values was negative

SELECT Extract(Month from Date) as Month, Sum(Close - Open) as NetChange
FROM `crypto_market_data_transformed.BTC_1H_Split`
WHERE Date >= '2018-01-01' and Date <= '2019-01-01'
GROUP BY Month
HAVING NetChange < 0
ORDER BY Month


-- Lists months during 2018 where BTC's net change from open to close values was positive

SELECT Extract(Month from Date) as Month, Sum(Close - Open) as NetChange
FROM `crypto_market_data_transformed.BTC_1H_Split`
WHERE Date >= '2018-01-01' and Date <= '2019-01-01'
GROUP BY Month
HAVING NetChange > 0
ORDER BY Month


-- Creates a table of monthly averages for BTC during the year 2018 with high trading volume
-- "High" is above the average trading volume for that respective month

SELECT Extract(Month from Date) as Month, Avg(Open) as Open, Avg(High) as High, Avg(Low) as Low, Avg(Close) as Close,
Avg(Volume_BTC) as Volume_BTC, Avg(Volume_usd) as Volume_USD
FROM `crypto_market_data_transformed.BTC_1H_Split`
WHERE Open > Close and Date >= '2018-01-01' and Date <= '2019-01-01'
GROUP BY Month
HAVING Volume_BTC > 800
ORDER BY Month


-- Creates a table of average 24 hour values for the BTC table

SELECT Date, Avg(Open) as Open, Avg(High) as High, Avg(Low) as Low, Avg(close) as Close,
Avg(Volume_BTC) as Volume_BTC, Avg(Volume_USD) as Volume_USD
FROM `crypto_market_data_transformed.BTC_1H_Split`
WHERE Date >= '2019-01-01'
GROUP BY Date
ORDER BY Date


-- Creates a table of monthly averages for BTC during the year 2018

SELECT Extract(Month from Date) as Month, Avg(Open) as Open, Avg(High) as High, Avg(Low) as Low, Avg(Close) as Close,
Avg(Volume_BTC) as Volume_BTC, Avg(Volume_usd) as Volume_USD
FROM `crypto_market_data_transformed.BTC_1H_Split`
WHERE Open > Close and Date >= '2018-01-01' and Date <= '2019-01-01'
GROUP BY Month
ORDER BY Month


-- Creates a table of average 24 hour values for the ETH table during the year 2019

SELECT Date, Avg(Open) as Open, Avg(High) as High, Avg(Low) as Low, Avg(close) as Close,
Avg(Volume_ETH) as Volume_ETH, Avg(Volume_USD) as Volume_USD
FROM `crypto_market_data_transformed.ETH_1H_Split`
WHERE Date >= '2019-01-01'
GROUP BY Date
ORDER BY Date


-- Creates a table of monthly averages for ETH during the year 2018

SELECT EXTRACT(Month from Date) as Month, Avg(Open) as Open, Avg(High) as High, Avg(Low) as Low, Avg(Close) as Close,
Avg(Volume_ETH) as Volume_ETH, Avg(Volume_usd) as Volume_USD
FROM `crypto_market_data_transformed.ETH_1H_Split`
WHERE Date <= '2019-01-01' and Date >= '2018-01-01'
GROUP BY Month
ORDER BY Month


-- Creates a table of average 24 hour values for the LTC table during the year 2019

SELECT Date, Avg(Open) as Open, Avg(High) as High, Avg(Low) as Low, Avg(close) as Close,
Avg(Volume_LTC) as Volume_LTC, Avg(Volume_USD) as Volume_USD
FROM `crypto_market_data_transformed.LTC_1H_Split`
WHERE Date >= '2019-01-01'
GROUP BY Date
ORDER BY Date


-- Creates a table of monthly averages for ETH during the year 2018

SELECT EXTRACT(Month from Date) as Month, Avg(Open) as Open, Avg(High) as High, Avg(Low) as Low, Avg(Close) as Close,
Avg(Volume_LTC) as Volume_LTC, Avg(Volume_usd) as Volume_USD
FROM `crypto_market_data_transformed.LTC_1H_Split`
WHERE Date <= '2019-01-01' and Date >= '2018-01-01'
GROUP BY Month
ORDER BY Month
