-- Creates a listing of days in 2019 where the day's average trading volume was greater than that of the average trading
-- volume in 2019

SELECT *
FROM `crypto_market_data_aggregate.BTC_24Hr_2019`
WHERE Volume_BTC >
(SELECT AVG(Volume_BTC)
FROM `crypto_market_data_aggregate.BTC_Monthly_2018`)


-- Creates a listing of BTC values where BTC dropped and both LTC/ETH increased

SELECT *
FROM `crypto_market_data_transformed.BTC_1H_Split`
WHERE Close < Open

and Date IN

(SELECT Date
FROM `crypto_market_data_transformed.ETH_1H_Split`
WHERE Close > Open)

and Date IN

(SELECT Date
FROM `crypto_market_data_transformed.LTC_1H_Split`
WHERE Close > Open)


-- Creates a listing of BTC values where BTC increased while LTC and ETH both rose for each day in 2019

SELECT *
FROM `crypto_market_data_aggregate.BTC_24Hr_2019`
WHERE Close < Open

and Date IN

(SELECT Date
FROM `crypto_market_data_aggregate.ETH_24Hr_2019`
WHERE Close > Open)

and Date IN

(SELECT Date
FROM `crypto_market_data_aggregate.LTC_24Hr_2019`
WHERE Close > Open)

ORDER BY date


-- Creates a listing of times when the trading volume of ETH was greater than that of
-- the average ETH trading volume combined with the average difference between
-- ETH and BTC trading volume
-- Indicative of when interest in ETH spikes

SELECT *
FROM `crypto_market_data_transformed.ETH_1H_Split`
WHERE Volume_USD >

(SELECT AVG(Volume_USD)
FROM `crypto_market_data_transformed.ETH_1H_Split`)

+

(SELECT AVG(a.Volume_USD - b.Volume_USD)
FROM `crypto_market_data_transformed.BTC_1H_Split` as a
RIGHT JOIN `crypto_market_data_transformed.ETH_1H_Split` as b
ON a.date = b.date)


-- Creates a listing of times when the trading volume of LTC was greater than that of
-- the average LTC trading volume combined with the average difference between
-- LTC and ETH trading volume
-- Indicative of when interest in LTC spikes

SELECT *
FROM `crypto_market_data_transformed.LTC_1H_Split`
WHERE Volume_USD >

(SELECT AVG(Volume_USD)
FROM `crypto_market_data_transformed.LTC_1H_Split`)

+

(SELECT AVG(a.Volume_USD - b.Volume_USD)
FROM `crypto_market_data_transformed.ETH_1H_Split` as a
RIGHT JOIN `crypto_market_data_transformed.LTC_1H_Split` as b
ON a.date = b.date)


-- Returns a listing of times where the trading volume of ethereum was higher than the average for the entirety of the data

SELECT *
FROM `crypto_market_data_transformed.ETH_1H_Split`
WHERE (Volume_ETH) >
(SELECT AVG(Volume_ETH)
FROM `crypto_market_data_transformed.ETH_1H_Split`)


--Returns how many times each hour of the day is below the daily close

SELECT time, count(time)
FROM (
SELECT a.id, a.date, a.time, a.Close as Hour_Close, b.Close as Day_Close
FROM crypto_market_data_transformed.BTC_1H_Split as a
JOIN crypto_market_data_aggregate.BTC_24Hr_2019 as b
ON a.Date = b.date
WHERE a.Close < b.Close
)
GROUP BY time


--Returns how many times each hour of the day is above the daily close

SELECT time, count(time)
FROM (
SELECT a.id, a.date, a.time, a.Close as Hour_Close, b.Close as Day_Close
FROM crypto_market_data_transformed.BTC_1H_Split as a
JOIN crypto_market_data_aggregate.BTC_24Hr_2019 as b
ON a.Date = b.date
WHERE a.Close > b.Close
)
GROUP BY time
