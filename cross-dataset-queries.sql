-- This query shows all the days in 2017 where Facebook stock rose while all 3 cryptos dropped
SELECT Facebook.Date as Date, Facebook.Range as Amazon_Range, BTC.Range as BTC_Range, ETH.Range as ETH_Range, LTC.Range as LTC_Range
FROM econ_data_range.Facebook as Facebook
JOIN `molten-position-230523.crypto_market_data_24H.BTC_24H_2017_Range` as BTC
ON Facebook.Date = BTC.Date
JOIN `molten-position-230523.crypto_market_data_24H.ETH_24H_2017_Range` as ETH
ON Facebook.Date = ETH.Date
JOIN `molten-position-230523.crypto_market_data_24H.LTC_24H_2017_Range` as LTC
ON Facebook.Date = LTC.Date
WHERE Facebook.Range > 0 and BTC.Range < 0 and ETH.Range < 0 and LTC.Range < 0
ORDER BY Facebook.Date

-- This query shows all the days in 2017 where Amazon stock rose while all 3 cryptos dropped
SELECT Amazon.Date as Date, Amazon.Range as Amazon_Range, BTC.Range as BTC_Range, ETH.Range as ETH_Range, LTC.Range as LTC_Range
FROM econ_data_range.Amazon as Amazon
JOIN `molten-position-230523.crypto_market_data_apache.crypto_market_data_24H.BTC_24H_2017_Range` as BTC
ON Amazon.Date = BTC.Date
JOIN `molten-position-230523.crypto_market_data_apache.crypto_market_data_24H.ETH_24H_2017_Range` as ETH
ON Amazon.Date = ETH.Date
JOIN `molten-position-230523.crypto_market_data_apache.crypto_market_data_24H.LTC_24H_2017_Range` as LTC
ON Amazon.Date = LTC.Date
WHERE Amazon.Range > 0 and BTC.Range < 0 and ETH.Range < 0 and LTC.Range < 0
ORDER BY Amazon.Date

--This query shows all the days in 2017 where Facebook stock rose while all 3 cryptos dropped
SELECT Apple.Date as Date, Apple.Range as Amazon_Range, BTC.Range as BTC_Range, ETH.Range as ETH_Range, LTC.Range as LTC_Range
FROM econ_data_range.Apple as Apple
JOIN `molten-position-230523.crypto_market_data_apache.crypto_market_data_24H.BTC_24H_2017_Range` as BTC
ON Apple.Date = BTC.Date
JOIN `molten-position-230523.crypto_market_data_apache.crypto_market_data_24H.ETH_24H_2017_Range` as ETH
ON Apple.Date = ETH.Date
JOIN `molten-position-230523.crypto_market_data_apache.crypto_market_data_24H.LTC_24H_2017_Range` as LTC
ON Apple.Date = LTC.Date
WHERE Apple.Range > 0 and BTC.Range < 0 and ETH.Range < 0 and LTC.Range < 0
ORDER BY Apple.Date

-- Creates a table of increasing LTC and Apple Stock values, matched on ID
SELECT BR.id, BR.date, BR.LTC_range as LTCRange, AR.Range as FaceBookRange
FROM `molten-position-230523.crypto_market_data_apache.LTC_All_Range` as BR
INNER JOIN `molten-position-230523.econ_data.Facebook_range` as AR ON BR.id = AR.id
WHERE AR.Range > 0 and BR.LTC_range > 0

-- Creates a table of increasing BTC and Amazon Stock values, matched on ID
SELECT BR.id, BR.date, BR.BTC_range as BTCRange, AR.Range as AmazonRange
FROM `molten-position-230523.crypto_market_data_apache.BTC_All_Range` as BR
INNER JOIN `molten-position-230523.econ_data.Amazon_range` as AR ON BR.id = AR.id
WHERE AR.Range > 0 and BR.BTC_range > 0

-- Creates a table of decreasing BTC values and increasing FB stock values
SELECT BR.id, BR.date, BR.btc_range as BitCoinRange, AR.Range as AmazonRange
FROM `crypto_market_data_apache.BTC_All_Range` as BR
INNER JOIN `econ_data.Apple_range` as AR ON BR.id = AR.id
WHERE AR.Range < 0 and BR.btc_range > 0
