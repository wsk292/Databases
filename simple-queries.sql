-- This query lists all non-bitcoin coins that's price have been higher than bitcoin's. Ordered by time.

SELECT symbol, date, price_btc
FROM `molten-position-230523.crypto_market_data.All_12H`
WHERE price_btc > 1 AND symbol != "BITBTC"
ORDER BY time

--This query shows the times where BTC fluctuated the most, ordered by net change in decending order.

SELECT Date, Open, Close, Close-Open as NetChange
FROM `molten-position-230523.crypto_market_data.BTC_1H`
WHERE abs(Close-Open) > 50
ORDER BY abs(NetChange) DESC

--This Query gets a number of attributes from the DB where the hourly net change is less than +/- $5 and the max differential is over $25.
--This shows times when the fluctuation during an hour is far greater than the net change

SELECT Date, Open, High, Low, Close, Close-Open as NetChange, High-Low as MaxDiff
FROM `molten-position-230523.crypto_market_data.ETH_1H`
WHERE abs(Close-Open) < 5 AND High-Low > 25
ORDER BY Date

--This selects the date when the net worth of Litecoin was greater than $50 an hour. Ordered by date

SELECT Date
FROM `molten-position-230523.crypto_market_data.LTC_1H`
WHERE High-Low>50
ORDER BY Date
