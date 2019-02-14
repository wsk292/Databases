SELECT T1.ID as ID, T1.Date as Date, (T1.Open - T1.Close) as BTC_Net, (T2.Open - T2.Close) as LTC_Net, (T3.Open - T3.Close) as ETH_Net
FROM `crypto_market_data.BTC_1H` T1 
LEFT JOIN `crypto_market_data.LTC_1H` T2
ON T1.ID = T2.ID 
LEFT JOIN `crypto_market_data.ETH_1H` T3
ON T1.ID = T3.ID 
WHERE (T1.Open - T1.Close) < 0 AND (T2.Open - T2.Close) > 0 AND (T3.Open - T3.Close) >0
ORDER BY ID limit 1000

-- This query outer joins the LTC and ETH table to the BTC table where BTC has a -net change/hr while LTC and ETH both have a +net change during that same hour


SELECT T1.ID as ID, T1.Date as Date, T1.Volume_LTC, T2.Volume_ETH, T1.Volume_USD as LTC_Volume_USD, T2.Volume_USD as ETH_Volume_USD
FROM `crypto_market_data.LTC_1H` T1 
JOIN `crypto_market_data.ETH_1H` T2
ON T1.ID = T2.ID
ORDER BY ID limit 1000

--This query joins the LTC and ETH tables on ID and shows the volume_USD and coin volume for each coin during during the same hour


SELECT T1.ID as ID, T1.Date as Date, (T1.Open - T1.Close) as BTC_Net, (T2.Open - T2.Close) as ETH_Net
FROM `crypto_market_data.BTC_1H` T1 
LEFT JOIN `crypto_market_data.ETH_1H` T2
ON T1.ID = T2.ID AND (T1.Open - T1.Close) < 0 AND (T2.Open - T2.Close) > 0
ORDER BY ID limit 1000

-- This query outer joins the ETH table to the BTC table where BTC has a -net change/hr while ETH has a +net change during that same hour


SELECT T1.ID as ID, T1.Date as Date, T1.Volume_BTC, T2.Volume_ETH, T1.Volume_USD as BTC_Volume_USD, T2.Volume_USD as ETH_Volume_USD
FROM `crypto_market_data.BTC_1H` T1 
JOIN `crypto_market_data.ETH_1H` T2
ON T1.ID = T2.ID
ORDER BY ID limit 1000

--This query joins the BTC and ETH tables on ID and shows the volume_USD and coin volume for each coin during during the same hour


select T1.ID as T1ID, T2.ID as T2ID, T1.open as T1open, T1.close as T1close, T2.open as T2open, T2.close as T2close
from `crypto_market_data.ETH_1H`  T1
right join `crypto_market_data.LTC_1H` T2
on T1.ID = T2.ID
and (T2.open > T2.close)
and (T2.open - T2.close) > (2*((T1.open - T1.close)))

-- This query right joins the ETH data with the LTC data and returns a table in which both cryptos were 
-- increasing in value, but the growth rate of LTC was more than double that of ETH


SELECT T1.ID as ID, T1.Date as Date, (T1.Open - T1.Close) as LTC_Net, (T2.Open - T2.Close) as ETH_Net
FROM `crypto_market_data.LTC_1H` T1 
LEFT JOIN `crypto_market_data.ETH_1H` T2
ON T1.ID = T2.ID AND (T1.Open - T1.Close) < 0 AND (T2.Open - T2.Close) > 0
ORDER BY ID limit 1000

-- This query outer joins the ETH table to the LTC table where LTC has a -net change/hr while ETH has a +net change during that same hour