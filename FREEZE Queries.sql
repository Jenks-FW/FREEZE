SELECT *
FROM Staging.dbo.indx_index_definition
WHERE ticker = 'freeze' -- index_id = 4707


SELECT TOP(100) * 
  FROM Staging.dbo.indx_granularity_item
  WHERE granularity_level_id = 5;


/*
SELECT *
FROM Staging.dbo.indx_index_definition t1
INNER JOIN 
Staging.dbo.elt_definition t2
ON t1.elt_id = t2.id
WHERE t1.ticker = 'freeze'
*/

SELECT TOP(1000) *
FROM [Warehouse].[dbo].[geo_h3gridlevel5]; --WeatherBit_Hourly_Forecast_CONUS


UPDATE t1
SET county_granularity1 = t2.granularity1
FROM Warehouse.dbo.geo_h3gridlevel5 t1
INNER JOIN 
Warehouse.dbo.geo_uscounties t2
ON t1.geom.MakeValid().STCentroid().STIntersects(t2.ogr_geometry)=1


ALTER TABLE Warehouse.dbo.geo_h3gridlevel5
ADD county_granularity1 VARCHAR(100)


SELECT TOP(1000) t1.[id], city_name, state_code, county_granularity1, 
				 temp, app_temp, geojson, wkt, 
				 granularity_level_id, returnedLatitude, 
				 returnedLongitude, timezone, precip, snow,
				 snow_depth, [datetime], ts, timestamp_local, timestamp_utc
FROM Warehouse.dbo.geo_h3gridlevel5 t1
INNER JOIN
Staging.dbo.indx_granularity_item t2
ON t1.county_granularity1 = t2.granularity1
INNER JOIN 
Staging.dbo.indx_granularity_levels t3
ON t2.granularity_level_id = t3.id
INNER JOIN
Warehouse.dbo.WeatherBit_Hourly_Forecast_CONUS t4
ON t1.id = t4.conus_id
WHERE t3.description LIKE '%US Counties%'
AND t4.app_temp < 33
--GROUP BY t1.id, geojson, wkt, pid; 

SELECT * FROM Staging.dbo.indx_granularity_levels
WHERE description LIKE '%COUNTY%'


SELECT DATA_TYPE, COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'WAREHOUSE'
AND TABLE_NAME = 'GEO_H3GRIDLEVEL5' 
AND COLUMN_NAME IN ('ID', 'GEOJSON', 'WKT', 'PID', 'GEOM', 'COUNTRY', 'COUNTY_GRANULARITY1')
	
	
	
DECLARE @RunDate date = '2020-01-30'
DROP TABLE IF EXISTS #wb_yesterday
SELECT *
INTO #wb_yesterday
FROM Warehouse.dbo.WeatherBit_Hourly_Historical_Hex_TEMP
WHERE CAST(timestamp_utc as date) = @RunDate
--This is for one day (yesterday) of historical data
	SELECT CAST(wb.timestamp_utc as date) as data_timestamp, ROUND(COUNT(wb.temp), 0) as data_value, counties.county_granularity_id as granularity_item_id, 4707 as index_id
	FROM Warehouse.dbo.geo_uscounties AS counties
			INNER JOIN Warehouse.dbo.geo_worldgridhex hex ON counties.ogr_geometry.STCentroid().STIntersects(hex.[GEOMETRY]) = 1
			INNER JOIN #wb_yesterday wb ON hex.ogc_fid = wb.ogc_fid
			INNER JOIN Staging.dbo.indx_granularity_item gi ON gi.id = counties.county_granularity_id
	WHERE CAST(timestamp_utc as date) = @RunDate AND wb.temp <= 32
	GROUP BY CAST(wb.timestamp_utc as date), counties.county_granularity_id, counties.granularity1
	ORDER BY CAST(wb.timestamp_utc as date)



-- something isn't right (21 hrs freeze counted for KSCheyenne on 2019-08-30)
DECLARE @RunDate date = '2019-08-30'
DROP TABLE IF EXISTS #wb_yesterday
SELECT *
INTO #wb_yesterday
FROM Warehouse.dbo.WeatherBit_Hourly_Historical_Hex_TEMP
WHERE CAST(timestamp_utc as date) = @RunDate
--This is for one day (yesterday) of historical data
	SELECT CAST(wb.timestamp_utc as date) as data_timestamp, 
	       COUNT(wb.temp) as data_value, 
		   counties.county_granularity_id as granularity_item_id, 
		   wb.city_name, wb.state_code,
		   --4707 as index_id, 
		   counties.granularity1
	FROM Warehouse.dbo.geo_uscounties AS counties
			INNER JOIN Warehouse.dbo.geo_worldgridhex hex ON counties.ogr_geometry.STCentroid().STIntersects(hex.[GEOMETRY]) = 1
			INNER JOIN #wb_yesterday wb ON hex.ogc_fid = wb.ogc_fid
			INNER JOIN Staging.dbo.indx_granularity_item gi ON gi.id = counties.county_granularity_id
	WHERE wb.temp <= 0 AND CAST(timestamp_utc as date) = @RunDate
	GROUP BY CAST(wb.timestamp_utc as date), city_name, state_code, counties.county_granularity_id, counties.granularity1
	ORDER BY CAST(wb.timestamp_utc as date), state_code, city_name



SELECT COUNT(*) FROM Warehouse.dbo.geo_worldgridhex
SELECT TOP(100) * FROM Warehouse.dbo.geo_h3gridlevel5
SELECT TOP(100) * 
FROM Warehouse.dbo.WeatherBit_Hourly_Historical_Hex_TEMP
SELECT TOP(100) * FROM Warehouse.dbo.geo_uscounties

--2020-01-30	2	6857	4707	TNSevier

--Historical WeatherBit table for hourly weather over one day, converted Celsius to Farenheit 
SELECT TOP(100) city_name, state_code, CAST(timestamp_utc as date) as data_timestamp, ROUND(AVG((temp * 9/5) + 32 ), 1) AS Temp
FROM Warehouse.dbo.WeatherBit_Hourly_Historical_Hex_TEMP 
WHERE temp <= 32 AND CAST(timestamp_utc as date) = '2020-02-02'
GROUP BY city_id, ogc_fid, city_name, state_code, CAST(timestamp_utc as date)
ORDER BY data_timestamp, city_name

--WeatherBit Hourly Forecast table (already in Farenheit)
SELECT city_name, state_code, CAST(timestamp_utc as date) as data_timestamp, ROUND(AVG(temp), 1) AS Temp_F
FROM Warehouse.dbo.WeatherBit_Hourly_Forecast_Hex_TEMP
WHERE city_name = 'Zortman'
GROUP BY CAST(timestamp_utc as date), city_name, state_code, ogc_fid
ORDER BY data_timestamp, city_name DESC

--Is the Hourly Historical Table temp column really in C instead of F
SELECT TOP(1000) city_name, state_code, CAST(timestamp_utc as date) as data_timestamp, ROUND(AVG((temp * 9/5) + 32 ), 1) AS Temp_F, ROUND(AVG(temp), 1) AS Temp_C,ROUND(AVG((temp - 32) * 5/9), 1) AS Temp_FFF
FROM Warehouse.dbo.WeatherBit_Hourly_Historical_Hex_TEMP 
WHERE city_name = 'Zortman'
GROUP BY CAST(timestamp_utc as date), city_name, state_code
ORDER BY data_timestamp, city_name DESC


SELECT TOP(100) *--counties.*, hex.county_granularity1, hex.id
FROM Warehouse.dbo.geo_uscounties AS counties
INNER JOIN Warehouse.dbo.geo_h3gridlevel5 hex ON counties.ogr_geometry.STCentroid().STIntersects(hex.[geom]) = 1
GROUP BY county_granularity_id, granularity1, ogr_fid, statefp