/****** Script for SelectTopNRows command from SSMS  ******/
SELECT TOP (1000) [city_name],[state_code],
				  COUNT([timestamp_local]) AS timestamp_count
  FROM [Warehouse].[dbo].[WeatherBit_Hourly_Historical_Hex_TEMP]
  WHERE CAST(timestamp_local as date) = '2019-08-30'
  GROUP BY city_name, state_code
  ORDER BY state_code

SELECT TOP (1000) [id],[city_name],[state_code],[datetime],
				  [precip],[snow],[solar_rad],(([temp] * 9/5) + 32) AS temp_F,
				  [timestamp_local],[timestamp_utc],
				  [weather_description],[timezone]
  FROM [Warehouse].[dbo].[WeatherBit_Hourly_Historical_Hex_TEMP]
  WHERE city_name = 'Bird City' 
  AND CAST(timestamp_local as date) = '2019-08-30'
  GROUP BY city_name, state_code
  ORDER BY city_name DESC

DECLARE @RunDate date = '2019-08-30'
DROP TABLE IF EXISTS #wb_yesterday
SELECT *
INTO #wb_yesterday
FROM Warehouse.dbo.WeatherBit_Hourly_Historical_Hex_TEMP
WHERE CAST(timestamp_utc as date) = @RunDate
--This is for one day (yesterday) of historical data
	SELECT CAST(wb.timestamp_utc as date) as data_timestamp, 
	       MIN((wb.temp * 9/5) + 32) as min_data_value, 
		   MAX((wb.temp * 9/5) + 32) as max_data_value, 
		   counties.county_granularity_id as granularity_item_id, 
		   wb.city_name, wb.state_code,
		   --4707 as index_id, 
		   counties.granularity1
	FROM Warehouse.dbo.geo_uscounties AS counties
			INNER JOIN Warehouse.dbo.geo_worldgridhex hex ON counties.ogr_geometry.STCentroid().STIntersects(hex.[GEOMETRY]) = 1
			INNER JOIN #wb_yesterday wb ON hex.ogc_fid = wb.ogc_fid
			INNER JOIN Staging.dbo.indx_granularity_item gi ON gi.id = counties.county_granularity_id
	WHERE wb.temp <= 10 AND CAST(timestamp_utc as date) = @RunDate
	GROUP BY CAST(wb.timestamp_utc as date), city_name, state_code, counties.county_granularity_id, counties.granularity1
	ORDER BY CAST(wb.timestamp_utc as date), state_code, city_name