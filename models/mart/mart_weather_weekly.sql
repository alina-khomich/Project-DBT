with weather_weekly as (
	select 
		date_trunc('week',"date") as trunc_week,
		round(avg(avg_temp_c),2) as avg_temp_c ,
		round(max(max_temp_c),2) as max_temp_c,
		round(min(min_temp_c),2) as min_temp_c, 
		round(avg(precipitation_mm),2) as precipitation_mm,
		round(max(max_snow_mm),2) as max_snow_mm,
		round(avg(avg_wind_direction),2) as avg_wind_direction,
		round(avg(avg_wind_speed_kmh),2) as avg_wind_speed_kmh,
		round(avg(wind_peakgust_kmh),2) as wind_peakgust_kmh,
		round(avg(avg_pressure_hpa),2) as avg_pressure_hpa,
		round(avg(sun_minutes),2) as sun_minutes
	from prep_weather_daily
	group by trunc_week)
select * from weather_weekly