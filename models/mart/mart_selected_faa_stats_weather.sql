with 
departures as (
	select origin,flight_date::date as unique_flight_date, count (*) as departure_total from  {{ref('prep_flights')}}
	where origin in ('JFK', 'LAX', 'MIA') and cancelled = 0
	group by origin,unique_flight_date),
arrivals as (
	select dest,flight_date::date as unique_flight_date, count (*) as arrival_total from  {{ref('prep_flights')}}
	where dest in ('JFK', 'LAX', 'MIA') and cancelled = 0
	group by dest,unique_flight_date), 
scheduled as (
	select airport, unique_flight_date, count(*) as schedulled_flights
	from 
		(select origin as airport,flight_date::date as unique_flight_date
		 from {{ref('prep_flights')}}
		 where origin in ('JFK','LAX','MIA')
		 union all
		 select dest as airport, flight_date::date as unique_flight_date
		 from {{ref('prep_flights')}}
		 where dest in ('JFK','LAX','MIA')) as t
	group by airport,unique_flight_date),
canseled as (
	select airport, unique_flight_date, count(*) as canseled_flights
	from 
		(select origin as airport,flight_date::date as unique_flight_date
		  from {{ref('prep_flights')}}
		  where origin in ('JFK','LAX','MIA') and cancelled = 1
		  union all
		  select dest as airport,flight_date::date as unique_flight_date
		  from {{ref('prep_flights')}}
		  where dest in ('JFK','LAX','MIA') and cancelled = 1) as t
		group by airport,unique_flight_date),
diverted as (
	select airport,unique_flight_date, count(*) as diverted_flights
	from
		(select origin as airport,flight_date::date as unique_flight_date
		  from {{ref('prep_flights')}}
		  where origin in ('JFK','LAX','MIA') and diverted = 1
		  union all
		  select dest as airport,flight_date::date as unique_flight_date
		  from {{ref('prep_flights')}}
		  where dest in ('JFK','LAX','MIA') and diverted = 1) as t
	group by airport,unique_flight_date),
unique_airplains as (
	select t.airport, unique_flight_date,avg(unique_airplains)  as avg_unique_airplains
	from 
		(select origin as airport,flight_date::date as unique_flight_date, count( distinct tail_number) as unique_airplains from {{ref('prep_flights')}}
		where origin in ('JFK','LAX','MIA')
		group by airport,unique_flight_date
		union all
		select dest as airport,flight_date::date as unique_flight_date, count( distinct tail_number) as unique_airplains from {{ref('prep_flights')}}
		where dest in ('JFK','LAX','MIA')
		group by airport,unique_flight_date) as t
	group by t.airport,unique_flight_date),
unique_airline as (
	select airport,unique_flight_date, avg(unique_airline)  as avg_unique_airline
	from 
		(select origin as airport,flight_date::date as unique_flight_date, count( distinct airline) as unique_airline from {{ref('prep_flights')}}
		where origin in ('JFK','LAX','MIA')
		group by airport,unique_flight_date
		union all
		select dest as airport,flight_date::date as unique_flight_date, count( distinct airline) as unique_airline from {{ref('prep_flights')}}
		where dest in ('JFK','LAX','MIA')
		group by airport,unique_flight_date) as t
	group  by airport,unique_flight_date)
select 
s.airport,
 name,
 country,
 region, 
 s.unique_flight_date,
 coalesce(d.departure_total, 0) as departure_total,
 coalesce(ar.arrival_total, 0) as arrival_total,
 s.schedulled_flights,
 coalesce(c.canseled_flights, 0) as canseled_flights,
 coalesce(div.diverted_flights, 0) as diverted_flights,
 coalesce(uap.avg_unique_airplains, 0) as avg_unique_airplains,
 coalesce(ual.avg_unique_airline, 0) as avg_unique_airline,
 max_temp_c,
 min_temp_c, 
 precipitation_mm,
 max_snow_mm,
 avg_wind_direction,
 avg_wind_speed_kmh,
 wind_peakgust_kmh
from scheduled s 
left join departures d on d.origin= s.airport and s.unique_flight_date = d.unique_flight_date
left join arrivals ar on ar.dest = s.airport and s.unique_flight_date = ar.unique_flight_date
left join canseled c on c.airport = s.airport and s.unique_flight_date = c.unique_flight_date
left join diverted div on div.airport = s.airport and s.unique_flight_date = div.unique_flight_date
left join unique_airplains uap on uap.airport = s.airport and s.unique_flight_date = uap.unique_flight_date
left join unique_airline ual on ual.airport = s.airport and s.unique_flight_date = ual.unique_flight_date
left join prep_airports on faa = s.airport
left join prep_weather_daily prep on prep.airport_code= s.airport and prep."date" = s.unique_flight_date
order  by s.airport;