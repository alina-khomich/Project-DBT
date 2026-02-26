with 
departures as (
	select origin, count (*) as departure_total from  {{ref('prep_flights')}}
	where cancelled = 0
	group by origin),
arrivals as (
	select dest, count (*) as arrival_total from  {{ref('prep_flights')}}
	where cancelled = 0
	group by dest), 
scheduled as (
	select airport, count(*) as schedulled_flights
	from 
		(select origin as airport
		 from {{ref('prep_flights')}}
		 union all
		 select dest as airport
		 from {{ref('prep_flights')}}) as t
	group by airport),
canseled as (
	select airport, count(*) as canseled_flights
	from 
		(select origin as airport
		  from {{ref('prep_flights')}}
		  where cancelled = 1
		  union all
		  select dest as airport
		  from {{ref('prep_flights')}}
		  where cancelled = 1) as t
		group by airport),
diverted as (
	select airport, count(*) as diverted_flights
	from
		(select origin as airport
		  from {{ref('prep_flights')}}
		  where  diverted = 1
		  union all
		  select dest as airport
		  from {{ref('prep_flights')}}
		  where  diverted = 1) as t
	group by airport),
unique_airplains as (
	select t.airport, avg(unique_airplains)  as avg_unique_airplains
	from 
		(select origin as airport, count( distinct tail_number) as unique_airplains from {{ref('prep_flights')}}
		group by airport
		union all
		select dest as airport, count( distinct tail_number) as unique_airplains from {{ref('prep_flights')}}
		group by airport) as t
	group by t.airport),
unique_airline as (
	select airport, avg(unique_airline)  as avg_unique_airline
	from 
		(select origin as airport, count( distinct airline) as unique_airline from {{ref('prep_flights')}}
		group by airport
		union all
		select dest as airport, count( distinct airline) as unique_airline from {{ref('prep_flights')}}
		group by airport) as t
	group  by airport)
select 
s.airport,
name,
country,
region, 
departure_total,
arrival_total, 
schedulled_flights,
canseled_flights, 
diverted_flights,
avg_unique_airplains,
avg_unique_airline
from scheduled s 
join departures on origin= airport
join arrivals on dest = airport
join canseled using (airport)
join diverted using (airport)
join unique_airplains using (airport)
join unique_airline using (airport)
join prep_airports on faa = airport
order by s.airport