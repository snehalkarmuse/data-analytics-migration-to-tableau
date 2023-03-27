
select count(dest) as dest,count(origin) as origin, year  from flights group by year