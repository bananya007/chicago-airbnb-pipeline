show pipes in airbnb.raw;

select system$pipe_status('airbnb.raw.pipe_calendar');
select _source_file, count(*) 
from airbnb.raw.listings 
group by 1;
select * from airbnb.audit.load_history order by last_load_time desc;