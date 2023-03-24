# buat dimention table untuk business

# buat dim date_business
select b.business_id,  b.hours as day_hours, c."date" 
from business b 
join checkin c 
on b.business_id  = c.business_id
where b.hours is not null;

# buat dim location
select business_id ,address ,UPPER(city) ,postal_code , state , latitude , longitude 
from  business b  
order by city asc; 

# buat dim store business
select distinct business_id ,name, attributes as feature_restaurant, categories as tema_business
from business b 
where attributes is not null

# buat dim review_business
select distinct  business_id , stars , review_count, is_open 
from business b 
where business_id is not null;

#####################################################

# buat dimention table untuk degreef

select index as ID, normal_min ,min ,normal_max ,max 
from degreef 

select index as ID, precipitation , precipitation_normal 
from vegas b

