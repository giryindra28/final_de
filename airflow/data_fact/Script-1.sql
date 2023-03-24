# built fact table store

select 

# buat fact store table
select ds.business_id , ds.name as store, ds.feature_restaurant , ds.tema_business, dl.address, dl.upper as city, dl.postal_code, dl.state, dd.day_hours,dd.date, dr.stars,dr.review_count
from dim_store ds 
join dim_location dl 
on ds.business_id = dl.business_id 
join dim_date dd 
on ds.business_id = dd.business_id 
join dim_review dr 
on ds.business_id = dr.business_id 
limit 5000;

# fact tabel suhu
select ds.normal_min ,ds.normal_max ,dp.precipitation_normal ,ds.min ,ds.max, dp.precipitation 
from dim_suhu ds 
join dim_precipation dp 
on ds.id = dp.id
limit 5000;

