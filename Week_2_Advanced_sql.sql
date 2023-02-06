-- CTE that counts food preference
with food_pref_count as (
    select 
        customer_id
        , count(*) as food_pref_count
    from vk_data.customers.customer_survey
    where is_active = true
    group by 1
)
-- Filter for chicago city
, chicago_tbl as (
    select 
        chic.state_abbr
        , chic.city_name
        , chic.geo_location
    from vk_data.resources.us_cities as chic
    where upper(city_name) = 'CHICAGO' and state_abbr = 'IL'
)
-- Filter for Gary city
, gary_tbl as (
    select 
        gary.state_abbr
        , gary.city_name
        , gary.geo_location
    from vk_data.resources.us_cities as gary
    where city_name = 'GARY' and state_abbr = 'IN'
)
-- Gather all the necessary customer information
, customer_info_tbl as (
    select 
    ca.customer_id
    , first_name || ' ' || last_name as customer_name
    , ca.customer_city
    , ca.customer_state
from vk_data.customers.customer_address    as ca
inner join vk_data.customers.customer_data as c 
    on ca.customer_id = c.customer_id
)
-- Combine customer information with food preferences table
, customer_food_pref_tbl as (
    select 
          cit.customer_name
        , cit.customer_city
        , cit.customer_state
        , fpc.food_pref_count
    from customer_info_tbl as cit
    inner join food_pref_count as fpc
    on cit.customer_id = fpc.customer_id    
)
-- Main table that cross join our customer infor with chicago and gary tables plus us city filters
, main_tbl as (
    select 
          cfp.customer_name
        , cfp.customer_city
        , cfp.customer_state
        , cfp.food_pref_count
        , (st_distance(usc.geo_location, chic.geo_location) / 1609)::int as chicago_distance_miles
        , (st_distance(usc.geo_location, gary.geo_location) / 1609)::int as gary_distance_miles
    from customer_food_pref_tbl as cfp
    left join vk_data.resources.us_cities as usc
    on upper(rtrim(ltrim(cfp.customer_state))) = upper(TRIM(usc.state_abbr))
    and trim(upper(cfp.customer_city)) = trim(upper(usc.city_name))
    cross join chicago_tbl as chic
    cross join gary_tbl as gary
    where 
    ((trim(usc.city_name) ilike any ('%concord%', '%georgetown%', '%ashland%'))
    and cfp.customer_state = 'KY')
    or
    (cfp.customer_state = 'CA' and (trim(usc.city_name) ilike any ('%oakland%', '%pleasant hill%')))
    or
    (cfp.customer_state = 'TX' and (trim(usc.city_name) ilike '%arlington%') or trim(usc.city_name) ilike '%brownsville%')
)
  select * from main_tbl