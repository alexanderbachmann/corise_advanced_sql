Excercise 1:

/*
We have 10,000 potential customers who have signed up with Virtual Kitchen. 
If the customer is able to order from us, then their city/state will be present in our database. 
Create a query in Snowflake that returns all customers that can place an order with Virtual Kitchen.
*/

-- Query 1
-- CTE with distinct city_name and state_abr for mapping purposes
with location_tbl as (
select 
    distinct
        LOWER(city_name) as city_name,
        LOWER(state_abbr) as state_name,
        LAT,
        LONG
 from vk_data.resources.us_cities 
),
-- CTE with the intent to extract suppliers Location combined by joining with the location CTE.
combined_supplier_tbl AS (
select
    si.supplier_id,
    si.supplier_name,
    lt.city_name as supplier_city,
    lt.state_name as supplier_state,
    lt.LAT        as supplier_lat,
    lt.LONG       as supplier_long
from vk_data.suppliers.supplier_info si
left join location_tbl lt
on lower(trim(si.supplier_city)) = lower(trim(lt.city_name)) and lower(trim(si.supplier_state)) = lower(trim(lt.state_name))
),
-- CTE with the intent to extract customers location combined by joining with the location CTE
combined_customer_tbl as (
select
    ca.customer_id,
    lt.city_name,
    lt.state_name,
    lt.LAT as customer_lat,
    lt.LONG as customer_long,
    cd.first_name,
    cd.last_name,
    cd.email
from vk_data.customers.customer_address ca
    left join location_tbl lt
    on lower(trim(ca.customer_city)) = lt.city_name AND lower(trim(ca.customer_state)) = lt.state_name
    inner join vk_data.customers.customer_data cd
    on ca.customer_id = cd.customer_id
),
-- CROSS JOIN supplier and customer table to get all possible combinations since we want all possible customers matched with all suppliers
-- in order to find the closest one to each customer.
combined_final_tbl as (
select 
   *
from combined_supplier_tbl cst
cross join combined_customer_tbl cct
),
-- CTE that calculates distance
distance_tbl as (
select
    customer_id,
    first_name,
    last_name,
    email,
    supplier_id,
    supplier_name,
    st_distance(
        st_makepoint(customer_long, customer_lat),
        st_makepoint(supplier_long, supplier_lat)
    ) / 1609 as distance_km
from combined_final_tbl
),
-- Rank to get closest distance by each customer_id
final_query as (
select 
    customer_id,
    first_name,
    last_name,
    email,
    supplier_id,
    supplier_name,
    distance_km,
    row_number() OVER (PARTITION BY customer_id order by distance_km) as ranked_distance
from distance_tbl
)
-- Final output and filtering for customers that don't have an assigned supplier and ranked 1 (closest supplier)
select 
    customer_id,
    first_name,
    last_name,
    email,
    supplier_id,
    supplier_name,
    distance_km
from final_query
where distance_km IS NOT NULL and ranked_distance = 1
order by last_name, first_name