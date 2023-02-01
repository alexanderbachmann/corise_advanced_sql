Excercise 2:

with food_preferences_tbl as (
select 
    cs.customer_id,
    rt.tag_property as customer_tags,
    row_number() over (partition by cs.customer_id order by rt.tag_property) as rank_order
from vk_data.resources.recipe_tags as rt 
inner join  vk_data.customers.customer_survey as cs
on lower(rt.tag_id) = lower(cs.tag_id) AND is_active = TRUE 
    INNER JOIN
        vk_data.customers.customer_data  cd ON
            cs.customer_id = cd.customer_id
    INNER JOIN
        vk_data.customers.customer_address ca ON
            cd.customer_id = ca.customer_id
    INNER JOIN vk_data.resources.us_cities uc ON
    -- remove whitespace customer_city from customer_address same for state
         (lower(trim(ca.customer_city)) = lower(uc.city_name)
            AND lower(trim(ca.customer_state)
            ) = lower(uc.state_abbr)
        )
),
-- Create food_preference_column 
filtered_preference_tbl as (
select 
    customer_id,
    customer_tags,
    rank_order
from food_preferences_tbl
where rank_order <= 3
),
-- Bring most metrics together 
complete_joined_tbl as (
    select 
        cd.customer_id,
        cd.first_name,
        cd.last_name,
        cd.email,
        fpt.rank_order,
        fpt.customer_tags
    from vk_data.customers.customer_data cd
    inner join filtered_preference_tbl fpt
    on lower(cd.customer_id) = lower(fpt.customer_id)  
),
-- Pivot food preferences
final_completed_tbl as (
select 
        *
from complete_joined_tbl
pivot (
    min(customer_tags)
    for rank_order in (1,2,3)
) as pivot_values (customer_id, first_name, last_name, email, food_preference_1, food_preference_2, food_preference_3)
),
-- Flatten all the recipes 
recipe_tbl as (
    select
    recipe_name,
    tag_lst.value::string as rcp_tags
    from vk_data.chefs.recipe, table(flatten(vk_data.chefs.recipe.tag_list)) AS tag_lst
),
-- Get the suggested recipe customer id will be used for joining purposed in our last output
suggested_recipe_tbl as (
    select 
        cjt.customer_id,
        any_value(rt.recipe_name) as suggested_recipe
    from complete_joined_tbl cjt
    inner join recipe_tbl rt 
    on cjt.customer_tags = rt.rcp_tags AND cjt.rank_order = 1 
    GROUP BY 1
)
-- Final output table
select
    fct.customer_id,
    fct.email,
    fct.first_name,
    fct.food_preference_1,
    fct.food_preference_2,
    fct.food_preference_3,
    srt.suggested_recipe
from final_completed_tbl fct
inner join suggested_recipe_tbl srt
on fct.customer_id = srt.customer_id
order by 2