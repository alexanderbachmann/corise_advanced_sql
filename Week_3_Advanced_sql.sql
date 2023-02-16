/* Pull unique activities table and parsing json values */
with web_events_tbl as (

	select
    	event_id
        , session_id
        ,  event_timestamp
        , parse_json(event_details):recipe_id::string as recipe_id
        , parse_json(event_details):event::string as event_name
    from  
    	vk_data.events.website_activity
    group by 
    	event_id
        , session_id
        , event_timestamp
        , recipe_id
        , event_name
    
)
/* Get the maximum and minimum session length abd get the difference */
, session_length_tbl as (
	
    select
        session_id,
        min(event_timestamp) as min_session,
        max(event_timestamp) as max_session,
		timestampdiff(second, min_session, max_session) as session_difference
    from 
    	web_events_tbl
    group by 
    	session_id
    
)
/* Pick the recipe id by day that had the highest number of impressions  */
, impressions_recipe_tbl as (
	
    select 
    	event_timestamp::date as daily_dt
        , recipe_id           as popular_recipe_id
        , count(*)    		  as total_impressions
    from 
    	web_events_tbl
    where recipe_id is not null 
    group by 
    	daily_dt 
    	, popular_recipe_id
    qualify 
    	row_number() over (partition by daily_dt order by total_impressions desc) = 1
    	
)
/*Count the number of event_name search - theres no recipe_id when event_name is search */
, search_tbl as (
	select
    	session_id
        , count_if(event_name = 'search') as cnt_search
    from 
    	web_events_tbl
    group by 
    	session_id
)
/* Final Transformations to get the final output */
, main_query as (

    select 
    	event_timestamp::date 							   		   as aggregated_daily_dt
        , count(web_events_tbl.session_id)   			   		   as unique_session_id
        , ROUND(avg(session_length_tbl.session_difference)) 	   as avg_session_length
        , ROUND(avg(search_tbl.cnt_search))		  			   	   as avg_search
        , max(impressions_recipe_tbl.popular_recipe_id)    		   as top_recipe_id
    from 
    	web_events_tbl
    inner join session_length_tbl
    on web_events_tbl.session_id = session_length_tbl.session_id
    inner join impressions_recipe_tbl
    on session_length_tbl.min_session::date = impressions_recipe_tbl.daily_dt
    inner join search_tbl
    on web_events_tbl.session_id = search_tbl.session_id
    group by aggregated_daily_dt
)
select 
	*
from main_query