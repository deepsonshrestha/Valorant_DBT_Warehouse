
  
    

  create  table "DataWarehouse"."dev_staging"."stg_metadata__dbt_tmp"
  as (
    with source as (

    select * from "DataWarehouse"."dev_staging"."base_metadata"

),

updated_source as (

    select
        *,
        replace(replace(lower(teams),'none','null'),'''', '"')::json as formatted_teams_data
    from source

),

teams_data_flattened as (

    select
        *,
        case
            when 
                formatted_teams_data->'red'->>'has_won'::varchar = 'true'
            then 1
            else 0
        end as red_match_won,
        formatted_teams_data->'red'->>'rounds_won' as red_rounds_won,
        formatted_teams_data->'red'->>'rounds_lost' as red_rounds_lost,
        case
            when 
                formatted_teams_data->'blue'->>'has_won'::varchar = 'true'
            then 1
            else 0
        end as blue_match_won,
        formatted_teams_data->'blue'->>'rounds_won' as blue_rounds_won,
        formatted_teams_data->'blue'->>'rounds_lost' as blue_rounds_lost
    from updated_source

)

select * from teams_data_flattened
  );
  