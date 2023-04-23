with source as (

    select * from "DataWarehouse"."dev_staging"."base_metadata"

),

updated_source as (

    select
        match_id,
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
        formatted_teams_data->'red'->>'rounds_won'::varchar as red_rounds_won,
        formatted_teams_data->'red'->>'rounds_lost'::integer as red_rounds_lost,
        case
            when 
                formatted_teams_data->'blue'->>'has_won'::varchar = 'true'
            then 1
            else 0
        end as blue_match_won,
        formatted_teams_data->'blue'->>'rounds_won'::integer as blue_rounds_won,
        formatted_teams_data->'blue'->>'rounds_lost'::integer as blue_rounds_lost
    from updated_source

),

final as (

    select
        match_id,
        red_match_won,
        red_rounds_won,
        red_rounds_lost,
        blue_match_won,
        blue_rounds_won,
        blue_rounds_lost
    from teams_data_flattened

)

select * from final