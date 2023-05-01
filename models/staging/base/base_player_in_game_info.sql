with source as (

    select * from {{ source('raw','user_in_game_details') }}

),

final as (

    select
        user_id as web_user_id,
        in_game_name,
        tagline,
        region,
        valorant_puuid,
        _loaded_at
    from source

)

select * from final
