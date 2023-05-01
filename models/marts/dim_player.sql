with source as (

    select * from {{ ref('stg_player_info') }}

),

key_generate as (

    select
        {{ dbt_utils.generate_surrogate_key(['valorant_puuid', 'web_user_id']) }} as player_key,
        web_user_id,
        valorant_puuid,
        in_game_name,
        tagline,
        region,
        vendor,
        date_joined
    from source
    where valorant_puuid is not null

),

final as (

    select
        player_key,
        web_user_id,
        valorant_puuid,
        in_game_name,
        tagline,
        region,
        vendor,
        date_joined
    from key_generate

)

select * from final
