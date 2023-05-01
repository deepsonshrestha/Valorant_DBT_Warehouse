with source as (

    select * from {{ ref('base_player_account_info') }}

),

player_in_game_info as (

    select * from {{ ref('base_player_in_game_info') }}

),

merged as (

    select
        source.web_user_id,
        coalesce(player_in_game_info.valorant_puuid, 'Unknown') as valorant_puuid,
        source.vendor,
        cast(source.date_joined  as date)as date_joined,
        coalesce(player_in_game_info.in_game_name, 'Unknown') as in_game_name,
        coalesce(player_in_game_info.tagline, 'Unknown') as tagline,
        coalesce(player_in_game_info.region, 'Unknown') as region
    from source
    left join player_in_game_info
        on source.web_user_id = player_in_game_info.web_user_id

),

final as (

    select
        web_user_id,
        valorant_puuid,
        in_game_name,
        tagline,
        region,
        vendor,
        date_joined
    from merged

)

select * from final
