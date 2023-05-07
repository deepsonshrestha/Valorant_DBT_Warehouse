with source as (

    select * from {{ ref('stg_match_info')}}

),

player as (

    select * from {{ ref('stg_player_info') }}

),

dim_match as (

    select * from {{ ref('dim_match') }}

),

dim_player as (

    select * from {{ ref('dim_player') }}

),

player_to_match_map as (

    select
        dim_match.match_key,
        dim_player.player_key
    from source
    left join player
        on source.puuid = player.valorant_puuid
    left join dim_match
        on source.match_id = dim_match.match_id
    left join dim_player
        on source.puuid = dim_player.valorant_puuid
    
),

final as (

    select
        match_key,
        player_key
    from player_to_match_map

)

select * from final
