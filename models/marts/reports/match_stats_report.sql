with source as (

    select * from {{ ref('fact_match_detailed') }}

),

dim_match as (

    select * from {{ ref('dim_match') }}

),

dim_player as (

    select * from {{ ref('dim_player') }}

),

joined_data as (

    select
        source.*,
        source.winning_team_key as winning_team,
        source.level_key as player_level,
        source.character_key as player_character,
        dim_match.cluster,
        dim_match.map,
        dim_match.mode,
        case
            when dim_match.mode = 'Deathmatch'
                then 'na'
            else source.team_key
        end as player_team,
        dim_match.start_date, --utc
        dim_player.in_game_name ||' #' || dim_player.tagline as in_game_name
    from source
    left join dim_match
        on source.match_key = dim_match.match_key
    left join dim_player
        on source.player_key = dim_player.player_key
    where in_game_name is not null
    
),

final as (

    select
        start_date,
        in_game_name,
        map,
        mode,
        cluster,
        player_team,
        winning_team,
        player_level,
        player_character,
        afk_rounds,
        friendly_fire_incoming,
        friendly_fire_outgoing,
        ability_cast_c,
        ability_cast_e,
        ability_cast_q,
        ability_cast_x,
        score,
        kills,
        deaths,
        assists,
        bodyshots,
        headshots,
        legshots,
        overall_spent,
        average_spent,
        overall_loadout_value,
        average_loadout_value,
        damage_made,
        damage_received
    from joined_data

)

select * from final
