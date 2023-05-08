with source as (

    select * from {{ ref('stg_match_detailed') }}

),

dim_match as (

    select * from {{ ref('dim_match') }}

),

dim_player as (

    select * from {{ ref('dim_player') }}

),

teams_info as (

    select * from {{ ref('stg_teams_info') }}

),

joined_keys as (

    select
        case
            when source.match_id is null 
                then {{ dbt_utils.generate_surrogate_key(['null']) }}
            when dim_match.match_key is null
                then {{ dbt_utils.generate_surrogate_key(['\'No Match\''])}}
            else dim_match.match_key
        end as match_key,
        case
            when source.puuid is null 
                then {{ dbt_utils.generate_surrogate_key(['null']) }}
            when dim_player.player_key is null
                then {{ dbt_utils.generate_surrogate_key(['\'No Match\''])}}
            else dim_player.player_key
        end as player_key,
        source.level as level_key,
        source.character as character_key,
        source.team as team_key,
        case
            when teams_info.red_match_won = '1'
                then 'red'
            when teams_info.blue_match_won = '1'
                then 'blue'
            else 'draw'
        end as winning_team_key,
        source.session_playtime_minutes,
        source.afk_rounds,
        source.friendly_fire_incoming,
        source.friendly_fire_outgoing,
        source.ability_cast_c,
        source.ability_cast_e,
        source.ability_cast_q,
        source.ability_cast_x,
        source.score,
        source.kills,
        source.deaths,
        source.assists,
        source.bodyshots,
        source.headshots,
        source.legshots,
        source.overall_spent,
        source.average_spent,
        source.overall_loadout_value,
        source.average_loadout_value,
        source.damage_made,
        source.damage_received
    from source
    left join dim_match
        on source.match_id = dim_match.match_id
    left join dim_player
        on source.puuid = dim_player.valorant_puuid
    left join teams_info
        on source.match_id = teams_info.match_id

),

final as (

    select
        match_key,
        player_key,
        team_key,
        winning_team_key,
        level_key,
        character_key,
        session_playtime_minutes,
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
    from joined_keys

)

select * from final
