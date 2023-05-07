with source as (

    select
        distinct
        match_id,
        players_data
    from {{ ref('base_metadata') }}

),

flattened as (

    select
        match_id,
        json_array_elements(replace(replace(lower(players_data),'none','null'),'''', '"')::json -> 'all_players') as _data
    from source

),


stats as (

    select
        match_id,
        _data->'puuid' as puuid,
        _data->'team' as team,
        _data->'level' as level,
        _data->'character' as character,
        _data->'session_playtime'->>'minutes' as session_playtime_minutes,
        _data->'behavior'->>'afk_rounds' as afk_rounds,
        _data->'behavior'->'friendly_fire'->>'incoming' as friendly_fire_incoming,
        _data->'behavior'->'friendly_fire'->>'outgoing' as friendly_fire_outgoing,
        _data->'ability_casts'->>'c_cast' as ability_cast_c,
        _data->'ability_casts'->>'q_cast' as ability_cast_q,
        _data->'ability_casts'->>'e_cast' as ability_cast_e,
        _data->'ability_casts'->>'x_cast' as ability_cast_x,
        _data->'stats'->>'score' as score,
        _data->'stats'->>'kills' as kills,
        _data->'stats'->>'deaths' as deaths,
        _data->'stats'->>'assists' as assists,
        _data->'stats'->>'bodyshots' as bodyshots,
        _data->'stats'->>'headshots' as headshots,
        _data->'stats'->>'legshots' as legshots,
        _data->'economy'->'spent'->>'overall' as overall_spent,
        _data->'economy'->'spent'->>'average' as average_spent,
        _data->'economy'->'loadout_value'->>'overall' as overall_loadout_value,
        _data->'economy'->'loadout_value'->>'average' as average_loadout_value,
        _data->'damage_made' as damage_made,
        _data->'damage_received' as damage_received
    from flattened

)

select * from stats
