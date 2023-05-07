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
        _data->>'puuid'::varchar as puuid,
        _data->>'team'::varchar as team,
        _data->>'level'::varchar as level,
        _data->>'character'::varchar as character,
        (_data->'session_playtime'->>'minutes')::float as session_playtime_minutes,
        (_data->'behavior'->>'afk_rounds')::float as afk_rounds,
        (_data->'behavior'->'friendly_fire'->>'incoming')::float as friendly_fire_incoming,
        (_data->'behavior'->'friendly_fire'->>'outgoing')::float as friendly_fire_outgoing,
        (_data->'ability_casts'->>'c_cast')::float as ability_cast_c,
        (_data->'ability_casts'->>'q_cast')::float as ability_cast_q,
        (_data->'ability_casts'->>'e_cast')::float as ability_cast_e,
        (_data->'ability_casts'->>'x_cast')::float as ability_cast_x,
        (_data->'stats'->>'score')::float as score,
        (_data->'stats'->>'kills')::float as kills,
        (_data->'stats'->>'deaths')::float as deaths,
        (_data->'stats'->>'assists')::float as assists,
        (_data->'stats'->>'bodyshots')::float as bodyshots,
        (_data->'stats'->>'headshots')::float as headshots,
        (_data->'stats'->>'legshots')::float as legshots,
        (_data->'economy'->'spent'->>'overall')::float as overall_spent,
        (_data->'economy'->'spent'->>'average')::float as average_spent,
        (_data->'economy'->'loadout_value'->>'overall')::float as overall_loadout_value,
        (_data->'economy'->'loadout_value'->>'average')::float as average_loadout_value,
        (_data->>'damage_made')::float as damage_made,
        (_data->>'damage_received')::float as damage_received
    from flattened

),

final as (

    select
        match_id,
        puuid,
        team,
        level,
        character,
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
    from stats

)

select * from final
