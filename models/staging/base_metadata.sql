with source as (

    select * from {{ source('raw', 'metadata') }}

),

updated as (

    select
        match_id,
        puuid,
        map,
        start_at,
        duration,
        mode,
        season_id,
        cluster,
        rounds_played,
        players_data,
        teams,
        _loaded_at
    from source

),

final as (

    select * from updated

)

select * from final

