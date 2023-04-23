with source as (

    select * from {{ ref('base_metadata') }}

),

updated as (

    select distinct
        match_id,
        mode,
        season_id,
        cluster,
        map,
        cast(to_timestamp(start_at::integer) as date) as start_date,
        to_timestamp(start_at::integer) as start_at
    from source

),

final as (

    select
        match_id,
        season_id,
        cluster,
        map,
        start_date,
        start_at
    from updated

)

select * from final
