with source as (

    select * from {{ ref('stg_match_info') }}

),

updated as (

    select
        distinct
        {{ dbt_utils.generate_surrogate_key(['match_id','season_id']) }} as match_key,
        match_id,
        season_id,
        cluster,
        map,
        mode,
        start_date,
        start_at
    from source

),

final as (
    
    select
        match_key,
        match_id,
        season_id,
        cluster,
        map,
        mode,
        start_date,
        start_at
    from updated

)

select * from final
