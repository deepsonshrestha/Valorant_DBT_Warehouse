
  
    

  create  table "DataWarehouse"."dev_staging"."base_metadata__dbt_tmp"
  as (
    with source as (

    select * from "DataWarehouse"."raw"."metadata"

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
  );
  