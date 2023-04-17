
  create view "DataWarehouse"."dev_staging"."test__dbt_tmp" as (
    with source as (

    select * from "DataWarehouse"."raw"."metadata"

)

select * from source limit 5
  );