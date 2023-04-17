with source as (

    select * from "DataWarehouse"."raw"."metadata"

)

select * from source limit 5