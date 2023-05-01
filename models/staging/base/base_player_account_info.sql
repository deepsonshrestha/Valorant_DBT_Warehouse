with source as (

    select * from {{ source('raw','user_social_details') }}

),

final as (

    select
        user_id as web_user_id,
        vendor,
        date_joined,
        _data,
        _loaded_at
    from source

)

select * from final
