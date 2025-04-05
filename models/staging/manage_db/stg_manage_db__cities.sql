with 

source as (

    select * from {{ source('manage_db', 'cities') }}

),

renamed as (

    select
        cityid,
        cityname,
        zipcode,
        countryid

    from source

)

select * from renamed
