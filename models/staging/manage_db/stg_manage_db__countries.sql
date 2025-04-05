with 

source as (

    select * from {{ source('manage_db', 'countries') }}

),

renamed as (

    select
        countryid,
        countryname,
        countrycode

    from source

)

select * from renamed
