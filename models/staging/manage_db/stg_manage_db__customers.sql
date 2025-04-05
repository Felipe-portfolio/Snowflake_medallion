with 

source as (

    select * from {{ source('manage_db', 'customers') }}

),

renamed as (

    select
        customerid,
        firstname,
        middleinitial,
        lastname,
        cityid,
        address

    from source

)

select * from renamed
