with 

source as (

    select * from {{ source('manage_db', 'categories') }}

),

renamed as (

    select
        categoryid,
        categoryname

    from source

)

select * from renamed
