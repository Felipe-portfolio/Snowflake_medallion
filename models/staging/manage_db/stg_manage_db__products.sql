with 

source as (

    select * from {{ source('manage_db', 'products') }}

),

renamed as (

    select
        productid,
        productname,
        price,
        categoryid,
        class,
        modifydate,
        resistant,
        isallergic,
        vitalitydays

    from source

)

select * from renamed
