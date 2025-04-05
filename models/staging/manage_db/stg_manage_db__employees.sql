with 

source as (

    select * from {{ source('manage_db', 'employees') }}

),

renamed as (

    select
        employeid,
        firstname,
        middleinitial,
        lastname,
        birthdate,
        gender,
        cityid,
        hiredate

    from source

)

select * from renamed
