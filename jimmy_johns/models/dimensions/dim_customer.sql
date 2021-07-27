{{
    config(
        materialized='incremental',
        transient='false',
        incremental_strategy='merge',
        unique_key='CUSTOMER_NUMBER'
    )
}}


select   dbt_jj.nextval                            as CUSTOMER_SK           
       , stg.customer_id                           as CUSTOMER_NUMBER     
       , stg.CUSTOMER_FIRST_NAME                   as CUSTOMER_FIRST_NAME   
       , stg.CUSTOMER_LAST_NAME                    as CUSTOMER_LAST_NAME   
       , stg.CUSTOMER_FULL_NAME                    as CUSTOMER_FULL_NAME   
       , stg.CUSTOMER_EMAIL_ADDRESS                as CUSTOMER_EMAIL_ADDRESS
       , stg.CUSTOMER_PHONE                        as CUSTOMER_PHONE       
       , '1/1/1900'                                as EFFECTIVE_DATE_START  
       , '12/31/2999'                              as EFFECTIVE_DATE_END    
       , 1                                         as IS_CURRENT            
       , 1                                         as ROW_VERSION           
       , current_timestamp                         as ROW_CREATED_DATE      
       , current_timestamp                         as ROW_LAST_UPDATED      
       , 'jj_source'                               as ROW_SOURCE_SYSTEM     


FROM {{ref('stg_customer')}} stg


{% if is_incremental() %}

WHERE stg.ROW_LAST_UPDATED > (select max(ROW_LAST_UPDATED) from {{ this }})

{% endif %}