SELECT
    {{dbt_utils.generate_surrogate_key(['stg_olist_ecom__sellers.seller_id'])}} as seller_key,
    seller_id,
    zip_code_prefix,
    city,
    state
FROM {{ref('stg_olist_ecom__sellers')}}