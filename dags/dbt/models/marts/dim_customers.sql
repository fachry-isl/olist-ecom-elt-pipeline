SELECT
    {{ dbt_utils.generate_surrogate_key(['stg_olist_ecom__customers.customer_id']) }} AS customer_key,
    customer_id,
    zip_code_prefix,
    city,
    state
FROM
    {{ref('stg_olist_ecom__customers')}}