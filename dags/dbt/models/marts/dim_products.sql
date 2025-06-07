SELECT
    {{dbt_utils.generate_surrogate_key(['stg_olist_ecom__products.product_id'])}} as product_key,
    product_id,
    category_name
FROM
    {{ref('stg_olist_ecom__products')}}