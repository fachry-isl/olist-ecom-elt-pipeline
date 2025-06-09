SELECT
    to_hex(md5(cast(coalesce(cast(stg_olist_ecom__products.product_id as string), '_dbt_utils_surrogate_key_null_') as string))) as product_key,
    product_id,
    category_name
FROM
    `gcp-refresh-2025`.`olist_ecom_all`.`stg_olist_ecom__products`