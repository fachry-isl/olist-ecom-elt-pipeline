SELECT
    to_hex(md5(cast(coalesce(cast(stg_olist_ecom__customers.customer_id as string), '_dbt_utils_surrogate_key_null_') as string))) AS customer_key,
    customer_id,
    zip_code_prefix,
    city,
    state
FROM
    `gcp-refresh-2025`.`olist_ecom_all`.`stg_olist_ecom__customers`