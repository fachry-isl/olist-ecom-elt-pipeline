SELECT
    to_hex(md5(cast(coalesce(cast(stg_olist_ecom__sellers.seller_id as string), '_dbt_utils_surrogate_key_null_') as string))) as seller_key,
    seller_id,
    zip_code_prefix,
    city,
    state
FROM `gcp-refresh-2025`.`olist_ecom_all`.`stg_olist_ecom__sellers`