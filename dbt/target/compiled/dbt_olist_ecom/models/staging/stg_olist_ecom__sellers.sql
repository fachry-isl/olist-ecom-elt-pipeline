SELECT
    seller_id,
    seller_zip_code_prefix AS zip_code_prefix,
    seller_city AS city,
    seller_state AS state
FROM
    `gcp-refresh-2025`.`olist_ecom_all`.`sellers`