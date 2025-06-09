SELECT
    customer_id,
    customer_zip_code_prefix as zip_code_prefix,
    customer_city AS city,
    customer_state AS state
FROM
    `gcp-refresh-2025`.`olist_ecom_all`.`customers`