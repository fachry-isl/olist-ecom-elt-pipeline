SELECT
    tb1.product_id,
    tb2.product_category_name_english as category_name
FROM
    `gcp-refresh-2025`.`olist_ecom_all`.`products` tb1
JOIN
    `gcp-refresh-2025`.`olist_ecom_all`.`product_category_name_translation` tb2
ON tb1.product_category_name=tb2.product_category_name