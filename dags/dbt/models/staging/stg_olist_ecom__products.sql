SELECT
    tb1.product_id,
    tb2.product_category_name_english as category_name
FROM
    {{ source('olist_ecom_all', 'products') }} tb1
JOIN
    {{ ref('product_category_name_translation')}} tb2
ON tb1.product_category_name=tb2.product_category_name
