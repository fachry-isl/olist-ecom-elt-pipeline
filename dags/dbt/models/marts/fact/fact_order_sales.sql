SELECT
    {{ dbt_utils.generate_surrogate_key(['tb1.order_id', 'tb1.order_item_id']) }} AS sales_key,
    {{ dbt_utils.generate_surrogate_key(['tb1.product_id']) }} AS product_key,
    {{ dbt_utils.generate_surrogate_key(['tb1.seller_id']) }} AS seller_key,
    {{ dbt_utils.generate_surrogate_key(['tb2.customer_id']) }} AS customer_key,
    tb1.order_id,
    tb1.order_item_id,
    tb1.shipping_limit_date,
    tb2.order_approved_at,
    tb2.order_delivered_carrier_date,
    tb2.order_delivered_customer_date,
    tb2.order_status,
    tb1.price,
    tb1.freight_value
FROM
    {{ ref('stg_olist_ecom__order_items') }} AS tb1
JOIN
    {{ ref('stg_olist_ecom__orders') }} AS tb2
    ON tb1.order_id = tb2.order_id
