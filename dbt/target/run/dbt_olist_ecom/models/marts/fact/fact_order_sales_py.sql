
  
    

    create or replace table `gcp-refresh-2025`.`olist_ecom_all`.`fact_order_sales_py`
      
    
    

    OPTIONS()
    as (
      SELECT
    to_hex(md5(cast(coalesce(cast(tb1.order_id as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(tb1.order_item_id as string), '_dbt_utils_surrogate_key_null_') as string))) AS sales_key,
    to_hex(md5(cast(coalesce(cast(tb1.product_id as string), '_dbt_utils_surrogate_key_null_') as string))) AS product_key,
    to_hex(md5(cast(coalesce(cast(tb1.seller_id as string), '_dbt_utils_surrogate_key_null_') as string))) AS seller_key,
    to_hex(md5(cast(coalesce(cast(tb2.customer_id as string), '_dbt_utils_surrogate_key_null_') as string))) AS customer_key,
    to_hex(md5(cast(coalesce(cast(tb1.order_id as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(tb3.payment_sequential as string), '_dbt_utils_surrogate_key_null_') as string))) AS payment_key,
    tb1.order_id,
    tb1.order_item_id,
    tb1.shipping_limit_date,
    tb2.order_approved_at,
    tb2.order_delivered_carrier_date,
    tb2.order_delivered_customer_date,
    tb2.order_status,
    tb1.price
FROM
    `gcp-refresh-2025`.`olist_ecom_all`.`stg_olist_ecom__order_items` AS tb1
JOIN
    `gcp-refresh-2025`.`olist_ecom_all`.`stg_olist_ecom__orders` AS tb2
    ON tb1.order_id = tb2.order_id
JOIN
    `gcp-refresh-2025`.`olist_ecom_all`.`stg_olist_ecom__order_payments` AS tb3
    ON tb1.order_id = tb3.order_id
    );
  