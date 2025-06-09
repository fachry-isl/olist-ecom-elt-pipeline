

  create or replace view `gcp-refresh-2025`.`olist_ecom_all`.`stg_olist_ecom__order_items`
  OPTIONS()
  as SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value
FROM
    `gcp-refresh-2025`.`olist_ecom_all`.`order_items`;

