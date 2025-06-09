SELECT
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value
FROM
    `gcp-refresh-2025`.`olist_ecom_all`.`order_payments`