SELECT
    {{ dbt_utils.generate_surrogate_key(['stg_olist_ecom__order_payments.order_id', 'stg_olist_ecom__order_payments.payment_sequential']) }} AS payment_key,
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value
FROM
    {{ref('stg_olist_ecom__order_payments')}} 