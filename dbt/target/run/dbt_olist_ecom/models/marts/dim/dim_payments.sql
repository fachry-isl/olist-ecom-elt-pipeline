
  
    

    create or replace table `gcp-refresh-2025`.`olist_ecom_all`.`dim_payments`
      
    
    

    OPTIONS()
    as (
      SELECT
    to_hex(md5(cast(coalesce(cast(stg_olist_ecom__order_payments.order_id as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(stg_olist_ecom__order_payments.payment_sequential as string), '_dbt_utils_surrogate_key_null_') as string))) AS payment_key,
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value
FROM
    `gcp-refresh-2025`.`olist_ecom_all`.`stg_olist_ecom__order_payments`
    );
  