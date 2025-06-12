
    
    

with dbt_test__target as (

  select payment_key as unique_field
  from `gcp-refresh-2025`.`olist_ecom_all`.`dim_payments`
  where payment_key is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


