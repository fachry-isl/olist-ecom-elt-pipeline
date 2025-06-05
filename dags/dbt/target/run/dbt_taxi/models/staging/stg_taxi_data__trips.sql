

  create or replace view `gcp-refresh-2025`.`dbt_taxi`.`stg_taxi_data__trips`
  OPTIONS()
  as SELECT
    VendorID as vendor_id,
    payment_type,
    fare_amount,
    tip_amount,
    total_amount
FROM
    `gcp-refresh-2025`.`trips_data_all`.`external_table`;

