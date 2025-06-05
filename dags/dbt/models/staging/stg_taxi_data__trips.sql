SELECT
    VendorID as vendor_id,
    payment_type,
    fare_amount,
    tip_amount,
    total_amount
FROM
    {{ source('trips_data_all', 'external_table') }}