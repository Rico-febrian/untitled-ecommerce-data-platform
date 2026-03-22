-- Staging: baca orders dari Silver layer
SELECT
    id AS order_id,
    user_id,
    order_date,
    status,
    total_amount
FROM {{ source('silver', 'orders') }}
