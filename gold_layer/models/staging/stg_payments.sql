-- Staging: baca payments dari Silver layer
SELECT
    id AS payment_id,
    order_id,
    payment_method,
    amount,
    status,
    paid_at
FROM {{ source('silver', 'payments') }}
