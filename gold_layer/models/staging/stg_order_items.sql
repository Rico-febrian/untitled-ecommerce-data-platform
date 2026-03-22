-- Staging: baca order_items dari Silver layer
SELECT
    id AS order_item_id,
    order_id,
    product_id,
    quantity,
    price
FROM {{ source('silver', 'order_items') }}
