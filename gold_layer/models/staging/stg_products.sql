-- Staging: baca products dari Silver layer
SELECT
    id AS product_id,
    name,
    category,
    price,
    stock
FROM {{ source('silver', 'products') }}
