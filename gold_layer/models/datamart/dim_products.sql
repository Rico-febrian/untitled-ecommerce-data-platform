-- Dimension: products
SELECT
    product_id,
    name,
    category,
    price,
    stock
FROM {{ ref('stg_products') }}
