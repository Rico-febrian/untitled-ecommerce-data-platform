-- Fact: order items (grain = 1 row per product per order)
SELECT
    oi.order_item_id,
    oi.order_id,
    o.user_id,
    oi.product_id,
    CAST(strftime(o.order_date::DATE, '%Y%m%d') AS INTEGER) AS date_key,
    o.status AS order_status,
    oi.quantity,
    oi.price AS unit_price,
    oi.quantity * oi.price AS line_total,
    p.payment_method,
    p.payment_status,
    p.paid_at,
    o.order_date
FROM {{ ref('stg_order_items') }} oi
JOIN {{ ref('stg_orders') }} o ON oi.order_id = o.order_id
LEFT JOIN (
    SELECT order_id, payment_method, status AS payment_status, paid_at
    FROM {{ ref('stg_payments') }}
) p ON o.order_id = p.order_id
