-- Fact: orders (join orders + order_items aggregate + payments)
WITH order_items_agg AS (
    SELECT
        order_id,
        COUNT(*) AS item_count,
        SUM(quantity) AS total_quantity
    FROM {{ ref('stg_order_items') }}
    GROUP BY order_id
),

payment_info AS (
    SELECT
        order_id,
        payment_method,
        status AS payment_status,
        paid_at
    FROM {{ ref('stg_payments') }}
)

SELECT
    o.order_id,
    o.user_id,
    CAST(strftime(o.order_date::DATE, '%Y%m%d') AS INTEGER) AS date_key,
    o.status AS order_status,
    o.total_amount,
    COALESCE(oi.item_count, 0) AS item_count,
    COALESCE(oi.total_quantity, 0) AS total_quantity,
    p.payment_method,
    p.payment_status,
    p.paid_at,
    o.order_date
FROM {{ ref('stg_orders') }} o
LEFT JOIN order_items_agg oi ON o.order_id = oi.order_id
LEFT JOIN payment_info p ON o.order_id = p.order_id
