-- Staging: baca clickstream dari Silver layer
SELECT
    event_id,
    user_id,
    event_type,
    product_id,
    timestamp AS event_timestamp,
    device,
    session_id
FROM {{ source('silver', 'clickstream') }}
