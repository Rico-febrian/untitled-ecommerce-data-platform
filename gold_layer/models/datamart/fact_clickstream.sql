-- Fact: clickstream events
SELECT
    event_id,
    user_id,
    product_id,
    CAST(strftime(event_timestamp::DATE, '%Y%m%d') AS INTEGER) AS date_key,
    event_type,
    device,
    session_id,
    event_timestamp
FROM {{ ref('stg_clickstream') }}
