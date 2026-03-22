-- Dimension: users
SELECT
    user_id,
    name,
    email,
    city,
    registered_at
FROM {{ ref('stg_users') }}
