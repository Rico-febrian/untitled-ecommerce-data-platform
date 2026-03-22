-- Staging: baca users dari Silver layer
SELECT
    id AS user_id,
    name,
    email,
    city,
    registered_at
FROM {{ source('silver', 'users') }}
