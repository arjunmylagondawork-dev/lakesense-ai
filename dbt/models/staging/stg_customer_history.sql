select
    customer_sk,
    customer_id,
    segment,
    country,
    email,
    valid_from,
    valid_to,
    is_current
from {{ source('lakehouse', 'customer_history') }}
