-- One row per order line, with the revenue rule applied in one place.
select
    order_line_id,
    order_id,
    customer_id,
    product_id,
    cast(order_date as date)                                         as order_date,
    order_ts,
    status,
    payment_method,
    country,
    quantity,
    unit_price,
    line_amount,
    status in ('placed', 'shipped', 'delivered')                     as is_revenue,
    status = 'returned'                                              as is_returned
from {{ source('lakehouse', 'orders') }}
