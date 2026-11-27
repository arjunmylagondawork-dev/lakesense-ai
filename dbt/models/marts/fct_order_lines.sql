-- Order-line fact, joined to the customer version that was valid WHEN the order
-- was placed (point-in-time join on the SCD2 history), not today's version.
with orders as (
    select * from {{ ref('stg_orders') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

history as (
    select * from {{ ref('stg_customer_history') }}
)

select
    o.order_line_id,
    o.order_id,
    o.order_date,
    o.customer_id,
    h.customer_sk,
    h.segment                                        as customer_segment_at_order,
    o.product_id,
    p.category,
    o.status,
    o.payment_method,
    o.quantity,
    o.line_amount,
    cast(case when o.is_revenue then o.line_amount else 0 end as decimal(14, 2)) as revenue_amount,
    o.is_revenue,
    o.is_returned
from orders o
left join products p
    on o.product_id = p.product_id
left join history h
    on o.customer_id = h.customer_id
   and o.order_ts >= h.valid_from
   and (o.order_ts < h.valid_to or h.valid_to is null)
