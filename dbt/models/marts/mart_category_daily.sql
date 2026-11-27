-- Daily revenue by product category and customer segment (at time of order).
select
    order_date,
    category,
    customer_segment_at_order                                   as customer_segment,
    -- orders that contain this category: do not sum across categories (an order can span several)
    cast(count(distinct order_id) as bigint)                    as orders_with_category,
    cast(sum(case when is_revenue then quantity else 0 end) as bigint) as units,
    cast(sum(revenue_amount) as decimal(16, 2))                 as revenue,
    cast(sum(case when is_returned then 1 else 0 end) as bigint) as returned_lines
from {{ ref('fct_order_lines') }}
group by order_date, category, customer_segment_at_order
