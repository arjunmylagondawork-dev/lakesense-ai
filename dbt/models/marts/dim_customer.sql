-- Current customer dimension: today's SCD2 version plus lifetime behaviour.
with current_version as (
    select * from {{ ref('stg_customer_history') }} where is_current
),

changes as (
    select customer_id, count(*) - 1 as attribute_changes
    from {{ ref('stg_customer_history') }}
    group by customer_id
),

orders as (
    select
        customer_id,
        count(distinct order_id)                                   as lifetime_orders,
        sum(revenue_amount)                                        as lifetime_revenue,
        min(order_date)                                            as first_order_date,
        max(order_date)                                            as last_order_date,
        avg(case when is_returned then 1.0 else 0.0 end)           as return_rate
    from {{ ref('fct_order_lines') }}
    group by customer_id
)

select
    c.customer_sk,
    c.customer_id,
    c.segment,
    c.country,
    c.email,
    c.valid_from                                                   as current_since,
    cast(coalesce(ch.attribute_changes, 0) as bigint)              as attribute_changes,
    cast(coalesce(o.lifetime_orders, 0) as bigint)                 as lifetime_orders,
    cast(coalesce(o.lifetime_revenue, 0) as decimal(16, 2))        as lifetime_revenue,
    o.first_order_date,
    o.last_order_date,
    cast(round(coalesce(o.return_rate, 0), 4) as double)           as return_rate
from current_version c
left join changes ch using (customer_id)
left join orders o using (customer_id)
