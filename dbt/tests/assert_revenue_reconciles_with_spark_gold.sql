-- Reconciliation: daily revenue computed by dbt (SQL, from silver) must equal the
-- gold daily_revenue mart computed by Spark. Any row returned is a mismatch.
with dbt_side as (
    select order_date, sum(revenue) as revenue
    from {{ ref('mart_category_daily') }}
    group by order_date
),

spark_side as (
    select order_date, revenue
    from {{ source('lakehouse', 'daily_revenue') }}
)

select
    coalesce(d.order_date, s.order_date) as order_date,
    d.revenue                            as dbt_revenue,
    s.revenue                            as spark_revenue
from dbt_side d
full outer join spark_side s on d.order_date = s.order_date
where coalesce(d.revenue, 0) <> coalesce(s.revenue, 0)
