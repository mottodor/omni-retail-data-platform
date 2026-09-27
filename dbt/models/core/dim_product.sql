{{ config(materialized='table') }}

-- Grain: one row per product. SCD Type 1 (current state; natural key).
-- Upstream: int_products, int_categories (self-referencing hierarchy).
select
    p.product_id,
    p.sku,
    p.name as product_name,
    p.brand,
    p.unit_price,
    p.unit_cost,
    p.is_active,
    p.category_id,
    c.name as category_name,
    c.parent_category_id,
    pc.name as parent_category_name
from {{ ref('int_products') }} as p
join {{ ref('int_categories') }} as c
    on c.category_id = p.category_id
left join {{ ref('int_categories') }} as pc
    on pc.category_id = c.parent_category_id
