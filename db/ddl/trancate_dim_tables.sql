-- Thruncate dimension tables
TRUNCATE TABLE
    {{schema}}.products,
    {{schema}}.product_subcategories,
    {{schema}}.product_categories,
    {{schema}}.customers,
    {{schema}}.stores,
    {{schema}}.employees,
    {{schema}}.payment_methods,
    {{schema}}.shipping_methods
    {{schema}}.currecy_rates
RESTART IDENTITY CASCADE;