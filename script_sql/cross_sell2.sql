SELECT
    t1.primary_product_id,
    COUNT(DISTINCT t1.order_id) AS total_orders,

    -- Use conditional aggregation (COUNT DISTINCT for each cross-sell)
    COUNT(DISTINCT CASE WHEN t2.product_id = 1 THEN t1.order_id END) AS orders_with_cross_sell_1,
    COUNT(DISTINCT CASE WHEN t2.product_id = 2 THEN t1.order_id END) AS orders_with_cross_sell_2,
    COUNT(DISTINCT CASE WHEN t2.product_id = 3 THEN t1.order_id END) AS orders_with_cross_sell_3,
    COUNT(DISTINCT CASE WHEN t2.product_id = 4 THEN t1.order_id END) AS orders_with_cross_sell_4,

    -- Calculate ratio (requires calculating the total again, or using a window function)
    -- This calculation remains the same for correctness:
    CAST(COUNT(DISTINCT CASE WHEN t2.product_id = 1 THEN t1.order_id END) AS NUMERIC) * 1.0 /
    COUNT(DISTINCT t1.order_id) AS orders_with_cross_sell_1_ratio
FROM
    orders AS t1
LEFT JOIN
    order_items AS t2
    ON t1.order_id = t2.order_id
    AND t2.is_primary_item = 0 --  (cross-sells)
WHERE
    t1.created_at >= '2014-12-05'
GROUP BY
    t1.primary_product_id
ORDER BY
    t1.primary_product_id;