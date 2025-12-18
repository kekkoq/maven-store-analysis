WITH session_analysis AS (
    -- 1. Combine all necessary session and pageview data into one aggregated view.
    SELECT
        wp_cart.website_session_id,
        CASE
            WHEN wp_cart.created_at < '2013-09-25' THEN 'pre_cross_sell'
            WHEN wp_cart.created_at >= '2013-09-25' THEN 'post_cross_sell'
        END AS period,
        -- Use MAX(CASE WHEN ...) to check if a subsequent pageview was /shipping
        MAX(CASE
            WHEN wp_sub.pageview_url = '/shipping' 
            AND wp_sub.website_pageview_id > wp_cart.website_pageview_id -- Must happen AFTER cart
            THEN 1 
            ELSE 0 
        END) AS reached_shipping,
        
        -- NULL if no order was made
        MAX(od.order_id) AS order_id,
        MAX(od.items_purchased) AS items_purchased,
        MAX(od.price_usd) AS price_usd
        
    FROM website_pageviews AS wp_cart
    -- Left join ALL subsequent pageviews (wp_sub) for funnel analysis
    LEFT JOIN website_pageviews AS wp_sub
        ON wp_sub.website_session_id = wp_cart.website_session_id
    LEFT JOIN orders AS od
        ON od.website_session_id = wp_cart.website_session_id
    WHERE wp_cart.created_at BETWEEN '2013-08-25' AND '2013-10-25'
      AND wp_cart.pageview_url = '/cart'    
    GROUP BY 
        wp_cart.website_session_id,
        wp_cart.created_at -- same as period grouping
)

    period,
    COUNT(website_session_id) AS cart_sessions,
    SUM(reached_shipping) AS shipping_sessions_from_cart,
    SUM(reached_shipping) * 1.0 / COUNT(website_session_id) AS cart_to_shipping_ctr,
    
    COUNT(order_id) AS total_orders,
    SUM(items_purchased) AS units_purchased,
    SUM(items_purchased) * 1.0 / COUNT(order_id) AS products_per_order, -- Calculation is safe due to order_id COUNT
    SUM(price_usd) AS total_revenue,
    AVG(price_usd) AS average_order_value,
    SUM(price_usd) / COUNT(website_session_id) AS revenue_per_cart_session
FROM session_analysis
GROUP BY period
ORDER BY period;