WITH session_funnel_steps AS (
    SELECT
        ws.website_session_id AS session_id,
        
        -- Use MAX() and CASE to aggregate pageview flags directly
        MAX(CASE WHEN wp.pageview_url = '/products' THEN 1 ELSE 0 END) AS viewed_products,
        MAX(CASE WHEN wp.pageview_url = '/the-original-mr-fuzzy' THEN 1 ELSE 0 END) AS viewed_fuzzy,
        MAX(CASE WHEN wp.pageview_url = '/cart' THEN 1 ELSE 0 END) AS viewed_cart,
        MAX(CASE WHEN wp.pageview_url = '/shipping' THEN 1 ELSE 0 END) AS viewed_shipping,
        MAX(CASE WHEN wp.pageview_url = '/billing' THEN 1 ELSE 0 END) AS viewed_billing,
        MAX(CASE WHEN wp.pageview_url = '/thank-you-for-your-order' THEN 1 ELSE 0 END) AS viewed_thank_you
        
    FROM website_sessions AS ws
    LEFT JOIN website_pageviews AS wp
        ON ws.website_session_id = wp.website_session_id
    WHERE ws.created_at BETWEEN '2012-08-05' AND '2012-09-04'
    AND ws.utm_source = 'gsearch' 
    AND ws.utm_campaign = 'nonbrand'
    
    -- Group by session_id to get one row per session
    GROUP BY ws.website_session_id
)
SELECT
    -- (The final aggregation remains the same)
    COUNT(session_id) AS total_sessions,
    COUNT(CASE WHEN viewed_products = 1 THEN session_id ELSE NULL END) * 1.0 / COUNT(session_id) AS product_clickthrough_rate,
    COUNT(CASE WHEN viewed_fuzzy = 1 THEN session_id ELSE NULL END) * 1.0 / NULLIF(COUNT(CASE WHEN viewed_products = 1 THEN session_id ELSE NULL END), 0) AS fuzzy_rate,
    COUNT(CASE WHEN viewed_cart = 1 THEN session_id ELSE NULL END) * 1.0 / NULLIF(COUNT(CASE WHEN viewed_fuzzy = 1 THEN session_id ELSE NULL END), 0) AS cart_rate,
    COUNT(CASE WHEN viewed_shipping = 1 THEN session_id ELSE NULL END) * 1.0 / NULLIF(COUNT(CASE WHEN viewed_cart = 1 THEN session_id ELSE NULL END), 0) AS shipping_rate,
    COUNT(CASE WHEN viewed_billing = 1 THEN session_id ELSE NULL END) * 1.0 / NULLIF(COUNT(CASE WHEN viewed_shipping = 1 THEN session_id ELSE NULL END), 0) AS billing_rate,
    COUNT(CASE WHEN viewed_thank_you = 1 THEN session_id ELSE NULL END) * 1.0 / NULLIF(COUNT(CASE WHEN viewed_billing = 1 THEN session_id ELSE NULL END), 0) AS purchase_rate
FROM session_funnel_steps;