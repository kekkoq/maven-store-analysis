WITH conversion_test_analysis AS (
    SELECT
        ws.website_session_id AS session_id,
        MAX(CASE WHEN wp.pageview_url = '/home' THEN 1 ELSE 0 END) AS viewed_home,
        MAX(CASE WHEN wp.pageview_url = '/lander-1' THEN 1 ELSE 0 END) AS viewed_lander,
        MAX(CASE WHEN wp.pageview_url = '/products' THEN 1 ELSE 0 END) AS viewed_products,
        MAX(CASE WHEN wp.pageview_url = '/the-original-mr-fuzzy' THEN 1 ELSE 0 END) AS viewed_fuzzy,
        MAX(CASE WHEN wp.pageview_url = '/cart' THEN 1 ELSE 0 END) AS viewed_cart,
        MAX(CASE WHEN wp.pageview_url = '/shipping' THEN 1 ELSE 0 END) AS viewed_shipping,
        MAX(CASE WHEN wp.pageview_url = '/billing' THEN 1 ELSE 0 END) AS viewed_billing,
        MAX(CASE WHEN wp.pageview_url = '/thank-you-for-your-order' THEN 1 ELSE 0 END) AS viewed_thank_you
    FROM website_sessions AS ws
    INNER JOIN website_pageviews AS wp
        ON ws.website_session_id = wp.website_session_id
    WHERE 
        ws.created_at < '2012-07-28' -- End date for the experiment
        AND wp.website_pageview_id >= 23504 -- Start pageview ID (can be simplified if '2012-06-19' is the actual start date)
        AND ws.utm_campaign = 'nonbrand'
        AND ws.utm_source = 'gsearch'
    GROUP BY ws.website_session_id 
)
SELECT  
    CASE WHEN viewed_home = 1 THEN 'home'
         WHEN viewed_lander = 1 THEN 'lander-1'
         ELSE 'other' END AS entry_page,
    COUNT(session_id) AS total_sessions,
    ROUND(COUNT(CASE WHEN viewed_products = 1 THEN session_id ELSE NULL END) * 1.0 / COUNT(session_id), 4) AS product_clickthrough_rate,
    ROUND(COUNT(CASE WHEN viewed_fuzzy = 1 THEN session_id ELSE NULL END) * 1.0 / NULLIF(COUNT(CASE WHEN viewed_products = 1 THEN session_id ELSE NULL END), 0), 4) AS fuzzy_rate,
    ROUND(COUNT(CASE WHEN viewed_cart = 1 THEN session_id ELSE NULL END) * 1.0 / NULLIF(COUNT(CASE WHEN viewed_fuzzy = 1 THEN session_id ELSE NULL END), 0), 4) AS cart_rate,
    ROUND(COUNT(CASE WHEN viewed_shipping = 1 THEN session_id ELSE NULL END) * 1.0 / NULLIF(COUNT(CASE WHEN viewed_cart = 1 THEN session_id ELSE NULL END), 0), 4) AS shipping_rate,
    ROUND(COUNT(CASE WHEN viewed_billing = 1 THEN session_id ELSE NULL END) * 1.0 / NULLIF(COUNT(CASE WHEN viewed_shipping = 1 THEN session_id ELSE NULL END), 0), 4) AS billing_rate,
    ROUND(COUNT(CASE WHEN viewed_thank_you = 1 THEN session_id ELSE NULL END) * 1.0 / NULLIF(COUNT(CASE WHEN viewed_billing = 1 THEN session_id ELSE NULL END), 0), 4) AS purchase_rate
FROM conversion_test_analysis
GROUP BY 2;
