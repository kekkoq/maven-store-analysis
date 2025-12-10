-- Calculate the click-through rates for each step in the funnel
-- checing if each session viewed each page in the funnel

WITH session_level_flags AS (
SELECT
    DISTINCT session_id,
    MAX(viewed_products) AS total_viewed_products,
    MAX(viewed_fuzzy) AS total_viewed_fuzzy,
    MAX(viewed_cart) AS total_viewed_cart,
    MAX(viewed_shipping) AS total_viewed_shipping,
    MAX(viewed_billing) AS total_viewed_billing,
    MAX(viewed_thank_you) AS total_viewed_thank_you
FROM (
        SELECT
            ws.created_at,
            ws.website_session_id AS session_id,
            wp.pageview_url AS pageview_url,
                CASE WHEN wp.pageview_url = '/products' THEN 1 ELSE 0 END AS viewed_products,
            CASE WHEN wp.pageview_url = '/the-original-mr-fuzzy' THEN 1 ELSE 0 END AS viewed_fuzzy,
            CASE WHEN wp.pageview_url = '/cart' THEN 1 ELSE 0 END AS viewed_cart,
            CASE WHEN wp.pageview_url = '/shipping' THEN 1 ELSE 0 END AS viewed_shipping,
            CASE WHEN wp.pageview_url = '/billing' THEN 1 ELSE 0 END AS viewed_billing,
            CASE WHEN wp.pageview_url = '/thank-you-for-your-order' THEN 1 ELSE 0 END AS viewed_thank_you
        FROM website_sessions AS ws
        LEFT JOIN website_pageviews AS wp
            ON ws.website_session_id = wp.website_session_id
        WHERE ws.created_at BETWEEN '2012-08-05' AND '2012-09-04'
        AND ws.utm_source = 'gsearch' 
        AND ws.utm_campaign = 'nonbrand') AS session_pageviews
    GROUP BY session_id
)
SELECT
    COUNT(CASE WHEN total_viewed_products = 1 THEN session_id ELSE NULL END) * 1.0 / COUNT(session_id) AS to_product,
    COUNT(CASE WHEN total_viewed_fuzzy = 1 THEN session_id ELSE NULL END) * 1.0 / COUNT(CASE WHEN total_viewed_products = 1 THEN session_id ELSE NULL END) AS to_fuzzy,
    COUNT(CASE WHEN total_viewed_cart = 1 THEN session_id ELSE NULL END) * 1.0 / COUNT(CASE WHEN total_viewed_fuzzy = 1 THEN session_id ELSE NULL END) AS to_cart,
    COUNT(CASE WHEN total_viewed_shipping = 1 THEN session_id ELSE NULL END) * 1.0 / COUNT(CASE WHEN total_viewed_cart = 1 THEN session_id ELSE NULL END) AS to_shipping,
    COUNT(CASE WHEN total_viewed_billing = 1 THEN session_id ELSE NULL END) * 1.0 / COUNT(CASE WHEN total_viewed_shipping = 1 THEN session_id ELSE NULL END) AS to_billing,
    COUNT(CASE WHEN total_viewed_thank_you = 1 THEN session_id ELSE NULL END) * 1.0 / COUNT(CASE WHEN total_viewed_billing = 1 THEN session_id ELSE NULL END) AS to_thank_you
FROM session_level_flags;