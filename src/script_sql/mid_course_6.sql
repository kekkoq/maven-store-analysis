WITH session_first_pageview AS (
    SELECT
        wp.website_session_id AS session_id,
        MIN(wp.website_pageview_id) AS first_pageview_id
    FROM website_pageviews AS wp
    INNER JOIN website_sessions AS ws
        ON wp.website_session_id = ws.website_session_id
    WHERE 
        ws.created_at < '2012-07-28'
        AND ws.utm_campaign = 'nonbrand'
        AND ws.utm_source = 'gsearch'
        AND wp.website_pageview_id > 23504
    GROUP BY 
        wp.website_session_id
)
SELECT
    sd.pageview_url,
    COUNT(sfp.session_id) AS total_sessions,
    COUNT(o.order_id) AS total_orders,
    COUNT(DISTINCT o.order_id) * 1.0 / COUNT(DISTINCT sfp.session_id) AS conversion_rate
FROM session_first_pageview AS sfp
INNER JOIN website_pageviews AS sd
    ON sfp.first_pageview_id = sd.website_pageview_id
LEFT JOIN orders AS o ON sfp.session_id = o.website_session_id
WHERE sd.pageview_url IN ('/home', '/lander-1')
GROUP BY sd.pageview_url


